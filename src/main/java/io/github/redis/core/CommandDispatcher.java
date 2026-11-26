package io.github.redis.core;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-threaded command dispatcher.
 *
 * <p>All command execution funnels through one thread, mirroring Redis's main
 * thread model. This gives us:
 * <ul>
 *   <li><b>Atomicity</b> — read-modify-write sequences (INCR, ZADD, HSET) cannot
 *       race with concurrent commands on the same key from other clients.</li>
 *   <li><b>Simpler concurrency</b> — most data structures don't need locking
 *       because they're only ever touched by one thread at a time.</li>
 *   <li><b>Consistent MULTI/EXEC</b> — queued commands execute back-to-back
 *       without interleaving from other clients.</li>
 * </ul>
 *
 * <p>Netty I/O workers continue to handle RESP encode/decode in parallel; only
 * the {@code handler.execute(client)} call is serialised. Reply writes
 * ({@code ctx.writeAndFlush}) are thread-safe and bounce back to the worker
 * automatically.
 *
 * <p>A slow command (KEYS, DEBUG SLEEP) blocks all clients — this matches
 * Redis behaviour. Phase 2 will move truly heavy operations (full SAVE, large
 * KEYS) to background threads.
 */
public final class CommandDispatcher {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(CommandDispatcher.class);

    private final ExecutorService executor;
    private final AtomicLong pendingTasks = new AtomicLong(0);

    public CommandDispatcher() {
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "redis-dispatcher");
                t.setDaemon(true);
                t.setUncaughtExceptionHandler((thread, cause) ->
                        log.error("Uncaught exception in dispatcher", cause));
                return t;
            }
        });
    }

    /**
     * Submit a task to be run on the single dispatcher thread. Returns immediately.
     */
    public void submit(Runnable task) {
        pendingTasks.incrementAndGet();
        executor.execute(() -> {
            try {
                task.run();
            } finally {
                pendingTasks.decrementAndGet();
            }
        });
    }

    /**
     * Submit a task and wait for it to complete. Useful for the initial RDB load
     * so the dispatcher thread owns the storage state before serving clients.
     */
    public void submitAndWait(Runnable task) {
        submit(task);
        // Spin until the task queue drains past the marker — cheap because
        // pendingTasks starts at 0 and we only need to wait for our own task.
        while (pendingTasks.get() > 0) {
            Thread.yield();
        }
    }

    public long pendingCount() {
        return pendingTasks.get();
    }

    /**
     * Graceful shutdown — wait up to 10 seconds for queued commands to drain.
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
