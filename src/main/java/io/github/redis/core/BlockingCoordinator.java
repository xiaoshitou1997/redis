package io.github.redis.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Blocking-command coordinator.
 *
 * <p>Single-threaded dispatcher model. When a blocking command (BLPOP etc.)
 * runs and finds no data, it <b>does not</b> reply; instead it parks itself
 * here via {@link #park}. The dispatcher thread returns immediately and keeps
 * serving other clients. When data arrives (via {@link #notifyPushedTo}) or
 * the timeout fires, the registered callback is submitted back to the
 * dispatcher, which re-runs the command. The second invocation finds data,
 * replies, and clears the blocked flag.
 *
 * <p>This matches Redis's behavior: at most one client is unblocked per push,
 * and parked clients don't stall the main thread.
 */
public final class BlockingCoordinator {

    /** Callback fired on the dispatcher thread when the wakeup condition triggers. */
    public interface Wakeup {
        void run(boolean timedOut);
    }

    private final CommandDispatcher dispatcher;
    private final ScheduledExecutorService timeoutScheduler;
    private final Map<String, List<Registration>> waitersByKey = new HashMap<>();
    private final AtomicLong blockedCount = new AtomicLong(0);

    public BlockingCoordinator(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.timeoutScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "redis-blocking-timeout");
            t.setDaemon(true);
            return t;
        });
    }

    public long getBlockedClientCount() { return blockedCount.get(); }

    /**
     * Register a wakeup for the given keys + timeout. Returns immediately
     * (does NOT block the calling thread). The caller must have already set
     * the client's "blocked" flag and must NOT have sent a reply.
     *
     * @param keys           keys the client is waiting on
     * @param timeoutMillis  0 = block forever, otherwise wake after this many ms
     * @param onWakeup       invoked on the dispatcher thread
     * @return a handle that can be passed to {@link #cancel} if the client dies
     */
    public Registration park(Set<String> keys, long timeoutMillis, Wakeup onWakeup) {
        final Registration reg = new Registration(keys, onWakeup);
        for (String k : keys) {
            waitersByKey.computeIfAbsent(k, x -> new ArrayList<>()).add(reg);
        }
        blockedCount.incrementAndGet();

        if (timeoutMillis > 0) {
            reg.timeout = timeoutScheduler.schedule(() -> {
                // On timeout, submit a wakeup with timedOut=true on the dispatcher.
                dispatcher.submit(() -> {
                    if (!reg.done) {
                        reg.done = true;
                        removeRegistration(reg);
                        blockedCount.decrementAndGet();
                        try { onWakeup.run(true); } catch (Exception ignored) {}
                    }
                });
            }, timeoutMillis, TimeUnit.MILLISECONDS);
        }
        return reg;
    }

    /**
     * Wake one waiter parked on {@code key} (FIFO). Submits the wakeup to the
     * dispatcher thread. Safe to call from any thread.
     */
    public void notifyPushedTo(String key) {
        List<Registration> regs = waitersByKey.get(key);
        if (regs == null || regs.isEmpty()) return;
        // Find the first non-done registration.
        Registration first = null;
        synchronized (this) {
            for (Registration r : regs) {
                if (!r.done) { first = r; break; }
            }
            if (first == null) return;
            first.done = true;
        }
        if (first.timeout != null) first.timeout.cancel(false);
        removeRegistration(first);
        blockedCount.decrementAndGet();
        final Registration f = first;
        dispatcher.submit(() -> { try { f.onWakeup.run(false); } catch (Exception ignored) {} });
    }

    /** Cancel a parked wakeup (e.g. when the client disconnects). */
    public void cancel(Registration reg) {
        if (reg == null || reg.done) return;
        reg.done = true;
        if (reg.timeout != null) reg.timeout.cancel(false);
        removeRegistration(reg);
        blockedCount.decrementAndGet();
    }

    private synchronized void removeRegistration(Registration reg) {
        for (String k : reg.keys) {
            List<Registration> regs = waitersByKey.get(k);
            if (regs != null) {
                regs.remove(reg);
                if (regs.isEmpty()) waitersByKey.remove(k);
            }
        }
    }

    public static final class Registration {
        final Set<String> keys;
        final Wakeup onWakeup;
        volatile ScheduledFuture<?> timeout;
        volatile boolean done;
        Registration(Set<String> keys, Wakeup w) { this.keys = keys; this.onWakeup = w; }
    }

    public void shutdown() {
        timeoutScheduler.shutdownNow();
        try { timeoutScheduler.awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /** Convenience: build a singleton set. */
    public static Set<String> keys(String... ks) {
        Set<String> s = new HashSet<>();
        for (String k : ks) s.add(k);
        return s;
    }
}
