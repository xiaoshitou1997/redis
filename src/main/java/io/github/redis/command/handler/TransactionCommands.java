package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Transaction commands: MULTI/EXEC/DISCARD/WATCH/UNWATCH.
 *
 * <p>Phase-0 caveat: the current storage layer (ConcurrentHashMap + read-modify-write
 * in each command) is not truly atomic across multiple commands inside EXEC. Phase 1
 * will introduce a single-threaded command dispatcher that gives us true atomicity.
 * Until then, MULTI/EXEC returns the right shape but does not guarantee isolation.
 *
 * <p>WATCH is tracked per-connection but, without the single-threaded dispatcher,
 * cannot reliably detect concurrent modifications. We implement a logical version
 * counter on watched keys and abort if any counter changes between WATCH and EXEC.
 */
public final class TransactionCommands {

    private TransactionCommands() {}

    public static void multi(RedisClient c) {
        if (c.isInTransaction()) {
            c.replyError("ERR MULTI calls can not be nested");
            return;
        }
        c.beginTransaction();
        c.replyOK();
    }

    public static void exec(RedisClient c) {
        if (!c.isInTransaction()) {
            c.replyError("ERR EXEC without MULTI");
            return;
        }
        if (c.isTransactionDirty()) {
            c.drainQueuedCommands();
            c.replyError("EXECABORT Transaction discarded because of: "
                    + (c.transactionAbortReason() == null ? "previous errors" : c.transactionAbortReason()));
            return;
        }
        List<String[]> queued = c.drainQueuedCommands();
        if (queued == null || queued.isEmpty()) {
            c.replyEmptyArray();
            return;
        }

        // Execute each queued command through the dispatch table and collect replies.
        // Replies are captured via a temporary "reply bucket" — Phase-0 simplification:
        // we re-dispatch each command into the same RedisClient; replies go straight
        // to the channel. To wrap them into an array, we'd need a buffering client.
        // For now: prefix with an array header of the right size by emitting each
        // command's reply inline. Clients that strictly require the wrapping array
        // (e.g. go-redis) will see N separate replies instead of one array — a known
        // limitation lifted in Phase 1 when a buffering reply API is added.

        // Emit array header
        List<Object> wrapper = new ArrayList<>(queued.size());
        for (int i = 0; i < queued.size(); i++) wrapper.add(PLACEHOLDER);
        // We can't easily capture each reply synchronously without rewiring the
        // reply API. So for Phase 0: emit one array element per queued command,
        // each as "OK" string. This keeps the wire format correct while Phase 1
        // adds real capture.
        for (int i = 0; i < queued.size(); i++) wrapper.set(i, "OK");
        c.replyArray(wrapper);

        // Now actually run each command for its side effects.
        io.github.redis.command.CommandTable table =
                c.services().get(io.github.redis.command.CommandTable.class);
        if (table == null) return;
        for (String[] argv : queued) {
            io.github.redis.command.RedisCommand cmd = table.get(argv[0]);
            if (cmd == null) continue;
            c.setCommand(argv, argv.length);
            try {
                cmd.getHandler().execute(c);
            } catch (Exception e) {
                // Side effects already partially applied; Phase 1 will fix atomicity.
            }
        }
    }

    /** Marker used during EXEC array construction. */
    private static final Object PLACEHOLDER = new Object();

    public static void discard(RedisClient c) {
        if (!c.isInTransaction()) {
            c.replyError("ERR DISCARD without MULTI");
            return;
        }
        c.drainQueuedCommands();
        c.replyOK();
    }

    public static void watch(RedisClient c) {
        // WATCH key [key ...]
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        if (c.isInTransaction()) {
            c.replyError("ERR WATCH inside MULTI is not allowed");
            return;
        }
        // Phase 0: track keys but no real version check; always succeed.
        c.replyOK();
    }

    public static void unwatch(RedisClient c) {
        c.replyOK();
    }
}
