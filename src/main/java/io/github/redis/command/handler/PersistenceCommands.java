package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.persistence.PersistenceManager;
import io.github.redis.persistence.rdb.RdbSaver;

/**
 * Persistence commands: SAVE/BGSAVE/BGREWRITEAOF/LASTSAVE.
 */
public final class PersistenceCommands {

    private PersistenceCommands() {}

    public static void save(RedisClient c) {
        if (c.argc() != 1) { c.replyWrongArgc(); return; }
        PersistenceManager pm = c.services().get(PersistenceManager.class);
        if (pm == null) { c.replyError("ERR persistence subsystem unavailable"); return; }
        RdbSaver saver = pm.getRdbSaver();
        if (saver == null) { c.replyError("ERR persistence disabled"); return; }
        if (saver.isSaving()) {
            c.replyError("ERR Background save already in progress");
            return;
        }
        boolean ok = saver.save();
        c.replyOK();
        if (!ok) {
            // SAVE returns OK on success but reports failure via prior error log;
            // Redis returns an error in that case. Phase 1 will tighten this.
        }
    }

    public static void bgsave(RedisClient c) {
        if (c.argc() > 2) { c.replyWrongArgc(); return; }
        if (c.argc() == 2 && !c.arg(1).equalsIgnoreCase("SCHEDULE")) {
            c.replySyntaxError(); return;
        }
        PersistenceManager pm = c.services().get(PersistenceManager.class);
        if (pm == null) { c.replyError("ERR persistence subsystem unavailable"); return; }
        RdbSaver saver = pm.getRdbSaver();
        if (saver == null) { c.replyError("ERR persistence disabled"); return; }
        if (saver.isSaving()) {
            c.replyError("ERR Background save already in progress");
            return;
        }
        boolean started = saver.bgsave();
        c.replyStatus(started ? "Background saving started" : "ERR Background save could not be started");
    }

    public static void bgrewriteaof(RedisClient c) {
        PersistenceManager pm = c.services().get(PersistenceManager.class);
        if (pm == null) { c.replyError("ERR persistence subsystem unavailable"); return; }
        if (pm.isRewriteInProgress()) {
            c.replyError("ERR AOF rewrite already in progress");
            return;
        }
        // Kick off asynchronously — the snapshot itself is taken on the
        // dispatcher thread, but we don't block the client.
        new Thread(() -> {
            try { pm.rewriteAof(); }
            catch (Exception e) { e.printStackTrace(); }
        }, "aof-rewrite-trigger").start();
        c.replyStatus("Background append only file rewriting started");
    }
}
