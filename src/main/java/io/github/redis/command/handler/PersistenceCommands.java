package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.persistence.PersistenceManager;
import io.github.redis.persistence.rdb.RdbSaver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistence commands: SAVE/BGSAVE/BGREWRITEAOF/LASTSAVE.
 */
public final class PersistenceCommands {

    private static final Logger log = LoggerFactory.getLogger(PersistenceCommands.class);

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
        if (ok) {
            c.replyOK();
        } else {
            // Redis replies with an error when SAVE fails; the cause is in the log.
            log.error("SAVE failed; replying error to client {}", c.channel().remoteAddress());
            c.replyError("ERR Failed to save the RDB file");
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
        if (started) {
            c.replyStatus("Background saving started");
        } else {
            c.replyError("ERR Background save could not be started");
        }
    }

    public static void bgrewriteaof(RedisClient c) {
        PersistenceManager pm = c.services().get(PersistenceManager.class);
        if (pm == null) { c.replyError("ERR persistence subsystem unavailable"); return; }
        if (pm.isRewriteInProgress()) {
            c.replyError("ERR Background append only rewriting already in progress");
            return;
        }
        // Runs on the manager's dedicated single-thread rewriter; the snapshot
        // is taken on the dispatcher thread inside the rewrite for consistency.
        if (!pm.startRewrite()) {
            c.replyError("ERR Append only rewriting is disabled or could not be started");
            return;
        }
        c.replyStatus("Background append only file rewriting started");
    }
}
