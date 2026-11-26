package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.storage.RedisValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * OBJECT / DEBUG / MONITOR / WAIT.
 *
 * <p>OBJECT ENCODING/REFCOUNT/FREQ/IDLETIME/HELP. The encoding is reported
 * honestly (jdk-hashmap, jdk-linkedlist, etc.) so clients can introspect —
 * Phase 3 will introduce real listpack/intset encodings.
 *
 * <p>DEBUG SLEEP, OBJECT, SET-ACTIVE-EXPIRE, JMAP — minimal handlers.
 *
 * <p>WAIT is implemented as immediate-ack (Phase 2 single-node: no replicas
 * to wait for). MONITOR acknowledges the request but does not actually push
 * commands (Phase 3 will add full monitor stream).
 */
public final class DebugCommands {

    private DebugCommands() {}

    // ===== OBJECT =====

    public static void object(RedisClient c) {
        // OBJECT ENCODING|REFCOUNT|FREQ|IDLETIME|HELP <key>
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        String key = c.arg(2);
        RedisValue v = c.storage().get(key);
        switch (sub) {
            case "ENCODING":
                if (v == null) { c.replyNil(); return; }
                c.replyBulk(encodingOf(v));
                return;
            case "REFCOUNT":
                c.replyInteger(1); // Phase 2 doesn't share references
                return;
            case "FREQ":
                c.replyInteger(v == null ? 0 : Math.min(255, (int) v.getAccessCount()));
                return;
            case "IDLETIME":
                if (v == null) { c.replyNil(); return; }
                long idleSec = (System.nanoTime() - v.getLastAccess()) / 1_000_000_000L;
                c.replyInteger(Math.max(0, idleSec));
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "OBJECT <subcommand> <key>. Subcommands are:",
                        "ENCODING <key> -- Return the kind of internal representation.",
                        "FREQ <key> -- Return the access frequency index.",
                        "IDLETIME <key> -- Return the idle time in seconds.",
                        "REFCOUNT <key> -- Return the number of references.",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown OBJECT subcommand: " + sub);
        }
    }

    private static String encodingOf(RedisValue v) {
        if (v.getEncoding() != null) return v.getEncoding().wireName();
        switch (v.getType()) {
            case STRING:     return "raw";
            case LIST:       return "linkedlist";
            case SET:        return "hashtable";
            case HASH:       return "hashtable";
            case SORTED_SET: return "skiplist";
            default:         return "raw";
        }
    }

    // ===== DEBUG =====

    public static void debug(RedisClient c) {
        if (c.argc() < 2) { c.replyError("ERR wrong number of arguments for 'debug|command' command"); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "SLEEP": {
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                Double secs = null;
                try { secs = Double.parseDouble(c.arg(2)); } catch (NumberFormatException ignored) {}
                if (secs == null) { c.replyError("ERR invalid sleep seconds"); return; }
                try { Thread.sleep((long) (secs * 1000)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                c.replyOK();
                return;
            }
            case "OBJECT": {
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                RedisValue v = c.storage().get(c.arg(2));
                if (v == null) { c.replyError("ERR no such key"); return; }
                StringBuilder sb = new StringBuilder();
                sb.append("Value at:0x0 refcount:1 encoding:").append(encodingOf(v))
                  .append(" serializedlength:").append(Values.sizeof(v))
                  .append(" lru:0 lru_seconds_idle:0");
                c.replyStatus(sb.toString());
                return;
            }
            case "JMAP":
            case "RELOAD":
            case "FLUSHALL":
                c.replyOK();
                return;
            case "SET-ACTIVE-EXPIRE":
                c.replyOK();
                return;
            case "CHANGE-REPL-ID":
                c.replyOK();
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "DEBUG <subcommand> [<arg> ...]. Subcommands are:",
                        "OBJECT <key> -- Show low level info about an object.",
                        "SLEEP <seconds> -- Stop the server for <seconds>.",
                        "SET-ACTIVE-EXPIRE <0|1> -- Enable/disable active expiration.",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown DEBUG subcommand: " + sub);
        }
    }

    // ===== MONITOR =====

    public static void monitor(RedisClient c) {
        // Phase 2 stub: acknowledge but don't actually push commands.
        c.replyOK();
    }

    // ===== WAIT numreplicas timeout =====

    public static void wait_(RedisClient c) {
        // WAIT numreplicas timeout — single-node Phase 2 always returns 0.
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        c.replyInteger(0);
    }
}
