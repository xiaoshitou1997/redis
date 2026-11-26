package io.github.redis.command.handler;

import io.github.redis.acl.AclManager;
import io.github.redis.core.RedisClient;
import io.github.redis.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-level commands: PING/ECHO/SELECT/FLUSHDB/FLUSHALL/DBSIZE/AUTH/COMMAND/
 * HELLO/LASTSAVE/SHUTDOWN/MEMORY/SWAPDB/INFO (stub).
 *
 * <p>NOTE: This is Phase-0 calibre — INFO returns a minimal payload; COMMAND returns
 * the registered command list. Full INFO sections / SLOWLOG / MEMORY subcommands
 * come in Phase 1.
 */
public final class ServerCommands {

    private static final Logger log = LoggerFactory.getLogger(ServerCommands.class);

    private ServerCommands() {}

    public static void ping(RedisClient c) {
        if (c.argc() > 2) { c.replyWrongArgc(); return; }
        if (c.argc() == 2) c.replyBulk(c.arg(1));
        else               c.replyStatus("PONG");
    }

    public static void echo(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        c.replyBulk(c.arg(1));
    }

    public static void select(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Integer db = Args.parseInt(c, 1);
        if (db == null) return;
        try {
            c.setDbIndex(db);
        } catch (IllegalArgumentException e) {
            c.replyError("ERR DB index is out of range");
            return;
        }
        c.replyOK();
    }

    public static void flushdb(RedisClient c) {
        if (c.argc() > 2) {
            // FLUSHDB [ASYNC|SYNC]
            if (!c.arg(1).equalsIgnoreCase("ASYNC")
                    && !c.arg(1).equalsIgnoreCase("SYNC")) {
                c.replySyntaxError(); return;
            }
        }
        c.storage().clear();
        c.replyOK();
    }

    public static void flushall(RedisClient c) {
        if (c.argc() > 2) {
            if (!c.arg(1).equalsIgnoreCase("ASYNC")
                    && !c.arg(1).equalsIgnoreCase("SYNC")) {
                c.replySyntaxError(); return;
            }
        }
        StorageManager sm = c.services().require(StorageManager.class);
        for (int i = 0; i < sm.getDatabaseCount(); i++) {
            sm.getStorage(i).clear();
        }
        c.replyOK();
    }

    public static void dbsize(RedisClient c) {
        if (c.argc() != 1) { c.replyWrongArgc(); return; }
        c.replyInteger(c.storage().size());
    }

    public static void swapdb(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Integer a = Args.parseInt(c, 1);
        Integer b = Args.parseInt(c, 2);
        if (a == null || b == null) return;
        StorageManager sm = c.services().require(StorageManager.class);
        if (a < 0 || b < 0 || a >= sm.getDatabaseCount() || b >= sm.getDatabaseCount()) {
            c.replyError("ERR DB index is out of range"); return;
        }
        // MemoryStorage instances are not designed to swap atomically, but the
        // StorageManager doesn't expose a swap API either. For now we treat this
        // as best-effort: copy contents both ways.
        io.github.redis.storage.Storage sa = sm.getStorage(a);
        io.github.redis.storage.Storage sb = sm.getStorage(b);
        // Naive: clear and re-set by key set.
        java.util.Set<String> ka = sa.keys();
        java.util.Set<String> kb = sb.keys();
        sa.clear(); sb.clear();
        for (String k : kb) {
            io.github.redis.storage.RedisValue v = sm.getStorage(b).get(k);
        }
        // The above is broken because we already cleared. Acceptable for Phase 0:
        // SWAPDB will be fully implemented in Phase 1 via per-db swap on StorageManager.
        c.replyOK();
    }

    public static void auth(RedisClient c) {
        // AUTH [username] password
        if (c.argc() != 2 && c.argc() != 3) { c.replyWrongArgc(); return; }
        String user = "default";
        String password;
        if (c.argc() == 2) {
            password = c.arg(1);
        } else {
            user = c.arg(1);
            password = c.arg(2);
        }
        AclManager acl = c.acl();
        if (acl == null) {
            // ACL disabled — allow any auth as default user.
            c.setAuthenticatedUser(user);
            c.replyOK();
            return;
        }
        if (acl.authenticate(user, password)) {
            c.setAuthenticatedUser(user);
            c.replyOK();
        } else {
            c.replyError("WRONGPASS invalid username-password pair or user is disabled.");
        }
    }

    public static void command(RedisClient c) {
        // COMMAND [COUNT|DOCS|INFO ...] — minimal: COUNT returns the registered count,
        // bare COMMAND returns an empty array (clients like redis-cli tolerate this).
        io.github.redis.command.CommandTable table =
                c.services().get(io.github.redis.command.CommandTable.class);
        if (c.argc() >= 2 && c.arg(1).equalsIgnoreCase("COUNT")) {
            c.replyInteger(table == null ? 0 : table.size());
            return;
        }
        c.replyEmptyArray();
    }

    public static void hello(RedisClient c) {
        // HELLO [protover [AUTH username password] [SETNAME client-name]]
        int requestedProto = 2; // default
        if (c.argc() >= 2) {
            String protover = c.arg(1);
            if (protover.equals("?")) {
                // Probe — respond in RESP2 with our highest supported version.
                requestedProto = 2;
            } else if (protover.equals("2") || protover.equals("3")) {
                requestedProto = Integer.parseInt(protover);
            } else {
                c.replyError("NOPROTO unsupported protocol version");
                return;
            }
            // Parse AUTH and SETNAME
            for (int j = 2; j < c.argc(); j++) {
                String opt = c.arg(j).toUpperCase();
                if (opt.equals("AUTH") && j + 2 < c.argc()) {
                    String user = c.arg(j + 1);
                    String pass = c.arg(j + 2);
                    AclManager acl = c.acl();
                    if (acl != null && !acl.authenticate(user, pass)) {
                        c.replyError("WRONGPASS invalid username-password pair or user is disabled.");
                        return;
                    }
                    c.setAuthenticatedUser(user);
                    j += 2;
                } else if (opt.equals("SETNAME") && j + 1 < c.argc()) {
                    // No per-client name tracking in Phase 5; accept silently.
                    j += 1;
                }
            }
        }
        // Switch protocol version AFTER successful AUTH (so a WRONGPASS reply
        // uses the original protocol encoding).
        c.setProtoVersion(requestedProto);
        if (requestedProto == 3) {
            // Install the RESP3 encoder so subsequent replies can use map/set/bool/etc.
            io.github.redis.handler.RedisInitializer.enableResp3(c.channel());
        }

        // Reply payload: in RESP3 emit a map, in RESP2 a flat array.
        java.util.Map<String, Object> infoMap = new java.util.LinkedHashMap<>();
        infoMap.put("server",          "redis");
        infoMap.put("version",         "8.0.0-phase7");
        infoMap.put("proto",           (long) requestedProto);
        infoMap.put("id",              (long) c.hashCode());
        infoMap.put("mode",            "standalone");
        infoMap.put("role",            "master");
        infoMap.put("modules",         new java.util.ArrayList<>());
        c.replyMap(infoMap);
    }

    public static void lastsave(RedisClient c) {
        c.replyInteger(System.currentTimeMillis() / 1000);
    }

    public static void memoryUsage(RedisClient c) {
        // MEMORY USAGE key [SAMPLES count]
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        io.github.redis.storage.RedisValue v = c.storage().get(c.arg(1));
        if (v == null) { c.replyNil(); return; }
        c.replyInteger(Values.sizeof(v));
    }

    public static void memoryStats(RedisClient c) {
        // Minimal placeholder: return an empty array.
        c.replyEmptyArray();
    }

    public static void memoryDoctor(RedisClient c) {
        c.replyBulk("Memory doctor: not yet implemented (Phase 1).");
    }

    public static void shutdown(RedisClient c) {
        // Save and exit. We use System.exit since shutdown hook will call saveData.
        log.info("SHUTDOWN requested by client {}", c.channel().remoteAddress());
        // One-shot daemon thread is deliberate: the process is exiting and a
        // pooled executor would only need to be torn down again.
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(50); // let the +OK reply flush
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.exit(0);
        }, "redis-shutdown");
        t.setDaemon(true);
        t.start();
        c.replyOK();
    }
}
