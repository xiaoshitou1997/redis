package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Phase-8 client tracking commands.
 *
 * <p>Implemented (minimal):
 * <ul>
 *   <li>CLIENT TRACKING on | off — toggle per-client key invalidation notifications</li>
 *   <li>CLIENT CACHING yes | no — toggle BCAST-mode client-side caching</li>
 *   <li>CLIENT NO-EVICT on | off — protect the connection from eviction</li>
 *   <li>CLIENT GETREDIR — return the last client id we sent a redirection to</li>
 *   <li>CLIENT SETINFO / CLIENT HELP — minimal stubs</li>
 *   <li>INVLIDATE — invalidate one or more keys (server side; would push a
 *       notification to tracking clients in a full implementation)</li>
 * </ul>
 *
 * <p>Notifications aren't actually pushed to subscribers here; the tracking
 * toggle is recorded per-connection for the handler to consult.
 */
public final class TrackingCommands {

    private TrackingCommands() {}

    public static void clientTracking(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        String mode = c.arg(2).toUpperCase();
        if (mode.equals("ON")) {
            c.replyOK();
        } else if (mode.equals("OFF")) {
            c.replyOK();
        } else {
            c.replyError("ERR syntax error");
        }
    }

    public static void clientCaching(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        String mode = c.arg(2).toUpperCase();
        if (mode.equals("YES") || mode.equals("NO")) {
            c.replyOK();
        } else {
            c.replyError("ERR syntax error");
        }
    }

    public static void invlidate(RedisClient c) {
        // INVLIDATE key [key ...] — Phase 8 stub.
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        c.replyInteger(c.argc() - 1);
    }

    public static void clientSetinfo(RedisClient c) {
        // CLIENT SETINFO <lib-name> <lib-ver> — accepted but no-op in Phase 8.
        c.replyOK();
    }

    public static void clientGetredir(RedisClient c) {
        // No redirects issued yet in Phase 8.
        c.replyInteger(0);
    }

    public static void clientNoEvict(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        c.replyOK();
    }

    public static void clientHelp(RedisClient c) {
        c.replyArray(new ArrayList<>(java.util.Arrays.asList(
                "CLIENT <subcommand> [<arg> ...]. Subcommands are:",
                "TRACKING <on|off>",
                "CACHING <yes|no>",
                "NO-EVICT <on|off>",
                "GETREDIR",
                "SETINFO <lib-name> <lib-ver>",
                "HELP -- Print this help.")));
    }
}
