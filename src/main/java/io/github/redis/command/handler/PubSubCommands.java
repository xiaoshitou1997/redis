package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.pubsub.PubSubManager;

/**
 * Pub/Sub commands: PUBLISH/SUBSCRIBE/UNSUBSCRIBE/PUBSUB.
 *
 * <p>PSUBSCRIBE/PUNSUBSCRIBE are stubs for Phase 0 — the underlying PubSubManager
 * only tracks channel subscriptions.
 */
public final class PubSubCommands {

    private PubSubCommands() {}

    public static void publish(RedisClient c) {
        // PUBLISH channel message
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        PubSubManager mgr = c.services().get(PubSubManager.class);
        if (mgr == null) { c.replyInteger(0); return; }
        long n = mgr.publish(c.arg(1), c.arg(2));
        c.replyInteger(n);
    }

    public static void subscribe(RedisClient c) {
        // SUBSCRIBE channel [channel ...]
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        PubSubManager mgr = c.services().get(PubSubManager.class);
        if (mgr == null) { c.replyError("ERR pubsub subsystem unavailable"); return; }
        String clientId = c.channel().id().asLongText();
        for (int j = 1; j < c.argc(); j++) {
            mgr.subscribe(c.arg(j), clientId, c.ctx());
        }
    }

    public static void unsubscribe(RedisClient c) {
        // UNSUBSCRIBE [channel ...]
        PubSubManager mgr = c.services().get(PubSubManager.class);
        if (mgr == null) { c.replyError("ERR pubsub subsystem unavailable"); return; }
        String clientId = c.channel().id().asLongText();
        if (c.argc() == 1) {
            mgr.unsubscribeAll(clientId);
        } else {
            for (int j = 1; j < c.argc(); j++) {
                mgr.unsubscribe(c.arg(j), clientId);
            }
        }
    }

    public static void psubscribe(RedisClient c) {
        // Phase 0 stub: behave like SUBSCRIBE (no real pattern matching).
        subscribe(c);
    }

    public static void punsubscribe(RedisClient c) {
        unsubscribe(c);
    }

    public static void pubsub(RedisClient c) {
        // PUBSUB CHANNELS [pattern] / NUMSUB / NUMPAT
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "CHANNELS":
                // Return empty array; pattern matching not supported in Phase 0.
                c.replyEmptyArray();
                return;
            case "NUMSUB":
                // Reply with [channel, count, channel, count, ...]
                java.util.List<Object> out = new java.util.ArrayList<>();
                PubSubManager mgr = c.services().get(PubSubManager.class);
                for (int j = 2; j < c.argc(); j++) {
                    out.add(c.arg(j));
                    out.add(0L); // count unknown in Phase 0
                }
                if (mgr == null && out.isEmpty()) { c.replyEmptyArray(); return; }
                c.replyArray(out);
                return;
            case "NUMPAT":
                c.replyInteger(0);
                return;
            default:
                c.replyError("ERR Unknown PUBSUB subcommand: " + sub);
        }
    }
}
