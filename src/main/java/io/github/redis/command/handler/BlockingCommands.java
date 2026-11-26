package io.github.redis.command.handler;

import io.github.redis.core.BlockingCoordinator;
import io.github.redis.core.RedisClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * Blocking commands: BLPOP / BRPOP / BZPOPMIN / BZPOPMAX.
 *
 * <p>Strategy: try the underlying pop; if no data, register a wakeup with
 * {@link BlockingCoordinator} and mark the client blocked. The dispatcher then
 * returns without replying. When a push arrives or the timeout fires, the
 * coordinator submits a re-dispatch of the original argv to the dispatcher
 * (so this handler runs again, finds data, replies, clears the blocked flag).
 *
 * <p>The blocking coordinator always re-runs the entire command — the second
 * invocation will either succeed (data found) or, if multiple BLPOPs raced,
 * register interest again with the remaining timeout.
 */
public final class BlockingCommands {

    private BlockingCommands() {}

    public static void blpop(RedisClient c) { blockingPop(c, true); }
    public static void brpop(RedisClient c) { blockingPop(c, false); }

    private static void blockingPop(RedisClient c, boolean left) {
        // BLPOP key [key ...] timeout
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        Double timeout = null;
        try { timeout = Double.parseDouble(c.arg(c.argc() - 1)); }
        catch (NumberFormatException ignored) {}
        if (timeout == null || timeout < 0) {
            c.replyError("ERR timeout is not a float or out of range"); return;
        }
        long timeoutMillis = (long) (timeout * 1000);

        // Try each key in order.
        for (int j = 1; j < c.argc() - 1; j++) {
            String key = c.arg(j);
            java.util.LinkedList<String> l;
            try { l = Values.getList(c.storage(), key); }
            catch (Values.WrongTypeException wte) {
                c.replyError(wte.getMessage()); return;
            }
            if (l != null && !l.isEmpty()) {
                String v = left ? l.removeFirst() : l.removeLast();
                if (l.isEmpty()) c.storage().delete(key);
                else             Values.saveList(c.storage(), key, l, null);
                List<Object> out = new ArrayList<>(2);
                out.add(key);
                out.add(v);
                c.replyArray(out);
                return;
            }
        }
        // No data — park.
        parkForList(c, timeoutMillis);
    }

    private static void parkForList(RedisClient c, long timeoutMillis) {
        BlockingCoordinator bc = c.services().get(BlockingCoordinator.class);
        if (bc == null) {
            // No coordinator configured; return nil immediately.
            c.replyNil();
            return;
        }
        Set<String> keys = new HashSet<>();
        for (int j = 1; j < c.argc() - 1; j++) keys.add(c.arg(j));

        // Capture a runnable that re-dispatches this command on the dispatcher.
        final String[] argv = new String[c.argc()];
        for (int i = 0; i < c.argc(); i++) argv[i] = c.arg(i);

        c.markBlocked();
        bc.park(keys, timeoutMillis, timedOut -> {
            c.clearBlocked();
            if (timedOut) {
                // Timeout fired — reply nil.
                c.replyNil();
                return;
            }
            // Push arrived — re-run this BLPOP. It will either find data and reply,
            // or re-park with whatever time remains.
            RedisClient client = c;
            client.setCommand(argv, argv.length);
            // Re-invoke the handler directly (we're already on the dispatcher).
            if (argv[0].equalsIgnoreCase("BLPOP")) blockingPop(client, true);
            else if (argv[0].equalsIgnoreCase("BRPOP")) blockingPop(client, false);
        });
    }

    public static void bzpopmin(RedisClient c) { blockingZpop(c, true); }
    public static void bzpopmax(RedisClient c) { blockingZpop(c, false); }

    private static void blockingZpop(RedisClient c, boolean min) {
        // BZPOPMIN key [key ...] timeout
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        Double timeout = null;
        try { timeout = Double.parseDouble(c.arg(c.argc() - 1)); }
        catch (NumberFormatException ignored) {}
        if (timeout == null || timeout < 0) {
            c.replyError("ERR timeout is not a float or out of range"); return;
        }
        long timeoutMillis = (long) (timeout * 1000);

        for (int j = 1; j < c.argc() - 1; j++) {
            String key = c.arg(j);
            TreeMap<String, Double> z;
            try { z = Values.getZSet(c.storage(), key); }
            catch (Values.WrongTypeException wte) {
                c.replyError(wte.getMessage()); return;
            }
            if (z != null && !z.isEmpty()) {
                java.util.List<java.util.Map.Entry<String, Double>> sorted = new java.util.ArrayList<>(z.entrySet());
                sorted.sort((a, b) -> {
                    int byVal = Double.compare(a.getValue(), b.getValue());
                    return byVal != 0 ? byVal : a.getKey().compareTo(b.getKey());
                });
                if (!min) java.util.Collections.reverse(sorted);
                java.util.Map.Entry<String, Double> e = sorted.get(0);
                String member = e.getKey();
                double score = e.getValue();
                z.remove(member);
                if (z.isEmpty()) c.storage().delete(key);
                else             Values.saveZSet(c.storage(), key, z, null);
                List<Object> out = new ArrayList<>(3);
                out.add(key);
                out.add(member);
                out.add(StringCommands.formatFloat(score));
                c.replyArray(out);
                return;
            }
        }
        parkForZSet(c, timeoutMillis, min);
    }

    private static void parkForZSet(RedisClient c, long timeoutMillis, boolean min) {
        BlockingCoordinator bc = c.services().get(BlockingCoordinator.class);
        if (bc == null) { c.replyEmptyArray(); return; }
        Set<String> keys = new HashSet<>();
        for (int j = 1; j < c.argc() - 1; j++) keys.add(c.arg(j));
        final String[] argv = new String[c.argc()];
        for (int i = 0; i < c.argc(); i++) argv[i] = c.arg(i);

        c.markBlocked();
        bc.park(keys, timeoutMillis, timedOut -> {
            c.clearBlocked();
            if (timedOut) { c.replyEmptyArray(); return; }
            c.setCommand(argv, argv.length);
            if (argv[0].equalsIgnoreCase("BZPOPMIN")) blockingZpop(c, true);
            else                                     blockingZpop(c, false);
        });
    }
}
