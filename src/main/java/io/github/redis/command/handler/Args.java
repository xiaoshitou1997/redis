package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;

/**
 * Common parsing helpers for handlers — parse numbers from argv with proper
 * Redis-style error replies.
 *
 * <p>Every method returns {@code null} on failure AND has already pushed the
 * appropriate error reply to the client. Handlers should pattern-match:
 * <pre>
 *   Long n = Args.parseLong(c, 2);
 *   if (n == null) return;
 * </pre>
 */
public final class Args {

    private Args() {}

    /** Parse a long; replies error on malformed input. Returns null on failure. */
    public static Long parseLong(RedisClient c, int idx) {
        try {
            return Long.parseLong(c.arg(idx));
        } catch (NumberFormatException e) {
            c.replyError("ERR value is not an integer or out of range");
            return null;
        }
    }

    public static Integer parseInt(RedisClient c, int idx) {
        try {
            return Integer.parseInt(c.arg(idx));
        } catch (NumberFormatException e) {
            c.replyError("ERR value is not an integer or out of range");
            return null;
        }
    }

    public static Double parseDouble(RedisClient c, int idx) {
        String s = c.arg(idx);
        // Redis accepts "+inf"/"-inf"/"inf" for scores.
        if (s.equalsIgnoreCase("inf") || s.equalsIgnoreCase("+inf")
                || s.equalsIgnoreCase("+infinity")) return Double.POSITIVE_INFINITY;
        if (s.equalsIgnoreCase("-inf") || s.equalsIgnoreCase("-infinity")) return Double.NEGATIVE_INFINITY;
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            c.replyError("ERR value is not a valid float");
            return null;
        }
    }

    /** Uppercase a single arg safely. */
    public static String upper(RedisClient c, int idx) {
        return c.arg(idx).toUpperCase();
    }
}
