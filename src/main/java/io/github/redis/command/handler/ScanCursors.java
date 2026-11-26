package io.github.redis.command.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Cursor-based scan utilities for SCAN/HSCAN/SSCAN/ZSCAN.
 *
 * <p>Phase-2 design: each scan call takes a fresh snapshot of the source
 * collection, advances linearly by {@code COUNT} items, and returns a new
 * cursor. This matches Redis's documented weak guarantee: mutations between
 * calls may cause the client to see a key twice or not at all, but iteration
 * always terminates.
 *
 * <p>The cursor is encoded as a decimal integer (the byte offset into the
 * snapshot). Cursor "0" is both the starting and terminating value, matching
 * Redis semantics. Negative or stale cursors are clamped to 0.
 */
public final class ScanCursors {

    private ScanCursors() {}

    /** Default batch size when COUNT is not specified. */
    public static final int DEFAULT_COUNT = 10;

    /**
     * Scan a snapshot, returning [newCursor, matching-items].
     *
     * @param cursorStr    opaque cursor string from the client
     * @param snapshot     snapshot of all candidates (must be stable enough within the call)
     * @param count        requested batch size
     * @param matchPattern optional glob pattern; null returns everything
     * @return list of exactly 2 elements: [newCursorAsString, itemList]
     */
    public static List<Object> scan(String cursorStr, Collection<String> snapshot,
                                    int count, String matchPattern) {
        int cursor = parseCursor(cursorStr);
        int effectiveCount = count <= 0 ? DEFAULT_COUNT : count;
        if (cursor < 0) cursor = 0;

        List<String> pool = (snapshot instanceof List)
                ? (List<String>) snapshot
                : new ArrayList<>(snapshot);

        // Apply MATCH filter by walking from the cursor and collecting up to COUNT hits.
        List<Object> hits = new ArrayList<>();
        int pos = cursor;
        while (pos < pool.size() && hits.size() < effectiveCount) {
            String k = pool.get(pos);
            if (matchPattern == null || GlobMatcher.matches(matchPattern, k)) {
                hits.add(k);
            }
            pos++;
        }

        // Compute new cursor. If we exhausted the snapshot, return "0" (done).
        String newCursor = (pos >= pool.size()) ? "0" : Integer.toString(pos);

        List<Object> result = new ArrayList<>(2);
        result.add(newCursor);
        result.add(hits);
        return result;
    }

    public static int parseCursor(String s) {
        if (s == null || s.isEmpty()) return 0;
        try {
            long n = Long.parseLong(s);
            if (n < 0) return 0;
            return (int) Math.min(n, Integer.MAX_VALUE);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static int parseCountArg(List<String> args, int fromIdx) {
        for (int i = fromIdx; i + 1 < args.size(); i++) {
            if (args.get(i).equalsIgnoreCase("COUNT")) {
                try { return Math.max(1, Integer.parseInt(args.get(i + 1))); }
                catch (NumberFormatException ignored) { return DEFAULT_COUNT; }
            }
        }
        return DEFAULT_COUNT;
    }

    public static String parseMatchArg(List<String> args, int fromIdx) {
        for (int i = fromIdx; i + 1 < args.size(); i++) {
            if (args.get(i).equalsIgnoreCase("MATCH")) {
                return args.get(i + 1);
            }
        }
        return null;
    }

    /** Glob matcher supporting {@code *} and {@code ?}. */
    public static final class GlobMatcher {
        private GlobMatcher() {}

        public static boolean matches(String pattern, String s) {
            return matches(pattern, 0, s, 0);
        }

        private static boolean matches(String p, int pi, String s, int si) {
            while (pi < p.length()) {
                char c = p.charAt(pi);
                if (c == '*') {
                    // Collapse consecutive '*'
                    while (pi + 1 < p.length() && p.charAt(pi + 1) == '*') pi++;
                    if (pi == p.length() - 1) return true;
                    for (int k = si; k <= s.length(); k++) {
                        if (matches(p, pi + 1, s, k)) return true;
                    }
                    return false;
                } else if (c == '?') {
                    if (si >= s.length()) return false;
                    pi++; si++;
                } else if (c == '[') {
                    int end = p.indexOf(']', pi);
                    if (end < 0) return false;
                    if (si >= s.length()) return false;
                    char target = s.charAt(si);
                    boolean negated = (pi + 1 < end && p.charAt(pi + 1) == '^');
                    int clsStart = negated ? pi + 2 : pi + 1;
                    boolean in = false;
                    for (int j = clsStart; j < end; j++) {
                        char lo = p.charAt(j);
                        if (j + 2 < end && p.charAt(j + 1) == '-') {
                            char hi = p.charAt(j + 2);
                            if (target >= lo && target <= hi) { in = true; break; }
                            j += 2;
                        } else {
                            if (target == lo) { in = true; break; }
                        }
                    }
                    if (negated == in) return false;
                    pi = end + 1; si++;
                } else if (c == '\\') {
                    if (pi + 1 >= p.length() || si >= s.length()) return false;
                    if (p.charAt(pi + 1) != s.charAt(si)) return false;
                    pi += 2; si++;
                } else {
                    if (si >= s.length() || s.charAt(si) != c) return false;
                    pi++; si++;
                }
            }
            return si == s.length();
        }
    }
}
