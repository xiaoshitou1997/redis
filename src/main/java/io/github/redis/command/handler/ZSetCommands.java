package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Sorted-set commands. Uses a {@code TreeMap<String, Double>} (member -> score)
 * as the underlying store. Not as feature-complete as Redis's skiplist+dict,
 * but covers all commonly used Z* commands.
 *
 * <p>Implemented: ZADD/ZREM/ZRANGE/ZREVRANGE/ZSCORE/ZMSCORE/ZCARD/ZRANK/ZREVRANK/
 * ZINCRBY/ZPOPMIN/ZPOPMAX/ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZCOUNT/ZREMRANGEBYRANK/
 * ZREMRANGEBYSCORE/ZUNIONSTORE/ZINTERSTORE/ZUNION/ZINTER/ZDIFF/ZDIFFSTORE.
 */
public final class ZSetCommands {

    private ZSetCommands() {}

    // ===== ZADD =====

    public static void zadd(RedisClient c) {
        // ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }

        boolean nx = false, xx = false, gt = false, lt = false, ch = false, incr = false;
        int j = 2;
        for (; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("NX")) { nx = true; continue; }
            if (opt.equals("XX")) { xx = true; continue; }
            if (opt.equals("GT")) { gt = true; continue; }
            if (opt.equals("LT")) { lt = true; continue; }
            if (opt.equals("CH")) { ch = true; continue; }
            if (opt.equals("INCR")) { incr = true; continue; }
            break; // first non-flag token = start of score/member pairs
        }
        if ((c.argc() - j) % 2 != 0 || c.argc() == j) {
            c.replySyntaxError(); return;
        }

        if (nx && xx) { c.replyError("ERR XX and NX options at the same time are not compatible"); return; }
        if ((gt || lt) && nx) { c.replyError("ERR GT, LT, and/or NX options at the same time are not compatible"); return; }
        if (gt && lt) { c.replyError("ERR GT, LT, and/or NX options at the same time are not compatible"); return; }
        if (incr && (c.argc() - j) != 2) { c.replyError("ERR INCR option supports a single increment-element pair"); return; }

        Object carrier;
        try {
            carrier = Values.getZSetCarrier(c.storage(), c.arg(1));
            if (carrier == null) {
                // create empty listpack
                io.github.redis.encoding.Listpack lp = new io.github.redis.encoding.Listpack();
                io.github.redis.storage.RedisValue rv = new io.github.redis.storage.RedisValue(lp, io.github.redis.storage.ValueType.SORTED_SET);
                rv.setEncoding(io.github.redis.encoding.Encoding.LISTPACK);
                c.storage().set(c.arg(1), rv);
                carrier = lp;
            }
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }

        long addedOrChanged = 0;
        Double incrResult = null;

        for (; j < c.argc(); j += 2) {
            Double score = Args.parseDouble(c, j);
            if (score == null) return;
            String member = c.arg(j + 1);

            // Locate existing member + its current score.
            Double curScore = zgetScore(carrier, member);
            boolean existed = curScore != null;
            if (nx && existed) continue;
            if (xx && !existed) continue;

            if (incr) {
                double base = existed ? curScore : 0.0;
                double next = base + score;
                if (gt && existed && next <= curScore) continue;
                if (lt && existed && next >= curScore) continue;
                carrier = zset(carrier, member, next);
                addedOrChanged++;
                incrResult = next;
            } else {
                if (!existed) {
                    carrier = zset(carrier, member, score);
                    addedOrChanged++;
                } else if (gt && score > curScore) {
                    carrier = zset(carrier, member, score);
                    addedOrChanged++;
                } else if (lt && score < curScore) {
                    carrier = zset(carrier, member, score);
                    addedOrChanged++;
                } else if (!gt && !lt && score != curScore) {
                    carrier = zset(carrier, member, score);
                    addedOrChanged++;
                }
            }
        }

        if (Values.zsetShouldPromote(carrier)) {
            carrier = Values.promoteZSetToTreeMap(carrier);
        }
        boolean becameNonEmpty = (zsize(carrier) > 0);
        if (zsize(carrier) == 0) c.storage().delete(c.arg(1));
        else                     Values.saveZSetCarrier(c.storage(), c.arg(1), carrier, null);

        if (becameNonEmpty && addedOrChanged > 0) {
            io.github.redis.core.BlockingCoordinator bc =
                    c.services().get(io.github.redis.core.BlockingCoordinator.class);
            if (bc != null) bc.notifyPushedTo(c.arg(1));
        }

        if (incr) {
            if (incrResult == null) c.replyNil();
            else                    c.replyBulk(StringCommands.formatFloat(incrResult));
        } else {
            c.replyInteger(addedOrChanged);
        }
    }

    /** Set a (member, score) entry. Returns the (possibly promoted) carrier. */
    @SuppressWarnings("unchecked")
    private static Object zset(Object carrier, String member, double score) {
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
            int idx = lp.indexOfMember(member);
            if (idx >= 0) {
                ((io.github.redis.encoding.Listpack.ScoreMember) lp.get(idx)).score = score;
            } else {
                lp.append(new io.github.redis.encoding.Listpack.ScoreMember(member, score));
            }
            return lp;
        }
        ((java.util.TreeMap<String, Double>) carrier).put(member, score);
        return carrier;
    }

    private static Double zgetScore(Object carrier, String member) {
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
            int idx = lp.indexOfMember(member);
            if (idx < 0) return null;
            return ((io.github.redis.encoding.Listpack.ScoreMember) lp.get(idx)).score;
        }
        @SuppressWarnings("unchecked")
        java.util.TreeMap<String, Double> z = (java.util.TreeMap<String, Double>) carrier;
        return z.get(member);
    }

    private static int zsize(Object carrier) {
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            return ((io.github.redis.encoding.Listpack) carrier).size();
        }
        return ((java.util.TreeMap<?, ?>) carrier).size();
    }

    public static void zrem(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null) { c.replyInteger(0); return; }
        long removed = 0;
        for (int j = 2; j < c.argc(); j++) {
            if (z.remove(c.arg(j)) != null) removed++;
        }
        if (z.isEmpty()) c.storage().delete(c.arg(1));
        else             Values.saveZSet(c.storage(), c.arg(1), z, null);
        c.replyInteger(removed);
    }

    public static void zcard(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        c.replyInteger(z == null ? 0 : z.size());
    }

    public static void zscore(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null) { c.replyNil(); return; }
        Double score = z.get(c.arg(2));
        if (score == null) c.replyNil();
        else               c.replyBulk(StringCommands.formatFloat(score));
    }

    public static void zmscore(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        List<Object> out = new ArrayList<>(c.argc() - 2);
        for (int j = 2; j < c.argc(); j++) {
            if (z == null) { out.add(null); continue; }
            Double s = z.get(c.arg(j));
            out.add(s == null ? null : StringCommands.formatFloat(s));
        }
        c.replyArray(out);
    }

    public static void zincrby(RedisClient c) {
        // ZINCRBY key increment member
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Double incr = Args.parseDouble(c, 2);
        if (incr == null) return;
        TreeMap<String, Double> z;
        try {
            z = Values.getOrCreateZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        String member = c.arg(3);
        double cur = z.getOrDefault(member, 0.0);
        double next = cur + incr;
        z.put(member, next);
        Values.saveZSet(c.storage(), c.arg(1), z, null);
        c.replyBulk(StringCommands.formatFloat(next));
    }

    // ===== Range queries by rank =====

    public static void zrange(RedisClient c) {
        // ZRANGE key min max [BYSCORE|BYINDEX] [REV] [LIMIT offset count] [WITHSCORES]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null || z.isEmpty()) { c.replyEmptyArray(); return; }

        // Parse options
        boolean withscores = false;
        boolean rev = false;
        boolean byScore = false;
        long limitOffset = 0, limitCount = -1;
        for (int j = 4; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            switch (opt) {
                case "WITHSCORES": withscores = true; break;
                case "REV": rev = true; break;
                case "BYSCORE": byScore = true; break;
                case "BYINDEX": byScore = false; break;
                case "LIMIT":
                    if (j + 2 >= c.argc()) { c.replySyntaxError(); return; }
                    Long off = Args.parseLong(c, j + 1);
                    Long cnt = Args.parseLong(c, j + 2);
                    if (off == null || cnt == null) return;
                    limitOffset = off;
                    limitCount = cnt;
                    j += 2;
                    break;
                default:
                    c.replySyntaxError(); return;
            }
        }

        List<Map.Entry<String, Double>> entries = new ArrayList<>(z.entrySet());
        // Sort by score (then by member for stable ordering). The underlying
        // TreeMap<String,Double> keeps members in lexical order; ZRANGE needs
        // score order. A real skiplist in Phase 3 will make this O(log N).
        entries.sort((a, b) -> {
            int byVal = Double.compare(a.getValue(), b.getValue());
            return byVal != 0 ? byVal : a.getKey().compareTo(b.getKey());
        });
        if (rev && !byScore) {
            java.util.Collections.reverse(entries);
        }

        List<Object> out = new ArrayList<>();
        if (byScore) {
            Double min = parseBound(c.arg(2), true, rev);
            Double max = parseBound(c.arg(3), false, rev);
            if (min == null || max == null) { c.replyError("ERR min or max is not a float"); return; }
            if (rev) {
                Double t = min; min = max; max = t;
            }
            int idx = 0;
            int emitted = 0;
            for (Map.Entry<String, Double> e : entries) {
                double s = e.getValue();
                if (s < min || s > max) continue;
                if (idx++ < limitOffset) continue;
                out.add(e.getKey());
                if (withscores) out.add(StringCommands.formatFloat(s));
                emitted++;
                if (limitCount >= 0 && emitted >= limitCount) break;
            }
        } else {
            int len = entries.size();
            int start = parseIndex(c.arg(2), len);
            int stop  = parseIndex(c.arg(3), len);
            if (start < 0) start = 0;
            if (stop >= len) stop = len - 1;
            if (start > stop || start >= len) { c.replyEmptyArray(); return; }
            for (int i = start; i <= stop; i++) {
                Map.Entry<String, Double> e = entries.get(i);
                out.add(e.getKey());
                if (withscores) out.add(StringCommands.formatFloat(e.getValue()));
            }
        }
        c.replyArray(out);
    }

    public static void zrevrange(RedisClient c) {
        // ZREVRANGE key start stop [WITHSCORES]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null || z.isEmpty()) { c.replyEmptyArray(); return; }
        boolean withscores = c.argc() >= 5 && c.arg(4).equalsIgnoreCase("WITHSCORES");

        List<Map.Entry<String, Double>> entries = new ArrayList<>(z.entrySet());
        entries.sort((a, b) -> {
            int byScore = Double.compare(a.getValue(), b.getValue());
            return byScore != 0 ? byScore : a.getKey().compareTo(b.getKey());
        });
        java.util.Collections.reverse(entries);
        int len = entries.size();
        int start = parseIndex(c.arg(2), len);
        int stop  = parseIndex(c.arg(3), len);
        if (start < 0) start = 0;
        if (stop >= len) stop = len - 1;
        if (start > stop || start >= len) { c.replyEmptyArray(); return; }
        List<Object> out = new ArrayList<>();
        for (int i = start; i <= stop; i++) {
            Map.Entry<String, Double> e = entries.get(i);
            out.add(e.getKey());
            if (withscores) out.add(StringCommands.formatFloat(e.getValue()));
        }
        c.replyArray(out);
    }

    public static void zrangebyscore(RedisClient c) {
        // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null || z.isEmpty()) { c.replyEmptyArray(); return; }
        Double min = parseBound(c.arg(2), true, false);
        Double max = parseBound(c.arg(3), false, false);
        if (min == null || max == null) { c.replyError("ERR min or max is not a float"); return; }

        boolean withscores = false;
        long limitOffset = 0, limitCount = -1;
        for (int j = 4; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("WITHSCORES")) withscores = true;
            else if (opt.equals("LIMIT")) {
                if (j + 2 >= c.argc()) { c.replySyntaxError(); return; }
                Long off = Args.parseLong(c, j + 1);
                Long cnt = Args.parseLong(c, j + 2);
                if (off == null || cnt == null) return;
                limitOffset = off; limitCount = cnt; j += 2;
            } else { c.replySyntaxError(); return; }
        }
        List<Object> out = new ArrayList<>();
        int idx = 0, emitted = 0;
        for (Map.Entry<String, Double> e : z.entrySet()) {
            double s = e.getValue();
            if (s < min || s > max) continue;
            if (idx++ < limitOffset) continue;
            out.add(e.getKey());
            if (withscores) out.add(StringCommands.formatFloat(s));
            emitted++;
            if (limitCount >= 0 && emitted >= limitCount) break;
        }
        c.replyArray(out);
    }

    public static void zrevrangebyscore(RedisClient c) {
        // ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null || z.isEmpty()) { c.replyEmptyArray(); return; }
        Double max = parseBound(c.arg(2), false, false);
        Double min = parseBound(c.arg(3), true, false);
        if (min == null || max == null) { c.replyError("ERR min or max is not a float"); return; }

        boolean withscores = false;
        long limitOffset = 0, limitCount = -1;
        for (int j = 4; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("WITHSCORES")) withscores = true;
            else if (opt.equals("LIMIT")) {
                if (j + 2 >= c.argc()) { c.replySyntaxError(); return; }
                Long off = Args.parseLong(c, j + 1);
                Long cnt = Args.parseLong(c, j + 2);
                if (off == null || cnt == null) return;
                limitOffset = off; limitCount = cnt; j += 2;
            } else { c.replySyntaxError(); return; }
        }
        List<Map.Entry<String, Double>> entries = new ArrayList<>(z.entrySet());
        java.util.Collections.reverse(entries);
        List<Object> out = new ArrayList<>();
        int idx = 0, emitted = 0;
        for (Map.Entry<String, Double> e : entries) {
            double s = e.getValue();
            if (s < min || s > max) continue;
            if (idx++ < limitOffset) continue;
            out.add(e.getKey());
            if (withscores) out.add(StringCommands.formatFloat(s));
            emitted++;
            if (limitCount >= 0 && emitted >= limitCount) break;
        }
        c.replyArray(out);
    }

    public static void zrank(RedisClient c) { rankBy(c, false); }
    public static void zrevrank(RedisClient c) { rankBy(c, true); }

    private static void rankBy(RedisClient c, boolean rev) {
        // [Z]REVRANK key member [WITHSCORE]
        if (c.argc() < 3 || c.argc() > 4) { c.replyWrongArgc(); return; }
        boolean withscore = false;
        if (c.argc() == 4) {
            if (!c.arg(3).equalsIgnoreCase("WITHSCORE")) {
                c.replySyntaxError(); return;
            }
            withscore = true;
        }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null) { c.replyNil(); return; }
        String member = c.arg(2);
        if (!z.containsKey(member)) { c.replyNil(); return; }
        List<Map.Entry<String, Double>> entries = new ArrayList<>(z.entrySet());
        if (rev) java.util.Collections.reverse(entries);
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).getKey().equals(member)) {
                if (withscore) {
                    List<Object> out = new ArrayList<>(2);
                    out.add((long) i);
                    out.add(StringCommands.formatFloat(entries.get(i).getValue()));
                    c.replyArray(out);
                } else {
                    c.replyInteger(i);
                }
                return;
            }
        }
        c.replyNil();
    }

    // ===== ZPOPMIN / ZPOPMAX =====

    public static void zpopmin(RedisClient c) { zpop(c, false); }
    public static void zpopmax(RedisClient c) { zpop(c, true); }

    private static void zpop(RedisClient c, boolean max) {
        // ZPOPMIN/ZPOPMAX key [count]
        if (c.argc() < 2 || c.argc() > 3) { c.replyWrongArgc(); return; }
        int count = 1;
        if (c.argc() == 3) {
            Integer n = Args.parseInt(c, 2);
            if (n == null) return;
            count = n;
        }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null || z.isEmpty()) { c.replyEmptyArray(); return; }
        List<Map.Entry<String, Double>> entries = new ArrayList<>(z.entrySet());
        if (max) java.util.Collections.reverse(entries);
        List<Object> out = new ArrayList<>();
        int emitted = 0;
        for (Map.Entry<String, Double> e : entries) {
            if (emitted >= count) break;
            out.add(e.getKey());
            out.add(StringCommands.formatFloat(e.getValue()));
            z.remove(e.getKey());
            emitted++;
        }
        if (z.isEmpty()) c.storage().delete(c.arg(1));
        else             Values.saveZSet(c.storage(), c.arg(1), z, null);
        c.replyArray(out);
    }

    // ===== ZCOUNT / ZREMRANGEBY* =====

    public static void zcount(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null) { c.replyInteger(0); return; }
        Double min = parseBound(c.arg(2), true, false);
        Double max = parseBound(c.arg(3), false, false);
        if (min == null || max == null) { c.replyError("ERR min or max is not a float"); return; }
        long cnt = 0;
        for (Double s : z.values()) {
            if (s >= min && s <= max) cnt++;
        }
        c.replyInteger(cnt);
    }

    public static void zremrangebyrank(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null) { c.replyInteger(0); return; }
        int len = z.size();
        int start = parseIndex(c.arg(2), len);
        int stop  = parseIndex(c.arg(3), len);
        if (start < 0) start = 0;
        if (stop >= len) stop = len - 1;
        if (start > stop || start >= len) { c.replyInteger(0); return; }
        List<String> members = new ArrayList<>(z.keySet());
        int removed = 0;
        for (int i = start; i <= stop; i++) {
            z.remove(members.get(i));
            removed++;
        }
        if (z.isEmpty()) c.storage().delete(c.arg(1));
        else             Values.saveZSet(c.storage(), c.arg(1), z, null);
        c.replyInteger(removed);
    }

    public static void zremrangebyscore(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null) { c.replyInteger(0); return; }
        Double min = parseBound(c.arg(2), true, false);
        Double max = parseBound(c.arg(3), false, false);
        if (min == null || max == null) { c.replyError("ERR min or max is not a float"); return; }
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, Double> e : z.entrySet()) {
            double s = e.getValue();
            if (s >= min && s <= max) toRemove.add(e.getKey());
        }
        for (String m : toRemove) z.remove(m);
        if (z.isEmpty()) c.storage().delete(c.arg(1));
        else             Values.saveZSet(c.storage(), c.arg(1), z, null);
        c.replyInteger(toRemove.size());
    }

    // ===== ZUNIONSTORE / ZINTERSTORE / ZUNION / ZINTER / ZDIFF =====

    public static void zunionstore(RedisClient c) { setAggregate(c, true, false); }
    public static void zinterstore(RedisClient c) { setAggregate(c, false, false); }
    public static void zdiffstore(RedisClient c) { setAggregate(c, false, true); }

    private static void setAggregate(RedisClient c, boolean union, boolean diff) {
        // [Z xxxSTORE] dst numkeys src [src ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        Integer numKeys = Args.parseInt(c, 2);
        if (numKeys == null || numKeys < 1 || c.argc() < 3 + numKeys) {
            c.replyError("ERR at least 1 input key is needed"); return;
        }
        int n = numKeys;
        String dst = c.arg(1);
        String[] srcKeys = new String[n];
        for (int i = 0; i < n; i++) srcKeys[i] = c.arg(3 + i);

        double[] weights = new double[n];
        java.util.Arrays.fill(weights, 1.0);
        String aggregate = "SUM";

        int j = 3 + n;
        while (j < c.argc()) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("WEIGHTS")) {
                if (j + n >= c.argc()) { c.replySyntaxError(); return; }
                for (int i = 0; i < n; i++) {
                    Double w = Args.parseDouble(c, j + 1 + i);
                    if (w == null) return;
                    weights[i] = w;
                }
                j += 1 + n;
            } else if (opt.equals("AGGREGATE")) {
                if (j + 1 >= c.argc()) { c.replySyntaxError(); return; }
                aggregate = c.arg(j + 1).toUpperCase();
                if (!aggregate.equals("SUM") && !aggregate.equals("MIN") && !aggregate.equals("MAX")) {
                    c.replySyntaxError(); return;
                }
                j += 2;
            } else {
                c.replySyntaxError(); return;
            }
        }

        TreeMap<String, Double> result = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            TreeMap<String, Double> src;
            try {
                src = Values.getZSet(c.storage(), srcKeys[i]);
                if (src == null) {
                    // try as a SET (each member weight 1)
                    java.util.Set<String> set = null;
                    Object carrier = Values.getSet(c.storage(), srcKeys[i]);
                    if (carrier != null) {
                        set = new java.util.HashSet<>(Values.setAsList(carrier));
                    }
                    if (set != null) {
                        src = new TreeMap<>();
                        for (String m : set) src.put(m, 1.0);
                    }
                }
            } catch (Values.WrongTypeException wte) {
                c.replyError(wte.getMessage()); return;
            }
            if (src == null) continue;

            if (i == 0) {
                for (Map.Entry<String, Double> e : src.entrySet()) {
                    result.put(e.getKey(), e.getValue() * weights[i]);
                }
            } else {
                if (diff) {
                    for (String m : src.keySet()) result.remove(m);
                } else if (union) {
                    for (Map.Entry<String, Double> e : src.entrySet()) {
                        aggregateInto(result, e.getKey(), e.getValue() * weights[i], aggregate);
                    }
                } else { // inter
                    TreeMap<String, Double> retained = new TreeMap<>();
                    for (Map.Entry<String, Double> e : src.entrySet()) {
                        String m = e.getKey();
                        if (result.containsKey(m)) {
                            double s1 = result.get(m);
                            double s2 = e.getValue() * weights[i];
                            retained.put(m, aggregate.equals("MAX") ? Math.max(s1, s2)
                                    : aggregate.equals("MIN") ? Math.min(s1, s2) : s1 + s2);
                        }
                    }
                    result = retained;
                }
            }
        }

        c.storage().delete(dst);
        if (!result.isEmpty()) Values.saveZSet(c.storage(), dst, result, null);
        c.replyInteger(result.size());
    }

    private static void aggregateInto(TreeMap<String, Double> result, String member, double score, String aggregate) {
        if (!result.containsKey(member)) {
            result.put(member, score);
        } else {
            double cur = result.get(member);
            result.put(member, aggregate.equals("MAX") ? Math.max(cur, score)
                    : aggregate.equals("MIN") ? Math.min(cur, score) : cur + score);
        }
    }

    public static void zscan(RedisClient c) {
        // ZSCAN key cursor [MATCH pattern] [COUNT count]
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        String key = c.arg(1);
        String cursor = c.arg(2);
        String match = null;
        int count = ScanCursors.DEFAULT_COUNT;
        for (int j = 3; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("MATCH") && ++j < c.argc()) match = c.arg(j);
            else if (opt.equals("COUNT") && ++j < c.argc()) {
                Integer n = Args.parseInt(c, j);
                if (n == null) return;
                count = Math.max(1, n);
            } else { c.replySyntaxError(); return; }
        }
        TreeMap<String, Double> z;
        try {
            z = Values.getZSet(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (z == null || z.isEmpty()) {
            java.util.List<Object> done = new java.util.ArrayList<>(2);
            done.add("0");
            done.add(new java.util.ArrayList<>());
            c.replyArray(done);
            return;
        }
        List<String> members = new ArrayList<>(z.keySet());
        List<Object> result = ScanCursors.scan(cursor, members, count, match);
        @SuppressWarnings("unchecked")
        List<String> hits = (List<String>) result.get(1);
        List<Object> flat = new ArrayList<>(hits.size() * 2);
        for (String m : hits) {
            flat.add(m);
            flat.add(StringCommands.formatFloat(z.get(m)));
        }
        List<Object> out = new ArrayList<>(2);
        out.add(result.get(0));
        out.add(flat);
        c.replyArray(out);
    }

    // ===== Bounds parsing =====

    /** Parse a score bound like "3", "3.0", "(2.5", "-inf", "+inf". */
    private static Double parseBound(String s, boolean isMin, boolean reversed) {
        boolean exclusive = s.startsWith("(");
        if (exclusive) s = s.substring(1);
        double d;
        if (s.equalsIgnoreCase("inf") || s.equalsIgnoreCase("+inf")) d = Double.POSITIVE_INFINITY;
        else if (s.equalsIgnoreCase("-inf")) d = Double.NEGATIVE_INFINITY;
        else {
            try { d = Double.parseDouble(s); }
            catch (NumberFormatException e) { return null; }
        }
        // Exclusive bounds handled by tiny epsilon adjustment; not perfectly accurate but workable.
        if (exclusive) {
            if (isMin != reversed) d = Math.nextUp(d);
            else                   d = Math.nextDown(d);
        }
        return d;
    }

    private static int parseIndex(String s, int len) {
        int i;
        try { i = Integer.parseInt(s); }
        catch (NumberFormatException e) { return -1; }
        return i < 0 ? len + i : i;
    }
}
