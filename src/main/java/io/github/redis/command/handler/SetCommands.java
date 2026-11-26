package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.encoding.Encoding;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Set commands: SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SPOP/SMOVE/SDIFF/SUNION/SINTER
 * + their *STORE variants, SRANDMEMBER, SINTERCARD.
 *
 * <p>Phase-3 encoding: small integer-only sets are stored as {@link io.github.redis.encoding.IntSet}
 * (8-32 bytes overhead regardless of size). When a non-integer member is added
 * or size exceeds {@link Encoding#SET_MAX_INTSET_ENTRIES}, the carrier is
 * promoted to a {@link HashSet}. All handlers go through {@link Values} so they
 * don't need to care which carrier is currently active.
 */
public final class SetCommands {

    private SetCommands() {}

    public static void sadd(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        Object carrier;
        try {
            carrier = Values.getOrCreateSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        long added = 0;
        for (int j = 2; j < c.argc(); j++) {
            String m = c.arg(j);
            // If carrier is an IntSet and we hit a non-integer (or exceed size),
            // promote once and continue.
            if (carrier instanceof io.github.redis.encoding.IntSet) {
                boolean willPromote = false;
                if (!Encoding.isInteger(m)) willPromote = true;
                else if (((io.github.redis.encoding.IntSet) carrier).size() + 1 > Encoding.SET_MAX_INTSET_ENTRIES) willPromote = true;
                if (willPromote) {
                    carrier = Values.promoteToHashSet(carrier);
                }
            }
            if (Values.setAdd(carrier, m)) added++;
        }
        // Final post-add promotion check: if intset grew past threshold, promote.
        if (carrier instanceof io.github.redis.encoding.IntSet
                && ((io.github.redis.encoding.IntSet) carrier).size() > Encoding.SET_MAX_INTSET_ENTRIES) {
            carrier = Values.promoteToHashSet(carrier);
        }
        Values.saveSet(c.storage(), c.arg(1), carrier, null);
        c.replyInteger(added);
    }

    public static void srem(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (carrier == null) { c.replyInteger(0); return; }
        long removed = 0;
        for (int j = 2; j < c.argc(); j++) {
            if (Values.setRemove(carrier, c.arg(j))) removed++;
        }
        if (Values.setSize(carrier) == 0) c.storage().delete(c.arg(1));
        else                              Values.saveSet(c.storage(), c.arg(1), carrier, null);
        c.replyInteger(removed);
    }

    public static void smembers(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (carrier == null || Values.setSize(carrier) == 0) { c.replyEmptyArray(); return; }
        c.replyArray(Values.setAsList(carrier));
    }

    public static void sismember(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        c.replyInteger(carrier != null && Values.setContains(carrier, c.arg(2)) ? 1 : 0);
    }

    public static void smismember(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        List<Object> out = new ArrayList<>(c.argc() - 2);
        for (int j = 2; j < c.argc(); j++) {
            out.add(carrier != null && Values.setContains(carrier, c.arg(j)) ? 1L : 0L);
        }
        c.replyArray(out);
    }

    public static void scard(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        c.replyInteger(carrier == null ? 0 : Values.setSize(carrier));
    }

    public static void spop(RedisClient c) {
        if (c.argc() < 2 || c.argc() > 3) { c.replyWrongArgc(); return; }
        Integer count = null;
        if (c.argc() == 3) {
            count = Args.parseInt(c, 2);
            if (count == null) return;
            if (count < 0) { c.replyError("ERR value is out of range, must be positive"); return; }
        }
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (carrier == null || Values.setSize(carrier) == 0) {
            if (count == null) c.replyNil(); else c.replyEmptyArray();
            return;
        }
        if (count == null) {
            String picked = Values.setRandom(carrier);
            Values.setRemove(carrier, picked);
            if (Values.setSize(carrier) == 0) c.storage().delete(c.arg(1));
            else                              Values.saveSet(c.storage(), c.arg(1), carrier, null);
            c.replyBulk(picked);
        } else {
            List<Object> out = new ArrayList<>(Math.min(count, Values.setSize(carrier)));
            for (int i = 0; i < count && Values.setSize(carrier) > 0; i++) {
                String v = Values.setRandom(carrier);
                Values.setRemove(carrier, v);
                out.add(v);
            }
            if (Values.setSize(carrier) == 0) c.storage().delete(c.arg(1));
            else                              Values.saveSet(c.storage(), c.arg(1), carrier, null);
            c.replyArray(out);
        }
    }

    public static void smove(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Object src;
        try {
            src = Values.getSet(c.storage(), c.arg(1));
            // For dst we need to be careful: if both are intset, move a long; else promote dst.
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        String member = c.arg(3);
        if (src == null || !Values.setContains(src, member)) { c.replyInteger(0); return; }
        Object dst;
        try {
            dst = Values.getOrCreateSet(c.storage(), c.arg(2));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        // Promote dst if it can't hold this member type.
        if (dst instanceof io.github.redis.encoding.IntSet && !Encoding.isInteger(member)) {
            dst = Values.promoteToHashSet(dst);
        }
        Values.setAdd(dst, member);
        Values.setRemove(src, member);
        if (Values.setSize(src) == 0) c.storage().delete(c.arg(1));
        else                          Values.saveSet(c.storage(), c.arg(1), src, null);
        Values.saveSet(c.storage(), c.arg(2), dst, null);
        c.replyInteger(1);
    }

    public static void srandmember(RedisClient c) {
        if (c.argc() < 2 || c.argc() > 3) { c.replyWrongArgc(); return; }
        Integer count = null;
        if (c.argc() == 3) {
            count = Args.parseInt(c, 2);
            if (count == null) return;
        }
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (carrier == null || Values.setSize(carrier) == 0) {
            if (count == null) c.replyNil(); else c.replyEmptyArray();
            return;
        }
        if (count == null) {
            c.replyBulk(Values.setRandom(carrier));
            return;
        }
        List<String> pool = Values.setAsList(carrier);
        List<Object> out = new ArrayList<>(Math.abs(count));
        if (count >= 0) {
            java.util.Collections.shuffle(pool);
            for (int i = 0; i < Math.min(count, pool.size()); i++) out.add(pool.get(i));
        } else {
            int abs = -count;
            for (int i = 0; i < abs; i++) {
                out.add(pool.get((int) (Math.random() * pool.size())));
            }
        }
        c.replyArray(out);
    }

    // ========== Set algebra ==========

    public static void sdiff(RedisClient c) { setAlgebra(c, Algebra.DIFF, false); }
    public static void sunion(RedisClient c) { setAlgebra(c, Algebra.UNION, false); }
    public static void sinter(RedisClient c) { setAlgebra(c, Algebra.INTER, false); }
    public static void sdiffstore(RedisClient c) { setAlgebra(c, Algebra.DIFF, true); }
    public static void sunionstore(RedisClient c) { setAlgebra(c, Algebra.UNION, true); }
    public static void sinterstore(RedisClient c) { setAlgebra(c, Algebra.INTER, true); }

    private enum Algebra { DIFF, UNION, INTER }

    private static void setAlgebra(RedisClient c, Algebra op, boolean store) {
        int firstKeyIdx = store ? 2 : 1;
        if (c.argc() < firstKeyIdx + 1) { c.replyWrongArgc(); return; }
        String dst = store ? c.arg(1) : null;

        List<java.util.Set<String>> sets = new ArrayList<>();
        for (int j = firstKeyIdx; j < c.argc(); j++) {
            Object carrier;
            try {
                carrier = Values.getSet(c.storage(), c.arg(j));
            } catch (Values.WrongTypeException wte) {
                c.replyError(wte.getMessage()); return;
            }
            // Normalise to HashSet for the algebra pass (the cost is acceptable since
            // these are O(N) operations anyway).
            java.util.Set<String> s = new HashSet<>();
            if (carrier != null) s.addAll(Values.setAsList(carrier));
            sets.add(s);
        }
        java.util.Set<String> result = computeAlgebra(sets, op);

        if (store) {
            c.storage().delete(dst);
            if (!result.isEmpty()) {
                Values.saveSet(c.storage(), dst, new HashSet<>(result), null);
            }
            c.replyInteger(result.size());
        } else {
            c.replyArray(new ArrayList<>(result));
        }
    }

    private static java.util.Set<String> computeAlgebra(List<java.util.Set<String>> sets, Algebra op) {
        if (sets.isEmpty()) return new HashSet<>();
        java.util.Set<String> first = new HashSet<>(sets.get(0));
        switch (op) {
            case UNION: {
                for (int i = 1; i < sets.size(); i++) first.addAll(sets.get(i));
                return first;
            }
            case INTER: {
                for (int i = 1; i < sets.size(); i++) first.retainAll(sets.get(i));
                return first;
            }
            case DIFF: {
                for (int i = 1; i < sets.size(); i++) first.removeAll(sets.get(i));
                return first;
            }
            default: return first;
        }
    }

    public static void sscan(RedisClient c) {
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
        Object carrier;
        try {
            carrier = Values.getSet(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (carrier == null || Values.setSize(carrier) == 0) {
            List<Object> done = new ArrayList<>(2);
            done.add("0");
            done.add(new ArrayList<>());
            c.replyArray(done);
            return;
        }
        List<Object> result = ScanCursors.scan(cursor, Values.setAsList(carrier), count, match);
        List<Object> out = new ArrayList<>(2);
        out.add(result.get(0));
        out.add(result.get(1));
        c.replyArray(out);
    }
}
