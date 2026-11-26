package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hash commands: HSET/HGET/HGETALL/HDEL/HEXISTS/HLEN/HKEYS/HVALS/HSETNX/HMSET/
 * HMGET/HINCRBY/HINCRBYFLOAT/HRANDFIELD/HSTRLEN.
 */
public final class HashCommands {

    private HashCommands() {}

    public static void hset(RedisClient c) {
        // HSET key field value [field value ...]
        if (c.argc() < 4 || ((c.argc() - 2) % 2) != 0) {
            c.replyError("ERR wrong number of arguments for 'hset' command");
            return;
        }
        Object carrier;
        try {
            carrier = Values.getOrCreateHashCarrier(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        long added = 0;
        for (int j = 2; j < c.argc(); j += 2) {
            String field = c.arg(j), value = c.arg(j + 1);
            if (carrier instanceof io.github.redis.encoding.Listpack) {
                io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
                int idx = lp.indexOfKey(field);
                if (idx < 0) {
                    lp.append(new io.github.redis.encoding.Listpack.FieldValue(field, value));
                    added++;
                } else {
                    ((io.github.redis.encoding.Listpack.FieldValue) lp.get(idx)).value = value;
                }
            } else {
                @SuppressWarnings("unchecked")
                Map<String, String> m = (Map<String, String>) carrier;
                if (m.put(field, value) == null) added++;
            }
        }
        if (Values.hashShouldPromote(carrier)) {
            carrier = Values.promoteHashToMap(carrier);
        }
        Values.saveHashCarrier(c.storage(), c.arg(1), carrier, null);
        c.replyInteger(added);
    }

    public static void hsetnx(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Object carrier;
        try {
            carrier = Values.getOrCreateHashCarrier(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        String field = c.arg(2), value = c.arg(3);
        boolean inserted;
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
            if (lp.indexOfKey(field) >= 0) inserted = false;
            else {
                lp.append(new io.github.redis.encoding.Listpack.FieldValue(field, value));
                inserted = true;
            }
        } else {
            @SuppressWarnings("unchecked")
            Map<String, String> m = (Map<String, String>) carrier;
            inserted = (m.put(field, value) == null);
        }
        if (Values.hashShouldPromote(carrier)) {
            carrier = Values.promoteHashToMap(carrier);
        }
        Values.saveHashCarrier(c.storage(), c.arg(1), carrier, null);
        c.replyInteger(inserted ? 1 : 0);
    }

    public static void hmset(RedisClient c) {
        if (c.argc() < 4 || ((c.argc() - 2) % 2) != 0) {
            c.replyError("ERR wrong number of arguments for 'hmset' command");
            return;
        }
        Object carrier;
        try {
            carrier = Values.getOrCreateHashCarrier(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        for (int j = 2; j < c.argc(); j += 2) {
            String field = c.arg(j), value = c.arg(j + 1);
            if (carrier instanceof io.github.redis.encoding.Listpack) {
                io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
                int idx = lp.indexOfKey(field);
                if (idx < 0) {
                    lp.append(new io.github.redis.encoding.Listpack.FieldValue(field, value));
                } else {
                    ((io.github.redis.encoding.Listpack.FieldValue) lp.get(idx)).value = value;
                }
            } else {
                @SuppressWarnings("unchecked")
                Map<String, String> m = (Map<String, String>) carrier;
                m.put(field, value);
            }
        }
        if (Values.hashShouldPromote(carrier)) {
            carrier = Values.promoteHashToMap(carrier);
        }
        Values.saveHashCarrier(c.storage(), c.arg(1), carrier, null);
        c.replyOK();
    }

    public static void hget(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null) { c.replyNil(); return; }
        c.replyBulk(h.get(c.arg(2)));
    }

    public static void hmget(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        List<Object> out = new ArrayList<>(c.argc() - 2);
        for (int j = 2; j < c.argc(); j++) {
            out.add(h == null ? null : h.get(c.arg(j)));
        }
        c.replyArray(out);
    }

    public static void hgetall(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null || h.isEmpty()) { c.replyEmptyArray(); return; }
        // Flatten to [field1, value1, field2, value2, ...]
        List<Object> out = new ArrayList<>(h.size() * 2);
        for (Map.Entry<String, String> e : h.entrySet()) {
            out.add(e.getKey());
            out.add(e.getValue());
        }
        c.replyArray(out);
    }

    public static void hdel(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null) { c.replyInteger(0); return; }
        long removed = 0;
        for (int j = 2; j < c.argc(); j++) {
            if (h.remove(c.arg(j)) != null) removed++;
        }
        if (h.isEmpty()) c.storage().delete(c.arg(1));
        else             Values.saveHash(c.storage(), c.arg(1), h, null);
        c.replyInteger(removed);
    }

    public static void hexists(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        c.replyInteger(h != null && h.containsKey(c.arg(2)) ? 1 : 0);
    }

    public static void hlen(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        c.replyInteger(h == null ? 0 : h.size());
    }

    public static void hkeys(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null || h.isEmpty()) { c.replyEmptyArray(); return; }
        c.replyArray(new ArrayList<>(h.keySet()));
    }

    public static void hvals(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null || h.isEmpty()) { c.replyEmptyArray(); return; }
        c.replyArray(new ArrayList<>(h.values()));
    }

    public static void hstrlen(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null) { c.replyInteger(0); return; }
        String v = h.get(c.arg(2));
        c.replyInteger(v == null ? 0 : v.length());
    }

    public static void hincrby(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Long delta = Args.parseLong(c, 3);
        if (delta == null) return;
        Map<String, String> h;
        try {
            h = Values.getOrCreateHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        String field = c.arg(2);
        String cur = h.get(field);
        long val;
        if (cur == null) val = delta;
        else {
            try { val = Long.parseLong(cur) + delta; }
            catch (NumberFormatException e) {
                c.replyError("ERR hash value is not an integer");
                return;
            }
        }
        h.put(field, Long.toString(val));
        Values.saveHash(c.storage(), c.arg(1), h, null);
        c.replyInteger(val);
    }

    public static void hincrbyfloat(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Double delta = Args.parseDouble(c, 3);
        if (delta == null) return;
        Map<String, String> h;
        try {
            h = Values.getOrCreateHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        String field = c.arg(2);
        String cur = h.get(field);
        double val;
        if (cur == null) val = delta;
        else {
            try { val = Double.parseDouble(cur) + delta; }
            catch (NumberFormatException e) {
                c.replyError("ERR hash value is not a float");
                return;
            }
        }
        if (Double.isNaN(val) || Double.isInfinite(val)) {
            c.replyError("ERR increment would produce NaN or Infinity");
            return;
        }
        String s = StringCommands.formatFloat(val);
        h.put(field, s);
        Values.saveHash(c.storage(), c.arg(1), h, null);
        c.replyBulk(s);
    }

    public static void hscan(RedisClient c) {
        // HSCAN key cursor [MATCH pattern] [COUNT count]
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
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null || h.isEmpty()) {
            List<Object> done = new ArrayList<>(2);
            done.add("0");
            done.add(new ArrayList<>());
            c.replyArray(done);
            return;
        }
        // For HSCAN we scan the (field, value) pairs; MATCH applies to fields.
        List<String> fields = new ArrayList<>(h.keySet());
        List<Object> result = ScanCursors.scan(cursor, fields, count, match);
        @SuppressWarnings("unchecked")
        List<String> hitFields = (List<String>) result.get(1);
        List<Object> flat = new ArrayList<>(hitFields.size() * 2);
        for (String f : hitFields) {
            flat.add(f);
            flat.add(h.get(f));
        }
        List<Object> out = new ArrayList<>(2);
        out.add(result.get(0));
        out.add(flat);
        c.replyArray(out);
    }

    public static void hrandfield(RedisClient c) {
        // HRANDFIELD key [count [WITHVALUES]]
        if (c.argc() < 2 || c.argc() > 4) { c.replyWrongArgc(); return; }
        Map<String, String> h;
        try {
            h = Values.getHash(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (h == null || h.isEmpty()) { c.replyEmptyArray(); return; }

        Integer count = null;
        boolean withvalues = false;
        if (c.argc() >= 3) {
            count = Args.parseInt(c, 2);
            if (count == null) return;
        }
        if (c.argc() == 4) {
            if (!c.arg(3).equalsIgnoreCase("WITHVALUES")) {
                c.replySyntaxError(); return;
            }
            withvalues = true;
        }

        if (count == null) {
            // single random field
            List<String> keys = new ArrayList<>(h.keySet());
            c.replyBulk(keys.get((int) (Math.random() * keys.size())));
            return;
        }

        List<String> keys = new ArrayList<>(h.keySet());
        java.util.Collections.shuffle(keys);
        List<Object> out = new ArrayList<>();
        if (count >= 0) {
            int limit = Math.min(count, keys.size());
            for (int i = 0; i < limit; i++) {
                out.add(keys.get(i));
                if (withvalues) out.add(h.get(keys.get(i)));
            }
        } else {
            int abs = -count;
            for (int i = 0; i < abs; i++) {
                String k = keys.get((int) (Math.random() * keys.size()));
                out.add(k);
                if (withvalues) out.add(h.get(k));
            }
        }
        c.replyArray(out);
    }
}
