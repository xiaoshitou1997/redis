package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.storage.ValueType;

import java.util.ArrayList;
import java.util.List;

/**
 * String-type commands — SET/GET/INCR/DECR/APPEND/STRLEN/MGET/MSET/SETEX/SETNX/
 * INCRBY/DECRBY/INCRBYFLOAT/GETRANGE/SETRANGE/GETSET.
 *
 * <p>Each method is the body of a Redis-style command handler: takes a
 * {@link RedisClient}, parses argv inline, replies directly via
 * {@code c.replyXXX(...)}.
 */
public final class StringCommands {

    private StringCommands() {}

    // ========== SET key value [EX seconds|PX millis|EXAT ts|PXAT ts] [KEEPTTL] [NX|XX] [GET] ==========

    public static void set(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }

        String key = c.arg(1);
        String value = c.arg(2);

        long ttlMillis = -1;          // -1 = no TTL set by this command
        boolean keepttl = false;
        boolean nx = false, xx = false, get = false;

        for (int j = 3; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            switch (opt) {
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "KEEPTTL": keepttl = true; break;
                case "GET": get = true; break;
                case "EX":
                case "PX":
                case "EXAT":
                case "PXAT":
                    if (++j >= c.argc()) { c.replySyntaxError(); return; }
                    Long n = Args.parseLong(c, j);
                    if (n == null) return;
                    if (opt.equals("EX")) ttlMillis = n * 1000L;
                    else if (opt.equals("PX")) ttlMillis = n;
                    else if (opt.equals("EXAT")) ttlMillis = n * 1000L - System.currentTimeMillis();
                    else ttlMillis = n - System.currentTimeMillis();
                    break;
                default:
                    c.replySyntaxError();
                    return;
            }
        }

        if (nx && xx) {
            c.replyError("ERR NX and XX options at the same time are not compatible");
            return;
        }
        if (keepttl && ttlMillis > 0) {
            c.replyError("ERR syntax error, KEEPTTL and EX/PX/EXAT/PXAT options are mutually exclusive");
            return;
        }

        // Capture previous value if GET is requested (or for type check)
        String prev = null;
        boolean existed = false;
        if (get) {
            try {
                prev = Values.getString(c.storage(), key);
                existed = prev != null;
            } catch (Values.WrongTypeException wte) {
                c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }
        } else {
            existed = c.storage().exists(key);
        }

        if (nx && existed) {
            if (get) c.replyBulk(prev); else c.replyNil();
            return;
        }
        if (xx && !existed) {
            if (get) c.replyNil(); else c.replyNil();
            return;
        }

        Long ttl = ttlMillis > 0 ? ttlMillis : null;
        if (keepttl && !get) {
            // preserve existing TTL: read current ttl, then re-set with it
            long currentTtl = c.storage().ttl(key);
            ttl = currentTtl > 0 ? currentTtl : null;
        }
        c.storage().set(key, value, ValueType.STRING, ttl);

        if (get) c.replyBulk(prev);
        else c.replyOK();
    }

    // ========== GET key ==========

    public static void get(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        String v;
        try {
            v = Values.getString(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        c.replyBulk(v);
    }

    // ========== DEL / UNLINK (treated as DEL) ==========

    public static void del(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        long count = 0;
        for (int j = 1; j < c.argc(); j++) {
            if (c.storage().delete(c.arg(j))) count++;
        }
        c.replyInteger(count);
    }

    // ========== EXISTS ==========

    public static void exists(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        long count = 0;
        for (int j = 1; j < c.argc(); j++) {
            if (c.storage().exists(c.arg(j))) count++;
        }
        c.replyInteger(count);
    }

    // ========== INCR / DECR / INCRBY / DECRBY ==========

    public static void incr(RedisClient c) { incrBy(c, 1); }
    public static void decr(RedisClient c) { incrBy(c, -1); }

    public static void incrby(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Long delta = Args.parseLong(c, 2);
        if (delta == null) return;
        incrBy(c, delta);
    }

    public static void decrby(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Long delta = Args.parseLong(c, 2);
        if (delta == null) return;
        incrBy(c, -delta);
    }

    private static void incrBy(RedisClient c, long delta) {
        String key;
        if (c.command().equals("INCR") || c.command().equals("DECR")) {
            if (c.argc() != 2) { c.replyWrongArgc(); return; }
            key = c.arg(1);
        } else {
            key = c.arg(1);
        }
        String cur;
        try {
            cur = Values.getString(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        long val;
        if (cur == null) {
            val = delta;
        } else {
            try {
                val = Long.parseLong(cur) + delta;
            } catch (NumberFormatException e) {
                c.replyError("ERR value is not an integer or out of range");
                return;
            }
        }
        // Preserve TTL
        long ttl = c.storage().ttl(key);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(key, Long.toString(val), ValueType.STRING, ttlArg);
        c.replyInteger(val);
    }

    // ========== INCRBYFLOAT ==========

    public static void incrbyfloat(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Double delta = Args.parseDouble(c, 2);
        if (delta == null) return;

        String key = c.arg(1);
        String cur;
        try {
            cur = Values.getString(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        double val;
        if (cur == null) {
            val = delta;
        } else {
            try {
                val = Double.parseDouble(cur) + delta;
            } catch (NumberFormatException e) {
                c.replyError("ERR value is not a valid float");
                return;
            }
        }
        if (Double.isNaN(val) || Double.isInfinite(val)) {
            c.replyError("ERR increment would produce NaN or Infinity");
            return;
        }
        String s = formatFloat(val);
        long ttl = c.storage().ttl(key);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(key, s, ValueType.STRING, ttlArg);
        c.replyBulk(s);
    }

    // ========== APPEND key value ==========

    public static void append(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        String key = c.arg(1), suffix = c.arg(2);
        String cur;
        try {
            cur = Values.getString(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        String result = cur == null ? suffix : cur + suffix;
        long ttl = c.storage().ttl(key);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(key, result, ValueType.STRING, ttlArg);
        c.replyInteger(result.length());
    }

    // ========== STRLEN ==========

    public static void strlen(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        String v;
        try {
            v = Values.getString(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        c.replyInteger(v == null ? 0 : v.length());
    }

    // ========== GETSET ==========

    public static void getset(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        String key = c.arg(1), value = c.arg(2);
        String prev;
        try {
            prev = Values.getString(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        c.storage().set(key, value, ValueType.STRING, null);
        c.replyBulk(prev);
    }

    // ========== SETEX / SETNX / PSETEX ==========

    public static void setex(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Long seconds = Args.parseLong(c, 2);
        if (seconds == null) return;
        if (seconds <= 0) { c.replyError("ERR invalid expire time in 'setex' command"); return; }
        c.storage().set(c.arg(1), c.arg(3), ValueType.STRING, seconds * 1000L);
        c.replyOK();
    }

    public static void psetex(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Long ms = Args.parseLong(c, 2);
        if (ms == null) return;
        if (ms <= 0) { c.replyError("ERR invalid expire time in 'psetex' command"); return; }
        c.storage().set(c.arg(1), c.arg(3), ValueType.STRING, ms);
        c.replyOK();
    }

    public static void setnx(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        String key = c.arg(1), value = c.arg(2);
        if (c.storage().exists(key)) {
            c.replyInteger(0);
            return;
        }
        c.storage().set(key, value, ValueType.STRING, null);
        c.replyInteger(1);
    }

    // ========== MGET / MSET ==========

    public static void mget(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        List<Object> out = new ArrayList<>(c.argc() - 1);
        for (int j = 1; j < c.argc(); j++) {
            String v;
            try {
                v = Values.getString(c.storage(), c.arg(j));
            } catch (Values.WrongTypeException wte) {
                v = null; // MGET treats wrong-type keys as nil
            }
            out.add(v);
        }
        c.replyArray(out);
    }

    public static void mset(RedisClient c) {
        if (c.argc() < 3 || (c.argc() % 2) != 1) {
            c.replyError("ERR wrong number of arguments for 'mset' command");
            return;
        }
        for (int j = 1; j < c.argc(); j += 2) {
            c.storage().set(c.arg(j), c.arg(j + 1), ValueType.STRING, null);
        }
        c.replyOK();
    }

    public static void msetnx(RedisClient c) {
        if (c.argc() < 3 || (c.argc() % 2) != 1) {
            c.replyError("ERR wrong number of arguments for 'msetnx' command");
            return;
        }
        for (int j = 1; j < c.argc(); j += 2) {
            if (c.storage().exists(c.arg(j))) {
                c.replyInteger(0);
                return;
            }
        }
        for (int j = 1; j < c.argc(); j += 2) {
            c.storage().set(c.arg(j), c.arg(j + 1), ValueType.STRING, null);
        }
        c.replyInteger(1);
    }

    // ========== GETRANGE / SETRANGE ==========

    public static void getrange(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Integer start = Args.parseInt(c, 2);
        Integer end = Args.parseInt(c, 3);
        if (start == null || end == null) return;

        String v;
        try {
            v = Values.getString(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        if (v == null || v.isEmpty()) { c.replyBulk(""); return; }

        int len = v.length();
        if (start < 0) start = Math.max(0, len + start);
        if (end < 0) end = len + end;
        if (start > end || start >= len) { c.replyBulk(""); return; }
        if (end >= len) end = len - 1;
        c.replyBulk(v.substring(start, end + 1));
    }

    public static void setrange(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Integer offset = Args.parseInt(c, 2);
        if (offset == null) return;
        if (offset < 0) { c.replyError("ERR offset is out of range"); return; }

        String key = c.arg(1), value = c.arg(3);
        String cur;
        try {
            cur = Values.getString(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        StringBuilder sb = new StringBuilder(cur == null ? "" : cur);
        while (sb.length() < offset) sb.append('\0');
        sb.replace(offset, Math.max(offset, sb.length()), value);
        String result = sb.toString();
        long ttl = c.storage().ttl(key);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(key, result, ValueType.STRING, ttlArg);
        c.replyInteger(result.length());
    }

    // ========== GETDEL ==========

    public static void getdel(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        String key = c.arg(1);
        String prev;
        try {
            prev = Values.getString(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        if (prev != null) c.storage().delete(key);
        c.replyBulk(prev);
    }

    // ========== helpers ==========

    static String formatFloat(double d) {
        if (d == Math.rint(d) && !Double.isInfinite(d)) {
            return Long.toString((long) d);
        }
        return Double.toString(d);
    }
}
