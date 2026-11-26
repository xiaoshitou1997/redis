package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.storage.ValueType;

import java.util.Set;

/**
 * Keyspace-management commands: EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT/TTL/PTTL/PERSIST/
 * TYPE/KEYS/RANDOMKEY/RENAME/RENAMENX/COPY/UNLINK (alias of DEL).
 *
 * SCAN is registered with a stub returning an empty cursor; real cursor-based
 * iteration lands in Phase 2.
 */
public final class KeyCommands {

    private KeyCommands() {}

    // ========== EXPIRE family ==========

    public static void expire(RedisClient c) {
        expireImpl(c, false, false);
    }
    public static void pexpire(RedisClient c) {
        expireImpl(c, true, false);
    }
    public static void expireat(RedisClient c) {
        expireImpl(c, false, true);
    }
    public static void pexpireat(RedisClient c) {
        expireImpl(c, true, true);
    }

    private static void expireImpl(RedisClient c, boolean millis, boolean absolute) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Long n = Args.parseLong(c, 2);
        if (n == null) return;

        String key = c.arg(1);
        if (!c.storage().exists(key)) { c.replyInteger(0); return; }

        long ttl;
        if (absolute) {
            long now = System.currentTimeMillis();
            long target = millis ? n : n * 1000L;
            ttl = target - now;
        } else {
            ttl = millis ? n : n * 1000L;
        }
        if (ttl <= 0) {
            c.storage().delete(key);
        } else {
            c.storage().expire(key, ttl);
        }
        c.replyInteger(1);
    }

    // ========== TTL / PTTL ==========

    public static void ttl(RedisClient c) { ttlImpl(c, false); }
    public static void pttl(RedisClient c) { ttlImpl(c, true); }

    private static void ttlImpl(RedisClient c, boolean millis) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        long ttlMillis = c.storage().ttl(c.arg(1));
        if (ttlMillis < 0) {
            c.replyInteger(ttlMillis); // -1 = no expiry, -2 = not exists
            return;
        }
        c.replyInteger(millis ? ttlMillis : (ttlMillis + 999) / 1000);
    }

    // ========== PERSIST ==========

    public static void persist(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        boolean ok = c.storage().persist(c.arg(1));
        c.replyInteger(ok ? 1 : 0);
    }

    // ========== TYPE ==========

    public static void type(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        ValueType t = Values.typeOf(c.storage(), c.arg(1));
        if (t == null) { c.replyBulk("none"); return; }
        switch (t) {
            case STRING:      c.replyBulk("string"); break;
            case HASH:        c.replyBulk("hash"); break;
            case LIST:        c.replyBulk("list"); break;
            case SET:         c.replyBulk("set"); break;
            case SORTED_SET:  c.replyBulk("zset"); break;
            default:          c.replyBulk("none");
        }
    }

    // ========== KEYS pattern ==========

    public static void keys(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        Set<String> keys = c.storage().keys(c.arg(1));
        c.replyArray(new java.util.ArrayList<>(keys));
    }

    // ========== RANDOMKEY ==========

    public static void randomkey(RedisClient c) {
        if (c.argc() != 1) { c.replyWrongArgc(); return; }
        Set<String> all = c.storage().keys();
        if (all.isEmpty()) { c.replyNil(); return; }
        String picked = all.iterator().next();
        c.replyBulk(picked);
    }

    // ========== RENAME / RENAMENX ==========

    public static void rename(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        String src = c.arg(1), dst = c.arg(2);
        io.github.redis.storage.RedisValue v = c.storage().get(src);
        if (v == null) { c.replyError("ERR no such key"); return; }
        long ttl = c.storage().ttl(src);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(dst, v.getValue(), v.getType(), ttlArg);
        c.storage().delete(src);
        c.replyOK();
    }

    public static void renamenx(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        String src = c.arg(1), dst = c.arg(2);
        io.github.redis.storage.RedisValue v = c.storage().get(src);
        if (v == null) { c.replyError("ERR no such key"); return; }
        if (c.storage().exists(dst)) { c.replyInteger(0); return; }
        long ttl = c.storage().ttl(src);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(dst, v.getValue(), v.getType(), ttlArg);
        c.storage().delete(src);
        c.replyInteger(1);
    }

    // ========== COPY ==========

    public static void copy(RedisClient c) {
        // COPY src dst [DB destination-db] [REPLACE]
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        String src = c.arg(1), dst = c.arg(2);
        boolean replace = false;
        for (int j = 3; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("REPLACE")) replace = true;
            else if (opt.equals("DB")) {
                if (++j >= c.argc()) { c.replySyntaxError(); return; }
                // Cross-db copy is not supported in this single-DB-aware layer; ignore.
            } else { c.replySyntaxError(); return; }
        }
        io.github.redis.storage.RedisValue v = c.storage().get(src);
        if (v == null) { c.replyInteger(0); return; }
        if (!replace && c.storage().exists(dst)) { c.replyInteger(0); return; }
        long ttl = c.storage().ttl(src);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(dst, v.getValue(), v.getType(), ttlArg);
        c.replyInteger(1);
    }

    // ========== SCAN — real cursor-based iteration ==========

    public static void scan(RedisClient c) {
        // SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String cursor = c.arg(1);
        String match = null;
        int count = ScanCursors.DEFAULT_COUNT;
        String typeFilter = null;
        for (int j = 2; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("MATCH") && ++j < c.argc()) match = c.arg(j);
            else if (opt.equals("COUNT") && ++j < c.argc()) {
                Integer n = Args.parseInt(c, j);
                if (n == null) return;
                count = Math.max(1, n);
            } else if (opt.equals("TYPE") && ++j < c.argc()) {
                typeFilter = c.arg(j).toLowerCase();
            } else { c.replySyntaxError(); return; }
        }
        java.util.Set<String> allKeys = c.storage().keys();
        if (typeFilter != null) {
            // Strip keys whose TYPE doesn't match.
            java.util.Iterator<String> it = allKeys.iterator();
            while (it.hasNext()) {
                ValueType t = Values.typeOf(c.storage(), it.next());
                String name = (t == null) ? "none" : t.name().toLowerCase().replace("_", "");
                if (!name.equals(typeFilter)) it.remove();
            }
        }
        java.util.List<Object> result = ScanCursors.scan(cursor, allKeys, count, match);
        // SCAN response shape: [cursor, [keys]]
        java.util.List<Object> out = new java.util.ArrayList<>(2);
        out.add(result.get(0));
        out.add(result.get(1));
        c.replyArray(out);
    }

    private static String wildcardToRegex(String pattern) {
        StringBuilder sb = new StringBuilder();
        for (char ch : pattern.toCharArray()) {
            switch (ch) {
                case '*': sb.append(".*"); break;
                case '?': sb.append('.'); break;
                case '.': case '+': case '(': case ')':
                case '[': case ']': case '{': case '}':
                case '^': case '$': case '|': case '\\':
                    sb.append('\\').append(ch); break;
                default:  sb.append(ch);
            }
        }
        return sb.toString();
    }
}
