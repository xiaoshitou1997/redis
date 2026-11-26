package io.github.redis.command.handler;

import io.github.redis.encoding.Encoding;
import io.github.redis.encoding.IntSet;
import io.github.redis.storage.RedisValue;
import io.github.redis.storage.Storage;
import io.github.redis.storage.ValueType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Type-safe accessors over {@link Storage} / {@link RedisValue}.
 *
 * <p>Storage stores values as opaque {@code Object}. This class centralises the
 * casts and WRONGTYPE checks so command handlers stay one-liners.
 *
 * <p>Getters return {@code null} when the key is missing; throw
 * {@link WrongTypeException} when the key holds a different type.
 */
public final class Values {

    private Values() {}

    public static final class WrongTypeException extends RuntimeException {
        public WrongTypeException() {
            super("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    // ========== String ==========

    /**
     * Read a string value. Uses Latin-1 (ISO-8859-1) encoding internally so the
     * returned String round-trips byte-perfectly through {@link #saveBinaryString}
     * — i.e. binary-safe for any byte sequence including invalid UTF-8.
     */
    public static String getString(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) return null;
        if (v.getType() != ValueType.STRING) throw new WrongTypeException();
        return (String) v.getValue();
    }

    /**
     * Read raw bytes of a string value, binary-safe. Use this when the caller
     * needs the exact byte sequence (e.g. BIT operations).
     */
    public static byte[] getStringBytes(Storage s, String key) {
        String str = getString(s, key);
        if (str == null) return null;
        return str.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
    }

    /**
     * Save a byte[] payload as a string value, binary-safe.
     */
    public static void saveBinaryString(Storage s, String key, byte[] bytes, Long ttl) {
        String carrier = new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1);
        s.set(key, carrier, ValueType.STRING, ttl);
    }

    // ========== Hash ==========

    public static Object getOrCreateHashCarrier(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) {
            // Start new hashes as Listpack (compact) — promoted to HashMap lazily.
            io.github.redis.encoding.Listpack lp = new io.github.redis.encoding.Listpack();
            RedisValue rv = new RedisValue(lp, ValueType.HASH);
            rv.setEncoding(io.github.redis.encoding.Encoding.LISTPACK);
            s.set(key, rv);
            return lp;
        }
        if (v.getType() != ValueType.HASH) throw new WrongTypeException();
        return v.getValue();
    }

    public static Object getHashCarrier(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) return null;
        if (v.getType() != ValueType.HASH) throw new WrongTypeException();
        return v.getValue();
    }

    /** Save the carrier and record its encoding name on the value. */
    public static void saveHashCarrier(Storage s, String key, Object carrier, Long ttl) {
        io.github.redis.encoding.Encoding enc =
                (carrier instanceof io.github.redis.encoding.Listpack)
                        ? io.github.redis.encoding.Encoding.LISTPACK
                        : io.github.redis.encoding.Encoding.HASHTABLE;
        RedisValue rv = new RedisValue(carrier, ValueType.HASH, ttl);
        rv.setEncoding(enc);
        s.set(key, rv);
    }

    /**
     * Promote a listpack-backed hash to a HashMap. No-op if already a map.
     * Returns the (possibly new) carrier.
     */
    public static Object promoteHashToMap(Object carrier) {
        if (!(carrier instanceof io.github.redis.encoding.Listpack)) return carrier;
        io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
        Map<String, String> promoted = new HashMap<>(lp.size() * 2);
        for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
            io.github.redis.encoding.Listpack.FieldValue fv = (io.github.redis.encoding.Listpack.FieldValue) e;
            promoted.put(fv.field, fv.value);
        }
        return promoted;
    }

    /** Returns true if the hash carrier should promote (size or value-length exceeded). */
    public static boolean hashShouldPromote(Object carrier) {
        if (!(carrier instanceof io.github.redis.encoding.Listpack)) return false;
        io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
        if (lp.size() > io.github.redis.encoding.Encoding.HASH_MAX_LISTPACK_ENTRIES) return true;
        for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
            if (e.maxElementBytes() > io.github.redis.encoding.Encoding.HASH_MAX_LISTPACK_VALUE) return true;
        }
        return false;
    }

    /** Convenience for legacy callers that still want a HashMap. */
    public static Map<String, String> getOrCreateHash(Storage s, String key) {
        Object carrier = getOrCreateHashCarrier(s, key);
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            // Caller expects a HashMap — promote eagerly for compatibility.
            io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
            Map<String, String> promoted = new HashMap<>(lp.size() * 2);
            for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
                io.github.redis.encoding.Listpack.FieldValue fv = (io.github.redis.encoding.Listpack.FieldValue) e;
                promoted.put(fv.field, fv.value);
            }
            saveHashCarrier(s, key, promoted, null);
            return promoted;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> m = (Map<String, String>) carrier;
        return m;
    }

    public static Map<String, String> getHash(Storage s, String key) {
        Object carrier = getHashCarrier(s, key);
        if (carrier == null) return null;
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
            Map<String, String> m = new HashMap<>(lp.size() * 2);
            for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
                io.github.redis.encoding.Listpack.FieldValue fv = (io.github.redis.encoding.Listpack.FieldValue) e;
                m.put(fv.field, fv.value);
            }
            return m;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> m = (Map<String, String>) carrier;
        return m;
    }

    public static void saveHash(Storage s, String key, Map<String, String> m, Long ttl) {
        saveHashCarrier(s, key, m, ttl);
    }

    // ========== List ==========

    public static LinkedList<String> getOrCreateList(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) {
            LinkedList<String> l = new LinkedList<>();
            s.set(key, l, ValueType.LIST, null);
            return l;
        }
        if (v.getType() != ValueType.LIST) throw new WrongTypeException();
        @SuppressWarnings("unchecked")
        LinkedList<String> l = (LinkedList<String>) v.getValue();
        return l;
    }

    public static LinkedList<String> getList(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) return null;
        if (v.getType() != ValueType.LIST) throw new WrongTypeException();
        @SuppressWarnings("unchecked")
        LinkedList<String> l = (LinkedList<String>) v.getValue();
        return l;
    }

    public static void saveList(Storage s, String key, LinkedList<String> l, Long ttl) {
        s.set(key, l, ValueType.LIST, ttl);
    }

    // ========== Set ==========

    /**
     * Get-or-create the underlying set carrier. Returns either an {@link IntSet}
     * (if the key is empty or already an intset) or a {@link HashSet} (once the
     * set has been promoted).
     *
     * <p>Callers must be prepared to receive either type — they iterate via
     * {@link #setAsObjectArray(Object)} / {@link #setContains(Object, String)} /
     * {@link #setAdd(Object, String)} rather than calling Set methods directly.
     */
    public static Object getOrCreateSet(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) {
            // Start new sets as IntSet — promoted lazily on first non-int member.
            IntSet is = new IntSet();
            RedisValue rv = new RedisValue(is, ValueType.SET);
            rv.setEncoding(Encoding.INTSET);
            s.set(key, rv);
            return is;
        }
        if (v.getType() != ValueType.SET) throw new WrongTypeException();
        return v.getValue();
    }

    public static Object getSet(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) return null;
        if (v.getType() != ValueType.SET) throw new WrongTypeException();
        return v.getValue();
    }

    public static void saveSet(Storage s, String key, Object carrier, Long ttl) {
        RedisValue existing = s.get(key);
        Encoding enc = (carrier instanceof IntSet) ? Encoding.INTSET : Encoding.HASHTABLE;
        if (existing != null) {
            existing.setEncoding(enc);
            // Re-set the carrier in-place: MemoryStorage.put replaces the value.
        }
        RedisValue rv = new RedisValue(carrier, ValueType.SET, ttl);
        rv.setEncoding(enc);
        s.set(key, rv);
    }

    /** Add a member to either an intset or a hashset. Returns true if added (was absent). */
    public static boolean setAdd(Object carrier, String member) {
        if (carrier instanceof IntSet) {
            if (!Encoding.isInteger(member)) return false; // caller must promote
            long v = Long.parseLong(member);
            return ((IntSet) carrier).add(v);
        }
        @SuppressWarnings("unchecked")
        Set<String> s = (Set<String>) carrier;
        return s.add(member);
    }

    public static boolean setRemove(Object carrier, String member) {
        if (carrier instanceof IntSet) {
            if (!Encoding.isInteger(member)) return false;
            long v = Long.parseLong(member);
            return ((IntSet) carrier).remove(v);
        }
        @SuppressWarnings("unchecked")
        Set<String> s = (Set<String>) carrier;
        return s.remove(member);
    }

    public static boolean setContains(Object carrier, String member) {
        if (carrier instanceof IntSet) {
            if (!Encoding.isInteger(member)) return false;
            return ((IntSet) carrier).contains(Long.parseLong(member));
        }
        @SuppressWarnings("unchecked")
        Set<String> s = (Set<String>) carrier;
        return s.contains(member);
    }

    public static int setSize(Object carrier) {
        if (carrier instanceof IntSet) return ((IntSet) carrier).size();
        return ((Set<?>) carrier).size();
    }

    public static java.util.List<String> setAsList(Object carrier) {
        if (carrier instanceof IntSet) {
            IntSet is = (IntSet) carrier;
            java.util.List<String> out = new java.util.ArrayList<>(is.size());
            long[] arr = is.toArray();
            for (long v : arr) out.add(Long.toString(v));
            return out;
        }
        @SuppressWarnings("unchecked")
        Set<String> s = (Set<String>) carrier;
        return new java.util.ArrayList<>(s);
    }

    /**
     * Promote an intset to a HashSet. No-op if already a HashSet.
     * Returns the (possibly new) carrier.
     */
    public static Object promoteToHashSet(Object carrier) {
        if (!(carrier instanceof IntSet)) return carrier;
        IntSet is = (IntSet) carrier;
        Set<String> promoted = new HashSet<>(is.size() * 2);
        for (long v : is.toArray()) promoted.add(Long.toString(v));
        return promoted;
    }

    /** Pick a random element from either carrier type. */
    public static String setRandom(Object carrier) {
        if (carrier instanceof IntSet) {
            IntSet is = (IntSet) carrier;
            if (is.isEmpty()) return null;
            return is.stringValue((int) (Math.random() * is.size()));
        }
        Set<?> s = (Set<?>) carrier;
        if (s.isEmpty()) return null;
        int idx = (int) (Math.random() * s.size());
        int i = 0;
        for (Object o : s) { if (i++ == idx) return (String) o; }
        return null;
    }

    // ========== Sorted Set ==========

    /**
     * Carrier for a sorted set — either a compact {@link io.github.redis.encoding.Listpack}
     * (when small &amp; members are short) or a {@code TreeMap<String,Double>} for the
     * Phase-3 baseline. The listpack carrier is used until either:
     * <ul>
     *   <li>entry count &gt; {@link io.github.redis.encoding.Encoding#ZSET_MAX_LISTPACK_ENTRIES}</li>
     *   <li>any member length &gt; {@link io.github.redis.encoding.Encoding#ZSET_MAX_LISTPACK_VALUE}</li>
     * </ul>
     */
    public static Object getOrCreateZSetCarrier(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) {
            io.github.redis.encoding.Listpack lp = new io.github.redis.encoding.Listpack();
            RedisValue rv = new RedisValue(lp, ValueType.SORTED_SET);
            rv.setEncoding(io.github.redis.encoding.Encoding.LISTPACK);
            s.set(key, rv);
            return lp;
        }
        if (v.getType() != ValueType.SORTED_SET) throw new WrongTypeException();
        return v.getValue();
    }

    public static Object getZSetCarrier(Storage s, String key) {
        RedisValue v = s.get(key);
        if (v == null) return null;
        if (v.getType() != ValueType.SORTED_SET) throw new WrongTypeException();
        return v.getValue();
    }

    public static void saveZSetCarrier(Storage s, String key, Object carrier, Long ttl) {
        io.github.redis.encoding.Encoding enc;
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            enc = io.github.redis.encoding.Encoding.LISTPACK;
        } else if (carrier instanceof io.github.redis.encoding.SkipList) {
            enc = io.github.redis.encoding.Encoding.SKIPLIST;
        } else {
            enc = io.github.redis.encoding.Encoding.SKIPLIST; // TreeMap fallback
        }
        RedisValue rv = new RedisValue(carrier, ValueType.SORTED_SET, ttl);
        rv.setEncoding(enc);
        s.set(key, rv);
    }

    /** Promote a listpack zset to a TreeMap (kept for backward compatibility with old handlers). */
    public static Object promoteZSetToTreeMap(Object carrier) {
        if (!(carrier instanceof io.github.redis.encoding.Listpack)) return carrier;
        io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
        TreeMap<String, Double> promoted = new TreeMap<>();
        for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
            io.github.redis.encoding.Listpack.ScoreMember sm = (io.github.redis.encoding.Listpack.ScoreMember) e;
            promoted.put(sm.member, sm.score);
        }
        return promoted;
    }

    /** Returns true if the zset carrier should promote (size or member-length exceeded). */
    public static boolean zsetShouldPromote(Object carrier) {
        if (!(carrier instanceof io.github.redis.encoding.Listpack)) return false;
        io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
        if (lp.size() > io.github.redis.encoding.Encoding.ZSET_MAX_LISTPACK_ENTRIES) return true;
        for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
            if (e.maxElementBytes() > io.github.redis.encoding.Encoding.ZSET_MAX_LISTPACK_VALUE) return true;
        }
        return false;
    }

    /** Convenience for legacy callers expecting TreeMap. Promotes listpack in-place. */
    @SuppressWarnings("unchecked")
    public static TreeMap<String, Double> getOrCreateZSet(Storage s, String key) {
        Object carrier = getOrCreateZSetCarrier(s, key);
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
            TreeMap<String, Double> promoted = new TreeMap<>();
            for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
                io.github.redis.encoding.Listpack.ScoreMember sm = (io.github.redis.encoding.Listpack.ScoreMember) e;
                promoted.put(sm.member, sm.score);
            }
            saveZSetCarrier(s, key, promoted, null);
            return promoted;
        }
        return (TreeMap<String, Double>) carrier;
    }

    @SuppressWarnings("unchecked")
    public static TreeMap<String, Double> getZSet(Storage s, String key) {
        Object carrier = getZSetCarrier(s, key);
        if (carrier == null) return null;
        if (carrier instanceof io.github.redis.encoding.Listpack) {
            io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) carrier;
            TreeMap<String, Double> m = new TreeMap<>();
            for (io.github.redis.encoding.Listpack.Entry e : lp.all()) {
                io.github.redis.encoding.Listpack.ScoreMember sm = (io.github.redis.encoding.Listpack.ScoreMember) e;
                m.put(sm.member, sm.score);
            }
            return m;
        }
        return (TreeMap<String, Double>) carrier;
    }

    public static void saveZSet(Storage s, String key, TreeMap<String, Double> z, Long ttl) {
        saveZSetCarrier(s, key, z, ttl);
    }

    // ========== Type helpers ==========

    public static ValueType typeOf(Storage s, String key) {
        RedisValue v = s.get(key);
        return v == null ? null : v.getType();
    }

    public static long sizeof(RedisValue v) {
        if (v == null || v.getValue() == null) return 0;
        Object o = v.getValue();
        if (o instanceof String) {
            // Approximate Redis SDS overhead: header + value bytes.
            return ((String) o).length() + 64;
        }
        if (o instanceof Map) {
            long n = 64; // dict header
            for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                n += sizeofString(e.getKey()) + sizeofString(e.getValue()) + 64;
            }
            return n;
        }
        if (o instanceof List) {
            long n = 64;
            for (Object x : (List<?>) o) n += sizeofString(x) + 16;
            return n;
        }
        if (o instanceof Set) {
            long n = 64;
            for (Object x : (Set<?>) o) n += sizeofString(x) + 16;
            return n;
        }
        return 64;
    }

    private static long sizeofString(Object o) {
        return o == null ? 0 : (o.toString().length() * 2L + 32);
    }
}
