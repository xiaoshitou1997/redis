package io.github.redis.encoding;

/**
 * Internal string encodings, mirroring Redis's {@code OBJ_ENCODING_*} constants.
 *
 * <p>Each value is the wire name reported by {@code OBJECT ENCODING}:
 * <ul>
 *   <li>{@code int} — short numeric string stored as a 64-bit signed integer</li>
 *   <li>{@code embstr} — short string embedded in the object header (one allocation)</li>
 *   <li>{@code raw} — pointer to a separately allocated byte buffer</li>
 *   <li>{@code ziplist} / {@code listpack} — compact serialized form for small collections</li>
 *   <li>{@code intset} — sorted array of integers for an all-integer set</li>
 *   <li>{@code hashtable} — full {@code java.util.HashMap}-backed hash</li>
 *   <li>{@code linkedlist} — Java {@code LinkedList}-backed list (Phase 3 placeholder for quicklist)</li>
 *   <li>{@code skiplist} — skiplist+dict-backed sorted set</li>
 * </ul>
 */
public enum Encoding {
    INT("int"),
    EMBSTR("embstr"),
    RAW("raw"),
    ZIPLIST("ziplist"),
    LISTPACK("listpack"),
    INTSET("intset"),
    HASHTABLE("hashtable"),
    LINKEDLIST("linkedlist"),
    SKIPLIST("skiplist"),
    QUICKLIST("quicklist");

    private final String wireName;

    Encoding(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    /** Redis cutoff: strings up to 44 bytes use embstr. */
    public static final int EMBSTR_MAX_LEN = 44;
    /** Redis default: hash with at most this many entries stays as listpack. */
    public static final int HASH_MAX_LISTPACK_ENTRIES = 128;
    /** Redis default: hash with any entry value longer than this is promoted to hashtable. */
    public static final int HASH_MAX_LISTPACK_VALUE = 64;
    /** Redis default: zset with at most this many entries stays as listpack. */
    public static final int ZSET_MAX_LISTPACK_ENTRIES = 128;
    /** Redis default: zset with any element longer than this is promoted to skiplist. */
    public static final int ZSET_MAX_LISTPACK_VALUE = 64;
    /** Redis default: list max listpack size (negative = size in KiB, positive = entries). */
    public static final int LIST_MAX_LISTPACK_SIZE = -2;
    /** Redis default: list with more than this many listpack nodes becomes quicklist. */
    public static final int LIST_MAX_ZIPLIST_ENTRIES = 128;
    /** Redis default: set of all-integers with at most this many stays as intset. */
    public static final int SET_MAX_INTSET_ENTRIES = 512;
    /** Redis default: stream node max size in bytes (used for listpack chunking). */
    public static final int STREAM_NODE_MAX_BYTES = 4096;
    /** Redis default: stream node max entries. */
    public static final int STREAM_NODE_MAX_ENTRIES = 100;
    /** Redis default: stream default MAXLEN for XADD with no trimming. */
    public static final int STREAM_DEFAULT_MAX_LEN = 8192;

    /**
     * Best-effort initial encoding guess based on the Java carrier type and value
     * shape. Used by {@link io.github.redis.storage.RedisValue} on construction.
     * The actual handler may promote it (e.g. SET-as-intset → SET-as-hashtable)
     * once size or element-type thresholds are crossed.
     */
    public static Encoding guess(Object value, io.github.redis.storage.ValueType type) {
        if (value == null) return RAW;
        switch (type) {
            case STRING: {
                String s = (value instanceof String) ? (String) value : String.valueOf(value);
                // Try int encoding first.
                if (isInteger(s)) return INT;
                // embstr for short strings.
                if (s.length() <= EMBSTR_MAX_LEN) return EMBSTR;
                return RAW;
            }
            case HASH:    return LISTPACK;
            case LIST:    return LINKEDLIST;
            case SET:     return INTSET;     // refined on first non-int insert
            case SORTED_SET: return LISTPACK;
            default:      return RAW;
        }
    }

    /** Redis considers a string an integer if it parses as a base-10 long and uses no leading zeros (except "0" itself) or sign-only. */
    public static boolean isInteger(String s) {
        if (s == null || s.isEmpty()) return false;
        // Fast path: must be all digits with optional leading sign.
        int i = 0;
        if (s.charAt(0) == '-' || s.charAt(0) == '+') {
            if (s.length() == 1) return false;
            i = 1;
        }
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        try {
            Long.parseLong(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
