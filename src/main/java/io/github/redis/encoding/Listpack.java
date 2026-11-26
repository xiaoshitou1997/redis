package io.github.redis.encoding;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory listpack — a compact, ordered list of byte-string entries used to
 * back small hashes, lists, and zsets before they grow large enough to promote
 * to the canonical (HashMap / skiplist / linkedlist) carrier.
 *
 * <p>This is <b>not</b> a byte-for-byte port of Redis's serialized listpack
 * (that's a Phase-3.5 concern for RDB persistence). It's a Java-side carrier
 * that gives us the same big-O memory footprint: roughly 16 bytes overhead per
 * entry regardless of value size, versus the 48-64 bytes per entry of a
 * {@code HashMap} or {@code TreeMap} node.
 *
 * <p>API supports three layouts via {@link Entry} subtypes:
 * <ul>
 *   <li>{@link SingleEntry} — a single string (used for LIST carrier)</li>
 *   <li>{@link FieldValue} — (field, value) pair (used for HASH carrier)</li>
 *   <li>{@link ScoreMember} — (member, score) pair (used for ZSET carrier)</li>
 * </ul>
 *
 * <p>Searches are linear — that's why we promote to HashMap / skiplist once
 * the entry count exceeds {@link Encoding#HASH_MAX_LISTPACK_ENTRIES} /
 * {@link Encoding#ZSET_MAX_LISTPACK_ENTRIES}.
 */
public final class Listpack {

    /** One entry per slot. */
    private final List<Entry> entries = new ArrayList<>();

    public int size() { return entries.size(); }
    public boolean isEmpty() { return entries.isEmpty(); }

    public void append(Entry e) { entries.add(e); }
    public void prepend(Entry e) { entries.add(0, e); }
    public void clear() { entries.clear(); }

    public Entry get(int i) { return entries.get(i); }
    public void set(int i, Entry e) { entries.set(i, e); }
    public void remove(int i) { entries.remove(i); }
    public List<Entry> all() { return entries; }

    /** Returns the index of the entry whose key matches, or -1. */
    public int indexOfKey(String key) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).key().equals(key)) return i;
        }
        return -1;
    }

    /** Returns the index of the entry whose member matches, or -1. */
    public int indexOfMember(String member) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).key().equals(member)) return i;
        }
        return -1;
    }

    /** Common interface for all listpack entries. */
    public interface Entry {
        /** Key used for lookup (field for HASH, member for ZSET, value for LIST). */
        String key();
        /** Length of the largest string carried by this entry — used for promotion checks. */
        int maxElementBytes();
    }

    /** Plain single-string entry (LIST carrier). */
    public static final class SingleEntry implements Entry {
        public String value;
        public SingleEntry(String v) { this.value = v; }
        @Override public String key() { return value; }
        @Override public int maxElementBytes() { return value == null ? 0 : value.length(); }
    }

    /** (field, value) entry for HASH carrier. */
    public static final class FieldValue implements Entry {
        public String field;
        public String value;
        public FieldValue(String f, String v) { this.field = f; this.value = v; }
        @Override public String key() { return field; }
        @Override public int maxElementBytes() {
            int fl = field == null ? 0 : field.length();
            int vl = value == null ? 0 : value.length();
            return Math.max(fl, vl);
        }
    }

    /** (member, score) entry for ZSET carrier. Keyed by member, sorted by score. */
    public static final class ScoreMember implements Entry {
        public String member;
        public double score;
        public ScoreMember(String m, double s) { this.member = m; this.score = s; }
        @Override public String key() { return member; }
        @Override public int maxElementBytes() { return member == null ? 0 : member.length(); }
    }
}
