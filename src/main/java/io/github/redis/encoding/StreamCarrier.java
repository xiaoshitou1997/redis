package io.github.redis.encoding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory Redis Stream carrier.
 *
 * <p>A stream is an append-only log of {@link Entry} objects, each identified
 * by a monotonically increasing ID of the form {@code <ms>-<seq>}. Entries
 * hold ordered field/value pairs.
 *
 * <p>Phase-4 implementation: a simple {@code ArrayList} of entries plus an
 * index from ID to position. Memory is not optimised — that's Phase 5+
 * territory (listpack chunks, radix tree, etc.).
 *
 * <p>Consumer groups ({@code XGROUP}/{@code XREADGROUP}/{@code XACK}/{@code XPENDING})
 * are tracked here so they survive across commands. Each group keeps its
 * last-delivered-id and a pending-entries-list (PEL) of messages delivered but
 * not yet acknowledged.
 */
public final class StreamCarrier {

    private final List<Entry> entries = new ArrayList<>();
    private final Map<String, Integer> idIndex = new LinkedHashMap<>();
    private final Map<String, ConsumerGroup> groups = new LinkedHashMap<>();
    private StreamId lastId = new StreamId(0, 0);
    private long maxDeletedId = 0;

    public int size() { return entries.size(); }
    public boolean isEmpty() { return entries.isEmpty(); }
    public StreamId lastId() { return lastId; }
    public long maxDeletedId() { return maxDeletedId; }

    /** Append a new entry. Caller is responsible for ID generation / collision checks. */
    public synchronized StreamId append(StreamId id, List<String> fieldValues) {
        entries.add(new Entry(id, fieldValues));
        idIndex.put(id.toString(), entries.size() - 1);
        if (id.compareTo(lastId) > 0) lastId = id;
        return id;
    }

    /** Remove the smallest entries until size == maxlen. Returns the count trimmed. */
    public synchronized int trim(int maxlen, boolean approximate) {
        if (entries.size() <= maxlen) return 0;
        int toRemove = entries.size() - maxlen;
        // Remove from the head.
        for (int i = 0; i < toRemove; i++) {
            Entry e = entries.remove(0);
            idIndex.remove(e.id.toString());
            long ms = e.id.ms;
            if (ms > maxDeletedId) maxDeletedId = ms;
        }
        // Rebuild id-index positions (positions shifted).
        rebuildIndex();
        return toRemove;
    }

    /** Remove a specific entry by ID. Returns true if found. */
    public synchronized boolean delete(String idStr) {
        Integer idx = idIndex.remove(idStr);
        if (idx == null) return false;
        Entry e = entries.remove((int) idx);
        if (e != null) {
            long ms = e.id.ms;
            if (ms > maxDeletedId) maxDeletedId = ms;
        }
        rebuildIndex();
        return true;
    }

    private void rebuildIndex() {
        idIndex.clear();
        for (int i = 0; i < entries.size(); i++) {
            idIndex.put(entries.get(i).id.toString(), i);
        }
    }

    /** Return entries in [start, end] inclusive. Stream IDs compare lexicographically in this format. */
    public synchronized List<Entry> range(StreamId start, StreamId end, int count, boolean reverse) {
        List<Entry> out = new ArrayList<>();
        Iterable<Entry> iter = reverse ? () -> {
            List<Entry> rev = new ArrayList<>(entries);
            Collections.reverse(rev);
            return rev.iterator();
        } : () -> entries.iterator();
        for (Entry e : iter) {
            if (e.id.compareTo(start) < 0) continue;
            if (e.id.compareTo(end) > 0) continue;
            out.add(e);
            if (count > 0 && out.size() >= count) break;
        }
        return out;
    }

    /** Return entries with id > start (or >= if startInclusive). */
    public synchronized List<Entry> readAfter(StreamId start, int count) {
        List<Entry> out = new ArrayList<>();
        for (Entry e : entries) {
            if (e.id.compareTo(start) <= 0) continue;
            out.add(e);
            if (count > 0 && out.size() >= count) break;
        }
        return out;
    }

    public synchronized Entry get(String idStr) {
        Integer idx = idIndex.get(idStr);
        return idx == null ? null : entries.get(idx);
    }

    // ===== Consumer groups =====

    public synchronized ConsumerGroup getOrCreateGroup(String group) {
        return groups.computeIfAbsent(group, g -> new ConsumerGroup(group, lastId));
    }

    public synchronized ConsumerGroup getGroup(String group) {
        return groups.get(group);
    }

    public synchronized boolean destroyGroup(String group) {
        return groups.remove(group) != null;
    }

    public synchronized List<ConsumerGroup> groups() {
        return new ArrayList<>(groups.values());
    }

    /** One log entry: ID + ordered (field, value) pairs. */
    public static final class Entry {
        public final StreamId id;
        public final List<String> fields; // flat: [f1, v1, f2, v2, ...]

        public Entry(StreamId id, List<String> fields) {
            this.id = id;
            this.fields = fields;
        }
    }

    /** {@code <ms>-<seq>} ID. */
    public static final class StreamId implements Comparable<StreamId> {
        public final long ms;
        public final long seq;

        public StreamId(long ms, long seq) {
            this.ms = ms;
            this.seq = seq;
        }

        /** Parse "<ms>-<seq>" form. Returns null if invalid. */
        public static StreamId parse(String s) {
            if (s == null) return null;
            int dash = s.indexOf('-');
            try {
                if (dash < 0) {
                    return new StreamId(Long.parseLong(s), 0);
                }
                return new StreamId(
                        Long.parseLong(s.substring(0, dash)),
                        Long.parseLong(s.substring(dash + 1)));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        @Override
        public int compareTo(StreamId o) {
            int byMs = Long.compare(ms, o.ms);
            return byMs != 0 ? byMs : Long.compare(seq, o.seq);
        }

        @Override
        public String toString() {
            return ms + "-" + seq;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StreamId)) return false;
            StreamId other = (StreamId) o;
            return ms == other.ms && seq == other.seq;
        }

        @Override
        public int hashCode() {
            return (int) (ms * 31 + seq);
        }
    }

    /** A consumer group within a stream. */
    public static final class ConsumerGroup {
        public final String name;
        public StreamId lastDeliveredId;
        public final Map<String, PelEntry> pending = new LinkedHashMap<>(); // id → PEL
        public final Map<String, Consumer> consumers = new LinkedHashMap<>();

        public ConsumerGroup(String name, StreamId lastId) {
            this.name = name;
            this.lastDeliveredId = lastId;
        }

        public Consumer getOrCreateConsumer(String consumer) {
            return consumers.computeIfAbsent(consumer, c -> new Consumer(c));
        }
    }

    /** Per-consumer state inside a group. */
    public static final class Consumer {
        public final String name;
        public long seenTime = System.currentTimeMillis();
        public long activeTime = System.currentTimeMillis();
        public final List<String> pendingIds = new ArrayList<>(); // ids this consumer holds

        public Consumer(String name) { this.name = name; }
    }

    /** Pending entry list record: who delivered it, when, delivery count. */
    public static final class PelEntry {
        public StreamId id;
        public String consumer;
        public long deliveryTime;
        public int deliveryCount;

        public PelEntry(StreamId id, String consumer, long deliveryTime, int deliveryCount) {
            this.id = id;
            this.consumer = consumer;
            this.deliveryTime = deliveryTime;
            this.deliveryCount = deliveryCount;
        }
    }
}
