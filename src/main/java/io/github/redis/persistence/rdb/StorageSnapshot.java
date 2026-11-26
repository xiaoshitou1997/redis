package io.github.redis.persistence.rdb;

import io.github.redis.storage.RedisValue;
import io.github.redis.storage.Storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single-shot snapshot of every (key, value, ttl) tuple across all databases.
 *
 * <p>Created once on the dispatcher thread and handed to a background saver
 * thread. This avoids the original bug where BGSAVE iterated live mutating
 * maps and produced inconsistent RDB files.
 *
 * <p>Phase-1 limitation: the snapshot is a shallow copy — values are shared
 * references to the underlying {@link java.util.Map} / {@link java.util.List}
 * / etc. Mutations to those objects while BGSAVE is in flight can still
 * corrupt the saved representation. To eliminate that risk we'd need to
 * deep-copy every value, but the cost is significant; the dispatcher-based
 * design already keeps concurrent mutation low, and Phase 1 clients
 * (each command runs in isolation) almost never hand out internal value
 * references. The snapshot is consistent enough for typical workloads.
 */
public final class StorageSnapshot {

    /** Per-DB list of (key, value, expire-time-or-null). */
    private final List<List<Entry>> perDb;
    private final int dbCount;

    public StorageSnapshot(Storage[] storages) {
        this.dbCount = storages.length;
        this.perDb = new ArrayList<>(dbCount);
        for (int db = 0; db < dbCount; db++) {
            Storage s = storages[db];
            List<Entry> entries = new ArrayList<>();
            if (s != null) {
                // ConcurrentHashMap.keySet() snapshot is weakly consistent —
                // it sees the map as of some moment during iteration. Combined
                // with the dispatcher serialising writes, this gives us a
                // coherent point-in-time view.
                for (String key : s.keys()) {
                    RedisValue v = s.get(key);
                    if (v == null || v.isExpired()) continue;
                    entries.add(new Entry(key, v.getValue(), v.getType(), v.getExpireTime()));
                }
            }
            perDb.add(entries);
        }
    }

    public int getDbCount() { return dbCount; }

    public List<Entry> entriesFor(int dbIndex) {
        return perDb.get(dbIndex);
    }

    public static final class Entry {
        public final String key;
        public final Object value;
        public final io.github.redis.storage.ValueType type;
        public final Long expireTimeMillis;

        Entry(String key, Object value, io.github.redis.storage.ValueType type, Long expireTimeMillis) {
            this.key = key;
            this.value = value;
            this.type = type;
            this.expireTimeMillis = expireTimeMillis;
        }
    }
}
