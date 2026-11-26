package io.github.redis.memory;

import io.github.redis.storage.RedisValue;
import io.github.redis.storage.Storage;
import io.github.redis.storage.StorageManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks and enforces {@code maxmemory} across all databases.
 *
 * <p>Strategy:
 * <ul>
 *   <li>Each successful write reports its delta to {@link #addAndGet(long)}. This
 *       is approximate — we don't count every map entry's overhead, but key and
 *       value payloads are accounted for.</li>
 *   <li>Before a write commits, the writer calls {@link #checkAndEvict(long)} to
 *       ensure the projected usage stays under the limit. If it would exceed,
 *       the configured {@link EvictionPolicy} kicks in (or OOM is returned).</li>
 *   <li>The dispatcher's single-threaded execution model means we don't need
 *       locking around these counters — only the dispatcher thread mutates.</li>
 * </ul>
 *
 * <p>Approximate LRU/LFU: on each eviction we sample up to
 * {@link #SAMPLE_SIZE} keys and pick the worst one. This matches Redis's
 * approximation trade-off (cheap, not strictly optimal).
 */
public final class MemoryTracker {

    /** Redis default sample size for approximate LRU/LFU/TTL. */
    public static final int SAMPLE_SIZE = 5;

    private final StorageManager storageManager;
    private final long maxBytes;
    private final EvictionPolicy policy;
    private final AtomicLong usedBytes = new AtomicLong(0);

    public MemoryTracker(StorageManager storageManager, long maxBytes, EvictionPolicy policy) {
        this.storageManager = storageManager;
        this.maxBytes = maxBytes;
        this.policy = policy == null ? EvictionPolicy.NO_EVICTION : policy;
    }

    public long getMaxBytes() { return maxBytes; }
    public long getUsedBytes() { return usedBytes.get(); }
    public EvictionPolicy getPolicy() { return policy; }

    public boolean isUnlimited() { return maxBytes <= 0; }

    /** Adjust the running counter; called by storage on every write. */
    public long addAndGet(long delta) {
        return usedBytes.addAndGet(delta);
    }

    /**
     * Return true if the caller may proceed with a write that adds {@code incomingBytes}
     * to memory. If false, the {@link EvictionPolicy} is applied until under the limit
     * (or, under NO_EVICTION, no eviction is attempted and false is returned to signal
     * the caller to abort with an OOM error).
     */
    public boolean checkAndEvict(long incomingBytes) {
        if (isUnlimited()) return true;
        if (usedBytes.get() + incomingBytes <= maxBytes) return true;
        if (policy == EvictionPolicy.NO_EVICTION) return false;

        // Evict until we have room for the incoming write (or until nothing's left).
        int safety = 1_000_000; // absolute cap to avoid infinite loop on pathological data
        while (usedBytes.get() + incomingBytes > maxBytes && safety-- > 0) {
            String victim = pickVictim();
            if (victim == null) return false; // nothing left to evict
            long freed = storageManager.getDefaultStorage().delete(victim) ? estimateForKey(victim, null) : 0;
            // delete() returns true but we don't know the size of the deleted value;
            // estimate conservatively as the key length plus 64 bytes overhead.
            usedBytes.addAndGet(-freed);
        }
        return usedBytes.get() + incomingBytes <= maxBytes;
    }

    private long estimateForKey(String key, RedisValue value) {
        long size = (key.length() * 2L) + 48;
        if (value != null) size += estimateValue(value);
        return size;
    }

    private long estimateValue(RedisValue v) {
        Object o = v == null ? null : v.getValue();
        if (o == null) return 0;
        if (o instanceof String) return ((String) o).length() * 2L + 32;
        if (o instanceof Map) {
            long n = 0;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                n += sizeofObj(e.getKey()) + sizeofObj(e.getValue()) + 48;
            }
            return n;
        }
        if (o instanceof Iterable) {
            long n = 0;
            for (Object x : (Iterable<?>) o) n += sizeofObj(x) + 16;
            return n;
        }
        return 64;
    }

    private long sizeofObj(Object o) {
        return o == null ? 0 : (o.toString().length() * 2L + 32);
    }

    /**
     * Sample a handful of keys and pick a victim according to the policy.
     * Returns null if no candidate is eligible.
     */
    private String pickVictim() {
        // Pull a slightly larger candidate pool than SAMPLE_SIZE so that volatile
        // policies can still find a candidate after filtering by TTL.
        List<String> allKeys = collectKeys();
        if (allKeys.isEmpty()) return null;

        Random rng = ThreadLocalRandom.current();
        int poolSize = Math.min(allKeys.size(), SAMPLE_SIZE * 4);
        List<String> pool = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            pool.add(allKeys.get(rng.nextInt(allKeys.size())));
        }

        // Filter to TTL-bearing keys for volatile-* policies.
        if (policy.isVolatile()) {
            pool.removeIf(k -> {
                RedisValue v = storageManager.getDefaultStorage().get(k);
                return v == null || v.getExpireTime() == null;
            });
            if (pool.isEmpty()) return null;
        }

        // Reduce to SAMPLE_SIZE.
        while (pool.size() > SAMPLE_SIZE) pool.remove(pool.size() - 1);

        switch (policy) {
            case ALLKEYS_RANDOM:
            case VOLATILE_RANDOM:
                return pool.get(rng.nextInt(pool.size()));
            case VOLATILE_TTL: {
                String victim = null;
                long best = Long.MAX_VALUE;
                for (String k : pool) {
                    RedisValue v = storageManager.getDefaultStorage().get(k);
                    if (v == null) continue;
                    long t = v.getExpireTime() == null ? Long.MAX_VALUE : v.getExpireTime();
                    if (t < best) { best = t; victim = k; }
                }
                return victim;
            }
            case ALLKEYS_LRU:
            case VOLATILE_LRU: {
                String victim = null;
                long best = Long.MAX_VALUE;
                for (String k : pool) {
                    RedisValue v = storageManager.getDefaultStorage().get(k);
                    if (v == null) continue;
                    long t = v.getLastAccess();
                    if (t < best) { best = t; victim = k; }
                }
                return victim;
            }
            case ALLKEYS_LFU:
            case VOLATILE_LFU: {
                String victim = null;
                long best = Long.MAX_VALUE;
                for (String k : pool) {
                    RedisValue v = storageManager.getDefaultStorage().get(k);
                    if (v == null) continue;
                    long f = v.getAccessCount();
                    if (f < best) { best = f; victim = k; }
                }
                return victim;
            }
            default:
                return null;
        }
    }

    private List<String> collectKeys() {
        // All databases could contribute, but Phase 1 uses the default DB only
        // (which is where every test we have runs). Multi-db eviction comes later.
        Storage s = storageManager.getDefaultStorage();
        return new ArrayList<>(s.keys());
    }
}
