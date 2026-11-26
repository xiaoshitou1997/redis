package io.github.redis.storage;

import io.github.redis.memory.MemoryTracker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * In-memory storage backed by {@link ConcurrentHashMap}.
 *
 * <p>Each {@link RedisValue} carries its own TTL, LRU timestamp, and LFU
 * counter. The optional {@link MemoryTracker} enforces maxmemory by sampling
 * and evicting victim keys on write.
 *
 * <p>Phase-1 note: with the single-threaded {@link io.github.redis.core.CommandDispatcher}
 * in place, the storage doesn't need its own locks — the dispatcher guarantees
 * only one thread mutates storage at a time.
 */
public class MemoryStorage implements Storage {

    private final Map<String, RedisValue> data = new ConcurrentHashMap<>();
    private volatile MemoryTracker memoryTracker;

    /** Wire a memory tracker; null disables enforcement (unlimited). */
    public void setMemoryTracker(MemoryTracker tracker) {
        this.memoryTracker = tracker;
    }

    @Override
    public RedisValue get(String key) {
        if (key == null) return null;
        RedisValue value = data.get(key);
        if (value == null) return null;
        if (value.isExpired()) {
            data.remove(key);
            return null;
        }
        value.touch(); // update LRU/LFU
        return value;
    }

    @Override
    public boolean set(String key, Object value, Long ttlMillis) {
        return set(key, value, ValueType.STRING, ttlMillis);
    }

    @Override
    public boolean set(String key, Object value, ValueType type, Long ttlMillis) {
        if (key == null) return false;
        Long expireTime = (ttlMillis != null && ttlMillis > 0)
                ? System.currentTimeMillis() + ttlMillis
                : null;

        // Estimate the new entry's memory footprint and enforce maxmemory.
        long incoming = estimateSize(key, value);
        if (memoryTracker != null && !memoryTracker.checkAndEvict(incoming)) {
            throw new OutOfMemoryError(
                    "OOM command not allowed when used memory > 'maxmemory'");
        }

        RedisValue prev = data.put(key, new RedisValue(value, type, expireTime));
        if (memoryTracker != null) {
            long freed = (prev == null) ? 0 : estimateSize(key, prev.getValue());
            memoryTracker.addAndGet(incoming - freed);
        }
        return true;
    }

    /**
     * Set a fully-built {@link RedisValue} (preserves encoding + LRU stats).
     * Used by handlers that need to override the encoding name reported by OBJECT ENCODING.
     */
    @Override
    public void set(String key, RedisValue value) {
        if (key == null) return;
        long incoming = estimateSize(key, value.getValue());
        if (memoryTracker != null && !memoryTracker.checkAndEvict(incoming)) {
            throw new OutOfMemoryError(
                    "OOM command not allowed when used memory > 'maxmemory'");
        }
        RedisValue prev = data.put(key, value);
        if (memoryTracker != null) {
            long freed = (prev == null) ? 0 : estimateSize(key, prev.getValue());
            memoryTracker.addAndGet(incoming - freed);
        }
    }

    @Override
    public boolean delete(String key) {
        if (key == null) return false;
        RedisValue prev = data.remove(key);
        if (prev == null) return false;
        if (memoryTracker != null) {
            memoryTracker.addAndGet(-estimateSize(key, prev.getValue()));
        }
        return true;
    }

    @Override
    public long delete(String... keys) {
        if (keys == null || keys.length == 0) return 0;
        long count = 0;
        for (String k : keys) if (delete(k)) count++;
        return count;
    }

    @Override
    public boolean exists(String key) {
        if (key == null) return false;
        RedisValue v = data.get(key);
        if (v == null) return false;
        if (v.isExpired()) {
            data.remove(key);
            return false;
        }
        return true;
    }

    @Override
    public boolean expire(String key, long ttlMillis) {
        if (key == null || ttlMillis <= 0) return false;
        RedisValue v = data.get(key);
        if (v == null || v.isExpired()) {
            data.remove(key);
            return false;
        }
        v.setExpireTime(System.currentTimeMillis() + ttlMillis);
        return true;
    }

    @Override
    public long ttl(String key) {
        if (key == null) return -2;
        RedisValue v = data.get(key);
        if (v == null) return -2;
        return v.getTtlMillis();
    }

    @Override
    public boolean persist(String key) {
        if (key == null) return false;
        RedisValue v = data.get(key);
        if (v == null) return false;
        if (v.isExpired()) { data.remove(key); return false; }
        v.setExpireTime(null);
        return true;
    }

    @Override
    public Set<String> keys(String pattern) {
        if (pattern == null || pattern.isEmpty()) return keys();
        Pattern compiled = Pattern.compile(patternToRegex(pattern));
        return data.entrySet().stream()
                .filter(e -> e.getValue() != null && !e.getValue().isExpired())
                .filter(e -> compiled.matcher(e.getKey()).matches())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> keys() {
        cleanupExpiredKeys();
        return new HashSet<>(data.keySet());
    }

    @Override
    public void clear() {
        if (memoryTracker != null) {
            long total = 0;
            for (Map.Entry<String, RedisValue> e : data.entrySet()) {
                total += estimateSize(e.getKey(), e.getValue().getValue());
            }
            memoryTracker.addAndGet(-total);
        }
        data.clear();
    }

    @Override
    public long size() {
        cleanupExpiredKeys();
        return data.size();
    }

    /** Approximate memory footprint of one entry in bytes. */
    private long estimateSize(String key, Object value) {
        long size = (key.length() * 2L) + 48;
        if (value == null) return size;
        if (value instanceof String) {
            return size + ((String) value).length() * 2L + 32;
        }
        if (value instanceof Map) {
            long n = 0;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
                n += sizeofStr(e.getKey()) + sizeofStr(e.getValue()) + 48;
            }
            return size + n;
        }
        if (value instanceof Iterable) {
            long n = 0;
            for (Object x : (Iterable<?>) value) n += sizeofStr(x) + 16;
            return size + n;
        }
        return size + 64;
    }

    private long sizeofStr(Object o) {
        return o == null ? 0 : (o.toString().length() * 2L + 32);
    }

    private String patternToRegex(String pattern) {
        StringBuilder regex = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            switch (c) {
                case '*':           regex.append(".*");          break;
                case '?':           regex.append(".");           break;
                case '.': case '+': case '(': case ')':
                case '[': case ']': case '{': case '}':
                case '^': case '$': case '|': case '\\':
                    regex.append("\\").append(c);               break;
                default:            regex.append(c);
            }
        }
        return regex.toString();
    }

    /** Best-effort sweep of expired entries. Called from size()/keys() and the cleanup scheduler. */
    public void cleanupExpiredKeys() {
        Iterator<Map.Entry<String, RedisValue>> it = data.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RedisValue> e = it.next();
            RedisValue v = e.getValue();
            if (v != null && v.isExpired()) {
                it.remove();
                if (memoryTracker != null) {
                    memoryTracker.addAndGet(-estimateSize(e.getKey(), v.getValue()));
                }
            }
        }
    }
}
