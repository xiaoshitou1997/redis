package io.github.redis.storage;

import io.github.redis.encoding.Encoding;

import java.util.Objects;

/**
 * Redis value wrapper class
 * Stores the actual value and expiration time
 */
public class RedisValue {

    private final Object value;
    private final ValueType type;
    private volatile Encoding encoding;
    private Long expireTime; // Expiration timestamp in milliseconds, null means never expire
    private volatile long lastAccess;   // updated on every read (LRU)
    private volatile long accessCount;  // incremented on every read (LFU)

    public RedisValue(Object value, ValueType type) {
        this.value = value;
        this.type = type;
        this.encoding = Encoding.guess(value, type);
        this.expireTime = null;
        this.lastAccess = System.nanoTime();
        this.accessCount = 0;
    }

    public RedisValue(Object value, ValueType type, Long expireTime) {
        this.value = value;
        this.type = type;
        this.encoding = Encoding.guess(value, type);
        this.expireTime = expireTime;
        this.lastAccess = System.nanoTime();
        this.accessCount = 0;
    }

    /**
     * Get the value
     */
    public Object getValue() {
        return value;
    }

    /**
     * Get the value and convert to specified type. Touches LRU/LFU stats.
     */
    public <T> T getValue(Class<T> clazz) {
        touch();
        if (value == null) {
            return null;
        }
        return clazz.cast(value);
    }

    /** Mark this value as recently accessed. Called by storage.get() and similar paths. */
    public void touch() {
        this.lastAccess = System.nanoTime();
        // Cap LFU counter at 2^63 - 1 to avoid overflow. Real Redis uses a
        // saturating 8-bit counter with probabilistic decay; this is a coarse
        // approximation.
        if (this.accessCount < Long.MAX_VALUE) {
            this.accessCount++;
        }
    }

    public long getLastAccess() { return lastAccess; }
    public long getAccessCount() { return accessCount; }

    /**
     * Get the value type
     */
    public ValueType getType() {
        return type;
    }

    /**
     * Get the internal encoding name (used by OBJECT ENCODING).
     */
    public Encoding getEncoding() { return encoding; }

    /** Allow handlers to override the encoding after a structural change (e.g. promote listpack → hashtable). */
    public void setEncoding(Encoding encoding) { this.encoding = encoding; }

    /**
     * Get the expiration timestamp in milliseconds
     */
    public Long getExpireTime() {
        return expireTime;
    }

    /**
     * Set the expiration timestamp in milliseconds
     */
    public void setExpireTime(Long expireTime) {
        this.expireTime = expireTime;
    }

    /**
     * Check if the value has expired
     */
    public boolean isExpired() {
        if (expireTime == null) {
            return false;
        }
        return System.currentTimeMillis() > expireTime;
    }

    /**
     * Get the remaining TTL in milliseconds
     * Returns -1 for no expiration, -2 for expired or not existing
     */
    public long getTtlMillis() {
        if (expireTime == null) {
            return -1; // Never expire
        }
        long now = System.currentTimeMillis();
        if (now > expireTime) {
            return -2; // Already expired
        }
        return expireTime - now;
    }

    /**
     * Get the remaining TTL in seconds
     */
    public long getTtlSeconds() {
        long ttlMillis = getTtlMillis();
        if (ttlMillis < 0) {
            return ttlMillis;
        }
        return ttlMillis / 1000;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisValue that = (RedisValue) o;
        return Objects.equals(value, that.value) &&
                type == that.type &&
                Objects.equals(expireTime, that.expireTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type, expireTime);
    }

    @Override
    public String toString() {
        return "RedisValue{" +
                "value=" + value +
                ", type=" + type +
                ", encoding=" + encoding +
                ", expireTime=" + expireTime +
                ", expired=" + isExpired() +
                '}';
    }
}
