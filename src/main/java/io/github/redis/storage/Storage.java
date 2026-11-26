package io.github.redis.storage;

import java.util.Set;

/**
 * Storage interface
 * Abstract storage layer supporting single and cluster implementations
 */
public interface Storage {

    /**
     * Set a fully-built {@link RedisValue} (preserves encoding + LRU stats).
     * Useful for handlers that need to override the encoding name reported by OBJECT ENCODING.
     */
    default void set(String key, RedisValue value) {
        set(key, value.getValue(), value.getType(), value.getExpireTime());
    }

    /**
     * Get value
     *
     * @param key the key
     * @return Redis value, or null if not found or expired
     */
    RedisValue get(String key);

    /**
     * Set value
     *
     * @param key        the key
     * @param value      the value
     * @param ttlMillis expiration time in milliseconds, null means never expire
     * @return true if set successfully
     */
    boolean set(String key, Object value, Long ttlMillis);

    /**
     * Set value (with type)
     *
     * @param key        the key
     * @param value      the value
     * @param type      the value type
     * @param ttlMillis expiration time in milliseconds, null means never expire
     * @return true if set successfully
     */
    boolean set(String key, Object value, ValueType type, Long ttlMillis);

    /**
     * Delete key
     *
     * @param key the key
     * @return true if key exists and deleted successfully
     */
    boolean delete(String key);

    /**
     * Delete multiple keys
     *
     * @param keys the list of keys
     * @return number of keys deleted
     */
    long delete(String... keys);

    /**
     * Check if key exists
     *
     * @param key the key
     * @return true if key exists and not expired
     */
    boolean exists(String key);

    /**
     * Set expiration time
     *
     * @param key        the key
     * @param ttlMillis expiration time in milliseconds
     * @return true if set successfully
     */
    boolean expire(String key, long ttlMillis);

    /**
     * Get remaining TTL in milliseconds
     *
     * @param key the key
     * @return -1 for never expire, -2 for key not found or expired, other values for remaining milliseconds
     */
    long ttl(String key);

    /**
     * Remove expiration time, making key never expire
     *
     * @param key the key
     * @return true if removed successfully
     */
    boolean persist(String key);

    /**
     * Get all matching keys
     *
     * @param pattern matching pattern (supports * and ? wildcards)
     * @return set of matching keys
     */
    Set<String> keys(String pattern);

    /**
     * Get all keys
     *
     * @return set of all keys
     */
    Set<String> keys();

    /**
     * Clear all data
     */
    void clear();

    /**
     * Get the number of keys stored
     *
     * @return number of keys
     */
    long size();
}