package io.github.redis.storage;

/**
 * Redis value type enum
 */
public enum ValueType {
    /**
     * String type
     */
    STRING,

    /**
     * Hash type
     */
    HASH,

    /**
     * List type
     */
    LIST,

    /**
     * Set type
     */
    SET,

    /**
     * Sorted set type
     */
    SORTED_SET,

    /**
     * Stream type (Redis 5.0+)
     */
    STREAM
}








