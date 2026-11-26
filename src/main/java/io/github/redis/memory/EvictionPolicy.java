package io.github.redis.memory;

/**
 * Memory eviction policies. Mirrors Redis's maxmemory-policy directives.
 *
 * <ul>
 *   <li>{@link #NO_EVICTION} — return OOM error when over limit (default).</li>
 *   <li>{@code ALLKEYS_*} — sample from all keys.</li>
 *   <li>{@code VOLATILE_*} — sample only from keys with an expiry.</li>
 *   <li>LRU — pick least-recently accessed from the sample.</li>
 *   <li>LFU  — pick least-frequently used from the sample.</li>
 *   <li>RANDOM — pick a random key from the sample.</li>
 * </ul>
 */
public enum EvictionPolicy {
    NO_EVICTION,
    ALLKEYS_LRU,
    ALLKEYS_LFU,
    ALLKEYS_RANDOM,
    VOLATILE_LRU,
    VOLATILE_LFU,
    VOLATILE_RANDOM,
    VOLATILE_TTL;

    /** Parse the redis.conf string (e.g. "allkeys-lru"). Case-insensitive. */
    public static EvictionPolicy parse(String s) {
        if (s == null) return NO_EVICTION;
        switch (s.toLowerCase().replace("-", "_").replace(" ", "")) {
            case "noeviction":       return NO_EVICTION;
            case "allkeys_lru":      return ALLKEYS_LRU;
            case "allkeys_lfu":      return ALLKEYS_LFU;
            case "allkeys_random":   return ALLKEYS_RANDOM;
            case "volatile_lru":     return VOLATILE_LRU;
            case "volatile_lfu":     return VOLATILE_LFU;
            case "volatile_random":  return VOLATILE_RANDOM;
            case "volatile_ttl":     return VOLATILE_TTL;
            default:                 return NO_EVICTION;
        }
    }

    public boolean isAllKeys() {
        return this == ALLKEYS_LRU || this == ALLKEYS_LFU || this == ALLKEYS_RANDOM;
    }

    public boolean isVolatile() {
        return this == VOLATILE_LRU || this == VOLATILE_LFU
                || this == VOLATILE_RANDOM || this == VOLATILE_TTL;
    }

    public boolean isLru()  { return this == ALLKEYS_LRU || this == VOLATILE_LRU; }
    public boolean isLfu()  { return this == ALLKEYS_LFU || this == VOLATILE_LFU; }
    public boolean isRandom() { return this == ALLKEYS_RANDOM || this == VOLATILE_RANDOM; }
}
