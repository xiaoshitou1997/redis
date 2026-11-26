package io.github.redis.observability;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Process-wide metrics collector. Single instance per server; updated by the
 * dispatcher thread and read by INFO / SLOWLOG / LATENCY.
 *
 * <p>Phase-2 scope: command counts, latency histogram, slow log, total connections,
 * rejected connections, keyspace hit/miss, ops/sec rolling window.
 */
public final class ServerMetrics {

    /** Default slow-log threshold in microseconds (10 ms = 10000 us). */
    public static final long DEFAULT_SLOWLOG_LOG_SLOWER_THAN = 10_000L;
    /** Max slow-log entries retained. */
    public static final int  DEFAULT_SLOWLOG_MAX_LEN = 128;

    private static final int LATENCY_BUCKETS = 32;
    private static final long[] LATENCY_US_BUCKETS = new long[] {
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
        1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000,
        256_000, 512_000, 1_000_000, 2_000_000, 4_000_000, 8_000_000,
        16_000_000, 32_000_000, 64_000_000, 128_000_000L, 256_000_000L,
        512_000_000L, 1_000_000_000L, Long.MAX_VALUE
    };

    // ===== server start time =====
    private final long startTimeMs = System.currentTimeMillis();

    // ===== counters =====
    private final AtomicLong totalConnectionsReceived = new AtomicLong(0);
    private final AtomicLong rejectedConnections      = new AtomicLong(0);
    private final AtomicLong totalCommandsProcessed   = new AtomicLong(0);
    private final AtomicLong keyspaceHits             = new AtomicLong(0);
    private final AtomicLong keyspaceMisses           = new AtomicLong(0);
    private final AtomicLong expiredKeys              = new AtomicLong(0);
    private final AtomicLong evictedKeys              = new AtomicLong(0);
    private final AtomicLong netInputBytes            = new AtomicLong(0);
    private final AtomicLong netOutputBytes           = new AtomicLong(0);

    // ===== per-command counters =====
    private final Map<String, AtomicLong> commandCalls = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> commandTotalMicros = new ConcurrentHashMap<>();
    private final AtomicLongArray latencyHistogram = new AtomicLongArray(LATENCY_BUCKETS);

    // ===== ops/sec rolling window (per-second buckets) =====
    private final AtomicLong opsThisSecond = new AtomicLong(0);
    private volatile long lastOpsSec = 0;
    private volatile long opsSecSecond = System.currentTimeMillis() / 1000;

    // ===== slow log =====
    private final Object slowLogLock = new Object();
    private final List<SlowLogEntry> slowLog = new ArrayList<>();
    private volatile long slowLogSlowerThan = DEFAULT_SLOWLOG_LOG_SLOWER_THAN;
    private volatile int  slowLogMaxLen = DEFAULT_SLOWLOG_MAX_LEN;
    private final AtomicLong slowLogSeq = new AtomicLong(0);

    // ===== public mutators (called from dispatcher) =====

    public void onCommandStart() { /* future: track concurrent */ }

    public void onCommandComplete(String name, long durationNanos, int argc, String[] argv) {
        totalCommandsProcessed.incrementAndGet();
        commandCalls.computeIfAbsent(name, k -> new AtomicLong()).incrementAndGet();
        long micros = durationNanos / 1000L;
        commandTotalMicros.computeIfAbsent(name, k -> new AtomicLong()).addAndGet(micros);
        recordLatency(micros);
        recordOpsSec();

        if (slowLogSlowerThan >= 0 && micros > slowLogSlowerThan) {
            pushSlowLog(name, argv, durationNanos);
        }
    }

    public void onNewConnection()           { totalConnectionsReceived.incrementAndGet(); }
    public void onRejectedConnection()      { rejectedConnections.incrementAndGet(); }
    public void onKeyspaceHit()             { keyspaceHits.incrementAndGet(); }
    public void onKeyspaceMiss()            { keyspaceMisses.incrementAndGet(); }
    public void onKeyExpired(int n)         { expiredKeys.addAndGet(n); }
    public void onKeyEvicted(int n)         { evictedKeys.addAndGet(n); }
    public void onNetInput(long bytes)      { netInputBytes.addAndGet(bytes); }
    public void onNetOutput(long bytes)     { netOutputBytes.addAndGet(bytes); }

    // ===== queries =====

    public long uptimeSeconds() { return (System.currentTimeMillis() - startTimeMs) / 1000; }

    public Map<String, Number> commandStats() {
        Map<String, Number> out = new LinkedHashMap<>();
        for (String name : commandCalls.keySet()) {
            long calls = commandCalls.get(name).get();
            long micros = commandTotalMicros.get(name).get();
            out.put(name + ".calls", calls);
            out.put(name + ".usec", micros);
            out.put(name + ".usec_per_call", calls == 0 ? 0.0 : (double) micros / calls);
        }
        return out;
    }

    public List<SlowLogEntry> slowLog(int max) {
        synchronized (slowLogLock) {
            int size = Math.min(max < 0 ? slowLog.size() : Math.min(max, slowLog.size()), slowLog.size());
            return new ArrayList<>(slowLog.subList(0, size));
        }
    }

    public void slowLogReset() {
        synchronized (slowLogLock) { slowLog.clear(); }
    }

    public int slowLogLen() {
        synchronized (slowLogLock) { return slowLog.size(); }
    }

    public void configureSlowLog(long slowerThan, int maxLen) {
        this.slowLogSlowerThan = slowerThan;
        this.slowLogMaxLen = Math.max(1, maxLen);
    }

    public long getSlowLogSlowerThan() { return slowLogSlowerThan; }
    public int  getSlowLogMaxLen()    { return slowLogMaxLen; }

    // ===== push tracking (Phase 8) =====

    private final java.util.concurrent.atomic.AtomicLong invalidations = new java.util.concurrent.atomic.AtomicLong(0);

    public void onKeyInvalidated(int n) { invalidations.addAndGet(n); }
    public long getInvalidations() { return invalidations.get(); }

    public Map<String, String> trackingSection() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("tracking_total_keys", "0");
        m.put("tracking_total_prefixes", "0");
        m.put("tracking_invalidations", String.valueOf(invalidations.get()));
        return m;
    }

    // ===== INFO builders =====

    /** Server section. */
    public Map<String, String> serverSection() {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("redis_version", "8.0.0-phase2");
        s.put("redis_mode", "standalone");
        s.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        s.put("arch_bits", "64");
        String pid;
        try {
            String nm = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            pid = nm.contains("@") ? nm.substring(0, nm.indexOf('@')) : nm;
        } catch (Throwable t) { pid = "0"; }
        s.put("process_id", pid);
        s.put("run_id", Long.toHexString(startTimeMs));
        s.put("uptime_in_seconds", Long.toString(uptimeSeconds()));
        s.put("uptime_in_days", Long.toString(uptimeSeconds() / 86400));
        s.put("tcp_port", "6379");
        return s;
    }

    /** Clients section. */
    public Map<String, String> clientsSection(long connectedClients) {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("connected_clients", Long.toString(connectedClients));
        s.put("blocked_clients", "0"); // updated in P2.5
        s.put("maxclients", "10000");
        return s;
    }

    /** Memory section. */
    public Map<String, String> memorySection(long usedBytes, long maxBytes) {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("used_memory", Long.toString(usedBytes));
        s.put("used_memory_human", humanSize(usedBytes));
        s.put("used_memory_rss", Long.toString(usedBytes));
        s.put("maxmemory", Long.toString(maxBytes));
        s.put("maxmemory_human", humanSize(maxBytes));
        s.put("maxmemory_policy", "noeviction");
        s.put("mem_fragmentation_ratio", "1.00");
        return s;
    }

    /** Stats section. */
    public Map<String, String> statsSection(long dbSize) {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("total_connections_received", Long.toString(totalConnectionsReceived.get()));
        s.put("rejected_connections", Long.toString(rejectedConnections.get()));
        s.put("total_commands_processed", Long.toString(totalCommandsProcessed.get()));
        s.put("instantaneous_ops_per_sec", Long.toString(getOpsSec()));
        s.put("keyspace_hits", Long.toString(keyspaceHits.get()));
        s.put("keyspace_misses", Long.toString(keyspaceMisses.get()));
        s.put("expired_keys", Long.toString(expiredKeys.get()));
        s.put("evicted_keys", Long.toString(evictedKeys.get()));
        s.put("net_input_bytes", Long.toString(netInputBytes.get()));
        s.put("net_output_bytes", Long.toString(netOutputBytes.get()));
        s.put("keys", Long.toString(dbSize));
        return s;
    }

    /** Replication section (Phase-2 single-node placeholder). */
    public Map<String, String> replicationSection() {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("role", "master");
        s.put("connected_slaves", "0");
        s.put("master_failover_state", "no-failover");
        return s;
    }

    /** Keyspace section. */
    public Map<String, String> keyspaceSection(long[] perDbCounts) {
        Map<String, String> s = new LinkedHashMap<>();
        for (int i = 0; i < perDbCounts.length; i++) {
            if (perDbCounts[i] > 0) {
                s.put("db" + i, "keys=" + perDbCounts[i] + ",expires=0,avg_ttl=0");
            }
        }
        return s;
    }

    // ===== private =====

    private void recordLatency(long micros) {
        for (int i = 0; i < LATENCY_BUCKETS; i++) {
            if (micros <= LATENCY_US_BUCKETS[i]) {
                latencyHistogram.incrementAndGet(i);
                return;
            }
        }
        latencyHistogram.incrementAndGet(LATENCY_BUCKETS - 1);
    }

    private void recordOpsSec() {
        long sec = System.currentTimeMillis() / 1000;
        if (sec != opsSecSecond) {
            synchronized (this) {
                if (sec != opsSecSecond) {
                    lastOpsSec = opsThisSecond.getAndSet(1);
                    opsSecSecond = sec;
                    return;
                }
            }
        }
        opsThisSecond.incrementAndGet();
    }

    public long getOpsSec() {
        long now = System.currentTimeMillis() / 1000;
        return now == opsSecSecond ? Math.max(lastOpsSec, opsThisSecond.get()) : lastOpsSec;
    }

    private void pushSlowLog(String name, String[] argv, long durationNanos) {
        SlowLogEntry entry = new SlowLogEntry(
                slowLogSeq.incrementAndGet(),
                System.currentTimeMillis() / 1000.0,
                durationNanos / 1000L, // microseconds
                argv,
                "127.0.0.1:0" // client address omitted (Phase 2 simplification)
        );
        synchronized (slowLogLock) {
            slowLog.add(0, entry);
            while (slowLog.size() > slowLogMaxLen) slowLog.remove(slowLog.size() - 1);
        }
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.2fK", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.2fM", bytes / (1024.0 * 1024));
        return String.format("%.2fG", bytes / (1024.0 * 1024 * 1024));
    }

    /** One slow-log entry, matching Redis's SLOWLOG GET reply shape. */
    public static final class SlowLogEntry {
        public final long id;
        public final double timestampSec;
        public final long durationMicros;
        public final String[] argv;
        public final String client;

        SlowLogEntry(long id, double ts, long dur, String[] argv, String client) {
            this.id = id;
            this.timestampSec = ts;
            this.durationMicros = dur;
            this.argv = argv;
            this.client = client;
        }
    }
}
