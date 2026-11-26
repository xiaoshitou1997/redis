package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.memory.MemoryTracker;
import io.github.redis.observability.ServerMetrics;
import io.github.redis.storage.StorageManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * INFO / SLOWLOG / LATENCY / CLIENT LIST backed by real {@link ServerMetrics}.
 *
 * <p>Each reply is a single bulk string with CR-LF-delimited key:value pairs
 * grouped under section headers (matching Redis's wire format).
 */
public final class ObservabilityCommands {

    private ObservabilityCommands() {}

    // ===== INFO [section] =====

    public static void info(RedisClient c) {
        ServerMetrics m = c.services().get(ServerMetrics.class);
        if (m == null) { c.replyError("ERR metrics subsystem unavailable"); return; }

        String section = c.argc() >= 2 ? c.arg(1).toLowerCase() : "default";
        MemoryTracker mt = c.services().get(MemoryTracker.class);
        StorageManager sm = c.services().require(StorageManager.class);

        long used = mt == null ? 0 : mt.getUsedBytes();
        long max  = mt == null ? 0 : mt.getMaxBytes();
        int dbCount = sm.getDatabaseCount();
        long[] perDbCounts = new long[dbCount];
        long totalKeys = 0;
        for (int i = 0; i < dbCount; i++) {
            perDbCounts[i] = sm.getStorage(i).size();
            if (i == c.dbIndex()) totalKeys += perDbCounts[i];
        }

        StringBuilder sb = new StringBuilder();

        if (want(section, "server", "default", "all")) {
            appendSection(sb, "server", m.serverSection());
        }
        if (want(section, "clients", "default", "all")) {
            appendSection(sb, "clients", m.clientsSection(1)); // Phase-2: client count is approximate
        }
        if (want(section, "memory", "default", "all")) {
            appendSection(sb, "memory", m.memorySection(used, max));
        }
        if (want(section, "persistence", "default", "all")) {
            Map<String, String> p = new java.util.LinkedHashMap<>();
            p.put("loading", "0");
            p.put("rdb_changes_since_last_save", "0");
            p.put("rdb_bgsave_in_progress", "0");
            p.put("aof_enabled", "0");
            p.put("aof_rewrite_in_progress", "0");
            appendSection(sb, "persistence", p);
        }
        if (want(section, "stats", "default", "all")) {
            appendSection(sb, "stats", m.statsSection(totalKeys));
        }
        if (want(section, "replication", "default", "all")) {
            io.github.redis.replication.ReplicationManager rm =
                    c.services().get(io.github.redis.replication.ReplicationManager.class);
            java.util.Map<String, String> rep = new java.util.LinkedHashMap<>();
            String body = io.github.redis.command.handler.ReplicationCommands.buildReplicationSection(rm);
            for (String line : body.split("\r\n")) {
                int idx = line.indexOf(':');
                if (idx > 0) rep.put(line.substring(0, idx), line.substring(idx + 1));
            }
            appendSection(sb, "replication", rep);
        }
        if (want(section, "cpu", "all")) {
            Map<String, String> cpu = new java.util.LinkedHashMap<>();
            cpu.put("used_cpu_sys", String.format("%.2f", 0.0));
            cpu.put("used_cpu_user", String.format("%.2f", ManagementThreadCpuUser()));
            appendSection(sb, "cpu", cpu);
        }
        if (want(section, "commandstats", "all")) {
            Map<String, Number> cs = m.commandStats();
            Map<String, String> csStrings = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, Number> e : cs.entrySet()) {
                csStrings.put(e.getKey(), formatNum(e.getValue()));
            }
            appendSection(sb, "commandstats", csStrings);
        }
        if (want(section, "keyspace", "default", "all")) {
            appendSection(sb, "keyspace", m.keyspaceSection(perDbCounts));
        }

        c.replyBulk(sb.toString());
    }

    private static double ManagementThreadCpuUser() {
        com.sun.management.ThreadMXBean mx = (com.sun.management.ThreadMXBean)
                java.lang.management.ManagementFactory.getThreadMXBean();
        long cpuNs = mx.getThreadCpuTime(java.lang.Thread.currentThread().getId());
        return cpuNs / 1_000_000_000.0;
    }

    private static String formatNum(Number n) {
        if (n instanceof Double || n instanceof Float) {
            return String.format("%.2f", n.doubleValue());
        }
        return n.toString();
    }

    private static boolean want(String asked, String... sections) {
        if (asked.equals("everything") || asked.equals("all")) return true;
        for (String s : sections) if (s.equals(asked)) return true;
        return false;
    }

    private static void appendSection(StringBuilder sb, String name, Map<String, String> kv) {
        // Redis uses Title Case headers: "# Server", "# Clients", "# Memory", ...
        String titled = name.substring(0, 1).toUpperCase() + name.substring(1);
        sb.append("# ").append(titled).append("\r\n");
        for (Map.Entry<String, String> e : kv.entrySet()) {
            sb.append(e.getKey()).append(':').append(e.getValue()).append("\r\n");
        }
        sb.append("\r\n");
    }

    // ===== SLOWLOG GET [count] / RESET / LEN / SET =====

    public static void slowlog(RedisClient c) {
        ServerMetrics m = c.services().get(ServerMetrics.class);
        if (m == null) { c.replyError("ERR metrics subsystem unavailable"); return; }
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "GET": {
                int max = 10;
                if (c.argc() >= 3) {
                    Integer n = Args.parseInt(c, 2);
                    if (n == null) return;
                    max = n;
                }
                List<ServerMetrics.SlowLogEntry> entries = m.slowLog(max);
                List<Object> out = new ArrayList<>(entries.size());
                for (ServerMetrics.SlowLogEntry e : entries) {
                    List<Object> row = new ArrayList<>(6);
                    row.add(e.id);
                    row.add((long) e.timestampSec);
                    row.add(e.durationMicros);
                    List<Object> argv = new ArrayList<>(e.argv.length);
                    for (String a : e.argv) argv.add(a);
                    row.add(argv);
                    row.add(e.client);
                    row.add(""); // client name omitted
                    out.add(row);
                }
                c.replyArray(out);
                return;
            }
            case "LEN":
                c.replyInteger(m.slowLogLen());
                return;
            case "RESET":
                m.slowLogReset();
                c.replyOK();
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
                        "GET [<count>] -- Return top <count> entries.",
                        "LEN -- Return length.",
                        "RESET -- Reset.",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown SLOWLOG subcommand: " + sub);
        }
    }

    // ===== LATENCY (stubbed — full feature lands in Phase 3) =====

    public static void latency(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "RESET": c.replyOK(); return;
            case "GRAPH":
            case "HISTORY":
            case "DOCTOR":
                c.replyBulk(""); return;
            case "LATEST":
                c.replyEmptyArray(); return;
            default:
                c.replyError("ERR Unknown LATENCY subcommand: " + sub);
        }
    }

    // ===== MEMORY (rewritten from stubs) =====

    public static void memory(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        MemoryTracker mt = c.services().get(MemoryTracker.class);
        switch (sub) {
            case "USAGE": {
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                io.github.redis.storage.RedisValue v = c.storage().get(c.arg(2));
                if (v == null) { c.replyNil(); return; }
                c.replyInteger(io.github.redis.command.handler.Values.sizeof(v));
                return;
            }
            case "STATS": {
                List<Object> out = new ArrayList<>();
                out.add("peak.allocated");
                out.add(mt == null ? 0L : mt.getUsedBytes());
                out.add("total.allocated");
                out.add(mt == null ? 0L : mt.getUsedBytes());
                out.add("startup.count");
                out.add(0L);
                c.replyArray(out);
                return;
            }
            case "DOCTOR":
                c.replyBulk("Sam, I detected a few issues in this Redis instance memory implants:\n"
                        + "  Phase-2 memory doctor: detail deferred to Phase 3.\n");
                return;
            case "PURGE":
            case "MALLOC-STATS":
                c.replyOK();
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "MEMORY <subcommand> [<arg> ...]. Subcommands are:",
                        "DOCTOR -- Return memory problems report.",
                        "MALLOC-STATS -- Return internal statistics report from the memory allocator.",
                        "PURGE -- Attempt to purge memory.",
                        "STATS -- Return internal memory statistics.",
                        "USAGE <key> [SAMPLES <count>] -- Estimate memory of a key.",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown MEMORY subcommand: " + sub);
        }
    }
}
