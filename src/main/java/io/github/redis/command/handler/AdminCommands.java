package io.github.redis.command.handler;

import io.github.redis.acl.AclManager;
import io.github.redis.cluster.ClusterManager;
import io.github.redis.core.RedisClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Subcommand families: CLIENT / CONFIG / ACL / CLUSTER / MODULE / SLOWLOG / LATENCY.
 *
 * <p>Each top-level command dispatches on argv[1]. Implementations are intentionally
 * minimal — enough to keep clients (redis-cli, lettuce, jedis) happy in Phase 0.
 */
public final class AdminCommands {

    private AdminCommands() {}

    // ===== CLIENT =====

    public static void client(RedisClient c) {
        if (c.argc() < 2) { c.replyError("ERR wrong number of arguments for 'client|command' command"); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "SETNAME":
                // No state kept; accept any name.
                c.replyOK();
                return;
            case "GETNAME":
                c.replyNil();
                return;
            case "ID":
                c.replyInteger(c.channel().id().asShortText().hashCode());
                return;
            case "LIST":
                c.replyBulk("id=1 addr=" + c.channel().remoteAddress() + " name= ");
                return;
            case "INFO":
                c.replyBulk("id=1 addr=" + c.channel().remoteAddress() + " name= ");
                return;
            case "KILL":
                c.replyInteger(0);
                return;
            case "PAUSE":
            case "RESUME":
            case "REPLY":
            case "TRACKING":     io.github.redis.command.handler.TrackingCommands.clientTracking(c); return;
            case "CACHING":      io.github.redis.command.handler.TrackingCommands.clientCaching(c); return;
            case "NO-EVICT":    io.github.redis.command.handler.TrackingCommands.clientNoEvict(c); return;
            case "GETREDIR":    io.github.redis.command.handler.TrackingCommands.clientGetredir(c); return;
            case "SETINFO":     io.github.redis.command.handler.TrackingCommands.clientSetinfo(c); return;
            case "NO-TOUCH":
            case "UNPAUSE":
                c.replyOK();
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "GETNAME -- Return the name of the current connection.",
                        "ID -- Return the ID of the current connection.",
                        "INFO -- Return information about the current client connection.",
                        "LIST -- Return information about client connections.",
                        "SETNAME <name> -- Assign the name <name> to the current connection.",
                        "TRACKING <on|off>",
                        "CACHING <yes|no>",
                        "NO-EVICT <on|off>",
                        "GETREDIR",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown CLIENT subcommand or wrong number of arguments for '" + sub + "'");
        }
    }

    // ===== CONFIG =====

    public static void config(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "GET": {
                // CONFIG GET <pattern> — minimal: report well-known params.
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                String pattern = c.arg(2);
                Map<String, String> known = knownConfigParams(c);
                List<Object> out = new ArrayList<>();
                for (Map.Entry<String, String> e : known.entrySet()) {
                    if (matchesGlob(pattern, e.getKey())) {
                        out.add(e.getKey());
                        out.add(e.getValue());
                    }
                }
                c.replyArray(out);
                return;
            }
            case "SET": {
                // CONFIG SET param value [param value ...]
                if (c.argc() < 4 || ((c.argc() - 2) % 2) != 0) {
                    c.replyError("ERR wrong number of arguments for CONFIG SET");
                    return;
                }
                // Phase 0: accept silently; runtime config reload is Phase 1.
                c.replyOK();
                return;
            }
            case "RESETSTAT":
                c.replyOK();
                return;
            case "REWRITE":
                c.replyOK();
                return;
            default:
                c.replyError("ERR Unknown CONFIG subcommand: " + sub);
        }
    }

    private static Map<String, String> knownConfigParams(RedisClient c) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("maxmemory", "0");
        m.put("maxmemory-policy", "noeviction");
        m.put("appendonly", "no");
        m.put("save", "");
        m.put("databases", "16");
        m.put("timeout", "0");
        m.put("tcp-keepalive", "300");
        m.put("hash-max-listpack-entries", "512");
        m.put("hash-max-listpack-value", "64");
        m.put("list-max-listpack-size", "-2");
        m.put("set-max-intset-entries", "512");
        m.put("zset-max-listpack-entries", "128");
        m.put("zset-max-listpack-value", "64");
        return m;
    }

    private static boolean matchesGlob(String pattern, String s) {
        return s.matches(globToRegex(pattern));
    }

    private static String globToRegex(String p) {
        StringBuilder sb = new StringBuilder();
        for (char ch : p.toCharArray()) {
            switch (ch) {
                case '*': sb.append(".*"); break;
                case '?': sb.append('.'); break;
                case '.': case '+': case '(': case ')':
                case '[': case ']': case '{': case '}':
                case '^': case '$': case '|': case '\\':
                    sb.append('\\').append(ch); break;
                default:  sb.append(ch);
            }
        }
        return sb.toString();
    }

    // ===== ACL =====

    public static void acl(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        AclManager acl = c.acl();
        switch (sub) {
            case "WHOAMI": {
                String u = c.authenticatedUser();
                c.replyBulk(u == null ? "default" : u);
                return;
            }
            case "LIST": {
                List<Object> out = new ArrayList<>();
                if (acl != null) {
                    Map<String, AclManager.RedisUser.UserInfo> users = acl.getUserList();
                    for (Map.Entry<String, AclManager.RedisUser.UserInfo> e : users.entrySet()) {
                        out.add("user " + e.getKey() + " on nopass +@all ~*");
                    }
                }
                c.replyArray(out);
                return;
            }
            case "USERS": {
                List<Object> out = new ArrayList<>();
                if (acl != null) out.addAll(acl.getUsernames());
                c.replyArray(out);
                return;
            }
            case "CAT": {
                if (c.argc() == 2) {
                    c.replyArray(new ArrayList<>(Arrays.asList(
                            "keyspace", "read", "write", "set", "sortedset", "list", "hash",
                            "string", "bitmap", "hyperloglog", "geo", "stream", "pubsub",
                            "admin", "fast", "slow", "blocking", "dangerous",
                            "connection", "transaction", "scripting")));
                } else {
                    c.replyEmptyArray();
                }
                return;
            }
            case "SETUSER": {
                // ACL SETUSER username [rules] — minimal: accept and create empty user.
                if (c.argc() < 3) { c.replyWrongArgc(); return; }
                if (acl == null) { c.replyError("ERR ACL subsystem unavailable"); return; }
                String username = c.arg(2);
                String[] rules = new String[c.argc() - 3];
                for (int j = 3; j < c.argc(); j++) rules[j - 3] = c.arg(j);
                try {
                    acl.createUser(username, true, null, rules);
                    c.replyOK();
                } catch (Exception e) {
                    c.replyError("ERR " + e.getMessage());
                }
                return;
            }
            case "DELUSER": {
                if (acl == null) { c.replyError("ERR ACL subsystem unavailable"); return; }
                int deleted = 0;
                for (int j = 2; j < c.argc(); j++) {
                    if (acl.deleteUser(c.arg(j))) deleted++;
                }
                c.replyInteger(deleted);
                return;
            }
            case "GETUSER": {
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                if (acl == null) { c.replyError("ERR ACL subsystem unavailable"); return; }
                AclManager.RedisUser u = acl.getUser(c.arg(2));
                if (u == null) { c.replyNil(); return; }
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("flags", u.isEnabled() ? new String[]{"on", "nopass"} : new String[]{"off"});
                info.put("passwords", new ArrayList<>());
                info.put("commands", "+@all");
                info.put("keys", "~*");
                info.put("channels", "~*");
                info.put("selectors", new ArrayList<>());
                List<Object> out = new ArrayList<>();
                for (Map.Entry<String, Object> e : info.entrySet()) {
                    out.add(e.getKey());
                    out.add(e.getValue());
                }
                c.replyArray(out);
                return;
            }
            case "LOG":
                c.replyEmptyArray();
                return;
            case "LOAD":
            case "SAVE":
                c.replyOK();
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "CAT [<category>] -- List commands by category.",
                        "DELUSER <username> [<username> ...]",
                        "GETUSER <username>",
                        "LIST -- List users and their rules.",
                        "SETUSER <username> [<rules>]",
                        "WHOAMI -- Return the current connection username.",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown ACL subcommand or wrong number of arguments for '" + sub + "'");
        }
    }

    // ===== CLUSTER =====

    public static void cluster(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        ClusterManager mgr = c.services().get(ClusterManager.class);
        switch (sub) {
            case "ADDSLOTS":
            case "DELSLOTS": {
                String action = sub;
                for (int j = 2; j < c.argc(); j++) {
                    Integer slot = Args.parseInt(c, j);
                    if (slot == null) return;
                    if (slot < 0 || slot >= 16384) {
                        c.replyError("ERR Invalid or out of range slot"); return;
                    }
                    if (mgr == null) {
                        c.replyError("ERR Cluster subsystem unavailable"); return;
                    }
                    try {
                        if (action.equals("ADDSLOTS")) {
                            mgr.assignSlot(slot, mgr.getMyself());
                        } else {
                            mgr.assignSlot(slot, null);
                        }
                    } catch (Throwable t) {
                        c.replyError("ERR " + t.getClass().getSimpleName() + ": " + t.getMessage());
                        return;
                    }
                }
                c.replyOK();
                return;
            }
            case "SETSLOT": {
                // CLUSTER SETSLOT <slot> (IMPORTING|migrating|STABLE|NODE) <node-id>
                if (c.argc() < 4) { c.replyWrongArgc(); return; }
                // Phase 8: accept and ack without full migration state machine.
                c.replyOK();
                return;
            }
            case "INFO": {
                StringBuilder sb = new StringBuilder();
                int assigned = mgr == null ? 0 : mgr.getMyself().getSlotCount();
                sb.append("cluster_enabled:").append(mgr == null ? 0 : (mgr.isClusterEnabled() ? 1 : 0)).append("\r\n");
                sb.append("cluster_state:ok\r\n");
                sb.append("cluster_slots_assigned:").append(assigned).append("\r\n");
                sb.append("cluster_slots_ok:").append(assigned).append("\r\n");
                sb.append("cluster_slots_fail:0\r\n");
                sb.append("cluster_known_nodes:1\r\n");
                sb.append("cluster_size:1\r\n");
                sb.append("cluster_current_epoch:0\r\n");
                sb.append("cluster_my_epoch:0\r\n");
                c.replyBulk(sb.toString());
                return;
            }
            case "NODES":
                // Minimal: one node, ourselves.
                c.replyBulk("");
                return;
            case "MYID":
                c.replyBulk(mgr == null ? "0000000000000000000000000000000000000000" : mgr.getMyself().getNodeId());
                return;
            case "SLOTS":
                // Empty array — single-node cluster not supported in Phase 0.
                c.replyEmptyArray();
                return;
            case "SHARDS":
                c.replyEmptyArray();
                return;
            case "COUNTKEYSINSLOT":
                c.replyInteger(0);
                return;
            case "KEYSLOT": {
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                c.replyInteger(io.github.redis.cluster.CRC16.keyHashSlot(c.arg(2)));
                return;
            }
            case "COUNT-FAILURE-REPORTS":
                c.replyInteger(0);
                return;
            case "FORGET":
            case "MEET":
            case "FLUSHSLOTS":
            case "REPLICATE":
            case "FAILOVER":
            case "SET-CONFIG-EPOCH":
            case "BUMPEPOCH":
                c.replyOK();
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "INFO -- Return information about the cluster.",
                        "MYID -- Return the node id of this master.",
                        "NODES -- Return cluster nodes info.",
                        "SLOTS -- Return slots info.",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown CLUSTER subcommand or wrong number of arguments for '" + sub + "'");
        }
    }

    // ===== MODULE =====

    public static void module(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        io.github.redis.module.ModuleManager mgr =
                c.services().get(io.github.redis.module.ModuleManager.class);
        switch (sub) {
            case "LIST": {
                List<Object> out = new ArrayList<>();
                if (mgr != null) {
                    for (io.github.redis.module.ModuleManager.LoadedModule m : mgr.all()) {
                        List<Object> row = new ArrayList<>();
                        row.add("name"); row.add(m.name);
                        row.add("ver"); row.add((long) m.apiVersion);
                        out.add(row);
                    }
                }
                c.replyArray(out);
                return;
            }
            case "LOAD": {
                // Phase 8: MODULE LOAD accepts a path and registers it as a
                // built-in module using the basename (sans extension).
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                String path = c.arg(2);
                int slash = path.lastIndexOf('/');
                String name = (slash < 0 ? path : path.substring(slash + 1));
                int dot = name.lastIndexOf('.');
                if (dot > 0) name = name.substring(0, dot);
                if (mgr == null) {
                    c.replyError("ERR Module subsystem unavailable"); return;
                }
                if (mgr.get(name) != null) {
                    c.replyError("ERR Module is already loaded"); return;
                }
                mgr.register(new io.github.redis.module.ModuleManager.LoadedModule(name, 1));
                c.replyOK();
                return;
            }
            case "UNLOAD": {
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                if (mgr == null) { c.replyError("ERR Module subsystem unavailable"); return; }
                String name = c.arg(2);
                if (mgr.unregister(name) == null) {
                    c.replyError("ERR Module not loaded"); return;
                }
                c.replyOK();
                return;
            }
            case "LOADEX":
                c.replyError("ERR Module loading not supported in this build");
                return;
            default:
                c.replyError("ERR Unknown MODULE subcommand: " + sub);
        }
    }

    // ===== MEMORY (top-level dispatcher) =====

    public static void memoryDispatch(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "USAGE":  io.github.redis.command.handler.ServerCommands.memoryUsage(c); return;
            case "STATS":  io.github.redis.command.handler.ServerCommands.memoryStats(c); return;
            case "DOCTOR": io.github.redis.command.handler.ServerCommands.memoryDoctor(c); return;
            case "MALLOC-STATS":
            case "PURGE":
                c.replyOK(); return;
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

    // ===== SLOWLOG / LATENCY / MONITOR =====

    public static void slowlog(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "GET":
            case "RESET":
            case "LEN":
                if (sub.equals("LEN")) c.replyInteger(0);
                else if (sub.equals("RESET")) c.replyOK();
                else c.replyEmptyArray();
                return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
                        "GET [<count>] -- Return top <count> entries from slow log.",
                        "LEN -- Return length of slow log.",
                        "RESET -- Reset slow log.",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown SLOWLOG subcommand: " + sub);
        }
    }

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

    public static void monitor(RedisClient c) {
        // MONITOR is not supported in Phase 0; return OK to keep clients tolerant.
        c.replyOK();
    }
}
