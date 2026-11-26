package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.replication.ReplicationManager;

/**
 * SLAVEOF / REPLICAOF command handler.
 *
 * <p>Phase-6 minimal: just flips the {@link ReplicationManager} role flag.
 * Full master→replica streaming replication requires the network connect
 * handshake which is set up by {@code ReplicationManager.startReplication};
 * here we accept the SLAVEOF command and record intent.
 *
 * <p>SLAVEOF NO ONE returns the server to master mode.
 * SLAVEOF NO ONE / SLAVEOF host port are both supported.
 */
public final class ReplicationCommands {

    private ReplicationCommands() {}

    public static void slaveof(RedisClient c) {
        // SLAVEOF host port | SLAVEOF NO ONE
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        ReplicationManager rm = c.services().get(ReplicationManager.class);
        if (rm == null) { c.replyError("ERR replication subsystem unavailable"); return; }
        String host = c.arg(1);
        String portStr = c.arg(2);
        if (host.equalsIgnoreCase("NO") && portStr.equalsIgnoreCase("ONE")) {
            rm.becomeMaster();
            c.replyOK();
            return;
        }
        int port;
        try { port = Integer.parseInt(portStr); }
        catch (NumberFormatException e) { c.replyError("ERR port is not an integer"); return; }
        rm.becomeReplica(host, port);
        c.replyOK();
    }

    /** Alias of SLAVEOF (Redis 5+ renamed). */
    public static void replicaof(RedisClient c) { slaveof(c); }

    /** INFO replication helper — invoked by ObservabilityCommands when section=replication. */
    public static String buildReplicationSection(ReplicationManager rm) {
        if (rm == null) {
            return "role:master\r\nconnected_slaves:0\r\n";
        }
        StringBuilder sb = new StringBuilder();
        if (rm.isReplica()) {
            sb.append("role:slave\r\n");
            sb.append("master_host:").append(rm.masterHost()).append("\r\n");
            sb.append("master_port:").append(rm.masterPort()).append("\r\n");
            sb.append("master_link_status:up\r\n");
        } else {
            sb.append("role:master\r\n");
            sb.append("connected_slaves:").append(rm.replicaCount()).append("\r\n");
            sb.append("master_repl_offset:").append(rm.masterOffset()).append("\r\n");
        }
        return sb.toString();
    }
}
