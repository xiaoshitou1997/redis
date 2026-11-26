package io.github.redis.replication;

import io.github.redis.core.RedisClient;
import io.github.redis.core.ServiceRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase-8 master-side replication handler.
 *
 * <p>Master recognises replica connections via the standard Redis handshake:
 * replica sends <code>PING</code>, <code>REPLCONF listening-port X</code>,
 * <code>REPLCONF capa eof capa psync2</code>, then either
 * <code>PSYNC ? -1</code> (full resync) or <code>PSYNC &lt;id&gt; &lt;off&gt;</code>
 * (partial resync). Phase 8 always responds with full resync and then streams
 * write commands.
 *
 * <p>Initial sync emits a minimal empty RDB marker ({@code $0\r\n\r\n} — Phase 8
 * does not stream a real snapshot; replicas receive only future commands).
 * This is enough to make replicas converge from a clean starting state and
 * is suitable for testing/learning — not production.
 */
public final class ReplicationServerHandler extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(ReplicationServerHandler.class);

    private final ServiceRegistry services;
    private final ReplicationManager replication;
    /** Replica ID → channel. */
    private final Map<String, Channel> replicas;
    private final AtomicLong nextReplicaId = new AtomicLong(1);

    /** Per-channel handshake state — added as a channel attribute. */
    private boolean helloSeen = false;
    private String replicaId = null;
    private int replicaPort = 0;

    public ReplicationServerHandler(ServiceRegistry services, ReplicationManager replication,
                                    Map<String, Channel> replicas) {
        this.services = services;
        this.replication = replication;
        this.replicas = replicas;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RedisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }
        // Convert argv from the inbound message.
        String[] argv = toStringArgv((RedisMessage) msg);
        log.info("Replication in: type={} argv={}", msg.getClass().getSimpleName(),
                argv == null ? "null" : java.util.Arrays.toString(argv));
        if (argv == null || argv.length == 0) {
            replyError(ctx, "ERR invalid replication request");
            return;
        }
        String cmd = argv[0].toUpperCase();
        switch (cmd) {
            case "PING":
                ctx.writeAndFlush(new SimpleStringRedisMessage("PONG"));
                return;
            case "REPLCONF":
                // REPLCONF listening-port N | REPLCONF capa eof capa psync2 | ...
                parseReplconf(argv);
                ctx.writeAndFlush(new SimpleStringRedisMessage("OK"));
                return;
            case "PSYNC":
            case "SYNC":
                // Phase 8 simplification: always do full resync.
                // Reply: +FULLRESYNC <replid> <offset>\r\n
                // then send an empty RDB: $0\r\n\r\n
                replicaId = "replica-" + nextReplicaId.getAndIncrement();
                replicas.put(replicaId, ctx.channel());
                replication.addReplica(replicaId, ctx.channel());
                ctx.write(new SimpleStringRedisMessage(
                        "FULLRESYNC " + replicationMasterId() + " " + replication.masterOffset()));
                // Send empty RDB marker.
                ctx.write(new FullBulkStringRedisMessage(Unpooled.EMPTY_BUFFER));
                ctx.writeAndFlush(new SimpleStringRedisMessage("CONTINUE"));
                helloSeen = true;
                log.info("Replica {} attached from {}", replicaId, ctx.channel().remoteAddress());
                return;
            case "REPLICAOF":
            case "SLAVEOF":
                // Standard SLAVEOF/REPLICAOF accepted on data link for compatibility.
                ctx.writeAndFlush(new SimpleStringRedisMessage("OK"));
                return;
            case "ACK":
                // Replica → master ACK messages are received but currently ignored.
                return;
            default:
                // Unknown command — log and reply OK to avoid dropping the connection.
                replyError(ctx, "ERR unknown replication command: " + cmd);
                return;
        }
    }

    private void parseReplconf(String[] argv) {
        for (int i = 1; i + 1 < argv.length; i += 2) {
            String opt = argv[i].toLowerCase();
            if (opt.equals("listening-port")) {
                try { replicaPort = Integer.parseInt(argv[i + 1]); }
                catch (NumberFormatException ignored) {}
            }
        }
    }

    /** Build a deterministic replication ID (Phase 8: derived from the master offset). */
    private String replicationMasterId() {
        // Redis uses a random 40-char hex; we return a stable 16-char hex instead.
        return Long.toHexString(replication.masterOffset() ^ System.nanoTime());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (replicaId != null) {
            replicas.remove(replicaId);
            replication.removeReplica(replicaId);
            log.info("Replica {} disconnected", replicaId);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Replication link error from {}: {}",
                ctx.channel().remoteAddress(), cause.getMessage());
        ctx.close();
    }

    @SuppressWarnings("unchecked")
    private static String[] toStringArgv(RedisMessage message) {
        if (message instanceof ArrayRedisMessage) {
            List<RedisMessage> children = ((ArrayRedisMessage) message).children();
            if (children == null || children.isEmpty()) return null;
            String[] argv = new String[children.size()];
            for (int i = 0; i < children.size(); i++) {
                argv[i] = extractString(children.get(i));
                if (argv[i] == null) return null;
            }
            return argv;
        }
        if (message instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) message;
            if (bulk.isNull()) return null;
            return new String[] { bulk.content().toString(CharsetUtil.ISO_8859_1) };
        }
        if (message instanceof SimpleStringRedisMessage) {
            return new String[] { ((SimpleStringRedisMessage) message).content() };
        }
        return null;
    }

    private static String extractString(RedisMessage msg) {
        if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) msg;
            if (bulk.isNull()) return null;
            return bulk.content().toString(CharsetUtil.ISO_8859_1);
        }
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        }
        if (msg instanceof io.netty.handler.codec.redis.IntegerRedisMessage) {
            return Long.toString(((io.netty.handler.codec.redis.IntegerRedisMessage) msg).value());
        }
        return null;
    }

    private static void replyError(ChannelHandlerContext ctx, String msg) {
        ctx.writeAndFlush(new ErrorRedisMessage(msg));
    }
}
