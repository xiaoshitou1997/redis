package io.github.redis.handler;

import io.github.redis.acl.AclManager;
import io.github.redis.cluster.CRC16;
import io.github.redis.cluster.ClusterManager;
import io.github.redis.cluster.ClusterNode;
import io.github.redis.command.CommandFlag;
import io.github.redis.command.CommandTable;
import io.github.redis.command.RedisCommand;
import io.github.redis.replication.ReplicationManager;
import io.github.redis.core.CommandDispatcher;
import io.github.redis.core.RedisClient;
import io.github.redis.observability.ServerMetrics;
import io.github.redis.persistence.PersistenceListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.InlineCommandRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis-style command dispatcher (Netty handler).
 *
 * <p>Pipeline position: after {@code RedisDecoder} + {@code RedisAggregator}.
 * Receives a fully-parsed {@link ArrayRedisMessage}, converts argv to String[],
 * and submits the work to the single-threaded {@link CommandDispatcher} — this
 * guarantees Redis-style atomicity (no two commands execute concurrently).
 *
 * <p>Transaction handling: when the per-channel {@link RedisClient} is inside
 * MULTI, all commands except EXEC/DISCARD/MULTI/WATCH are queued rather than
 * executed.
 */
@ChannelHandler.Sharable
public class CommandParser extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(CommandParser.class);

    private final CommandTable commandTable;
    private final PersistenceListener persistenceListener;
    private final CommandDispatcher dispatcher;
    private final ServerMetrics metrics;

    public CommandParser(CommandTable commandTable) {
        this(commandTable, null, null, null);
    }

    public CommandParser(CommandTable commandTable, PersistenceListener persistenceListener,
                         CommandDispatcher dispatcher, ServerMetrics metrics) {
        this.commandTable = commandTable;
        this.persistenceListener = persistenceListener;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RedisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }
        final String[] argv;
        try {
            argv = toArgv((RedisMessage) msg);
        } catch (Exception e) {
            log.error("Error parsing incoming message: {}", msg.getClass().getSimpleName(), e);
            replyError(ctx, "ERR " + safeMsg(e));
            return;
        }
        if (argv == null || argv.length == 0) {
            replyError(ctx, "ERR invalid command format");
            return;
        }

        // Resolve the per-channel client on the worker thread (cheap; just an
        // attribute lookup). The dispatcher then runs the heavy command logic.
        final RedisClient client = RedisClient.get(ctx);

        if (dispatcher == null) {
            // Fallback path (no dispatcher configured) — run inline.
            // Useful for tests that construct the pipeline manually.
            try {
                dispatch(client, argv);
            } catch (Throwable t) {
                log.error("Inline dispatch failed", t);
                replyError(client, "ERR " + safeMsg(t));
            }
            return;
        }

        dispatcher.submit(() -> {
            try {
                dispatch(client, argv);
            } catch (Throwable t) {
                log.error("Dispatch threw", t);
                replyError(client, "ERR " + safeMsg(t));
            }
        });
    }

    private void dispatch(RedisClient client, String[] argv) {
        String name = argv[0].toUpperCase();
        RedisCommand cmd = commandTable.get(name);
        if (cmd == null) {
            replyError(client, "ERR unknown command '" + argv[0] + "'");
            return;
        }
        if (!cmd.checkArity(argv.length)) {
            replyError(client, "ERR wrong number of arguments for '" + name.toLowerCase() + "' command");
            return;
        }

        // Load argv into the client so the handler can read them.
        client.setCommand(argv, argv.length);

        // ACL enforcement: AUTH/HELLO/RESET/PING are always allowed so a client
        // can authenticate. Everything else requires an authenticated user and
        // the command must be in the user's allowed set (Redis: NOAUTH / NOPERM).
        if (!isAuthBypassed(name)) {
            AclManager acl = client.acl();
            if (acl != null && acl.isEnabled()) {
                String user = client.authenticatedUser();
                if (user == null) {
                    // Default user — if it has no password set, allow; otherwise NOAUTH.
                    AclManager.RedisUser def = acl.getUser("default");
                    boolean needsAuth = def != null && !def.isNoPassword()
                            && def.getPassword() != null;
                    if (needsAuth) {
                        replyError(client, "NOAUTH Authentication required.");
                        return;
                    }
                }
                // Command-level permission. Default setups grant "+@all" to the
                // default user, so only explicitly restricted users are affected.
                AclManager.RedisUser permitted = acl.getUser(user);
                if (permitted != null && !acl.checkCommandPermission(permitted, name)) {
                    replyError(client, "NOPERM User " + (user == null ? "default" : user)
                            + " is not allowed to execute the '" + name.toLowerCase() + "' command");
                    return;
                }
            }
        }

        // Cluster MOVED redirect: when cluster is enabled and the command's
        // key(s) don't hash to a slot owned by this node, reply with a MOVED
        // error so the client can retry against the right node.
        ClusterManager cm = client.services().get(ClusterManager.class);
        if (cm != null && cm.isClusterEnabled() && !cmd.getKeySpecs().isEmpty()) {
            List<String> keys = cmd.extractKeys(argv, argv.length);
            for (String key : keys) {
                int slot = CRC16.keyHashSlot(key);
                if (!cm.isMySlot(slot)) {
                    ClusterNode owner = cm.getNodeForSlot(slot);
                    String addr = (owner == null) ? "0.0.0.0:0" : owner.getAddress();
                    replyError(client, "MOVED " + slot + " " + addr);
                    return;
                }
            }
        }

        // Transaction queueing: everything except the transaction control commands.
        if (client.isInTransaction()
                && !name.equals("EXEC") && !name.equals("DISCARD")
                && !name.equals("MULTI") && !name.equals("WATCH") && !name.equals("UNWATCH")
                && !name.equals("RESET")) {
            client.enqueueCommand(argv);
            client.ctx().writeAndFlush(new SimpleStringRedisMessage("QUEUED"));
            return;
        }

        try {
            long t0 = metrics == null ? 0 : System.nanoTime();
            cmd.getHandler().execute(client);
            // If the handler marked the client blocked (BLPOP etc.) it has NOT
            // replied; skip persistence + metrics bookkeeping. The wakeup
            // callback will re-dispatch later.
            if (client.isBlocked()) return;
            // Persistence: record only successful WRITE commands, AFTER execution.
            if (persistenceListener != null && cmd.hasFlag(CommandFlag.WRITE)) {
                persistenceListener.onWriteCommand(toTokenList(argv));
            }
            // Replication: forward successful writes to replicas (when this server is a master).
            if (cmd.hasFlag(CommandFlag.WRITE) && !client.isReplica()) {
                ReplicationManager rm = client.services().get(ReplicationManager.class);
                if (rm != null && !rm.isReplica()) {
                    rm.propagate(toTokenList(argv));
                }
            }
            if (metrics != null) {
                metrics.onCommandComplete(name, System.nanoTime() - t0, argv.length, argv);
            }
        } catch (OutOfMemoryError oom) {
            // maxmemory violation surfaced by MemoryStorage.set()
            replyError(client, "OOM command not allowed when used memory > 'maxmemory'.");
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg == null || msg.isEmpty()) msg = e.getClass().getSimpleName();
            if (!msg.startsWith("ERR") && !msg.startsWith("WRONGTYPE")
                    && !msg.startsWith("NOSCRIPT") && !msg.startsWith("EXECABORT")
                    && !msg.startsWith("LOADING") && !msg.startsWith("READONLY")
                    && !msg.startsWith("MISCONF") && !msg.startsWith("BUSY")
                    && !msg.startsWith("NOAUTH") && !msg.startsWith("OOM")) {
                msg = "ERR " + msg;
            }
            log.debug("Handler '{}' failed: {}", name, msg);
            replyError(client, msg);
        }
    }

    /** Convert any supported incoming RedisMessage into a String[] argv. */
    private String[] toArgv(RedisMessage message) {
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
        if (message instanceof InlineCommandRedisMessage) {
            String content = ((InlineCommandRedisMessage) message).content();
            if (content == null) return null;
            content = content.trim();
            if (content.isEmpty()) return null;
            String[] parts = content.split("\\s+");
            List<String> tokens = new ArrayList<>(parts.length);
            for (String p : parts) if (!p.isEmpty()) tokens.add(p);
            return tokens.toArray(new String[0]);
        }
        // Single bulk string: treat as inline.
        if (message instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) message;
            if (bulk.isNull()) return null;
            // Latin-1 to preserve binary safety of argv (commands and keys/values).
            String content = bulk.content().toString(CharsetUtil.ISO_8859_1);
            return new String[] { content };
        }
        if (message instanceof SimpleStringRedisMessage) {
            return new String[] { ((SimpleStringRedisMessage) message).content() };
        }
        log.warn("Unsupported incoming message type: {}", message.getClass());
        return null;
    }

    private String extractString(RedisMessage msg) {
        if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) msg;
            if (bulk.isNull()) return null;
            // Latin-1 for binary safety.
            return bulk.content().toString(CharsetUtil.ISO_8859_1);
        }
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        }
        if (msg instanceof IntegerRedisMessage) {
            return Long.toString(((IntegerRedisMessage) msg).value());
        }
        return null;
    }

    private static List<String> toTokenList(String[] argv) {
        List<String> l = new ArrayList<>(argv.length);
        for (String s : argv) l.add(s);
        return l;
    }

    private static String safeMsg(Throwable t) {
        String m = t.getMessage();
        return (m == null || m.isEmpty()) ? t.getClass().getSimpleName() : m;
    }

    private static void replyError(ChannelHandlerContext ctx, String msg) {
        ctx.writeAndFlush(new ErrorRedisMessage(msg));
    }

    private static void replyError(RedisClient client, String msg) {
        client.replyError(msg);
    }

    /** Commands a client may run before authenticating. Matches Redis's list. */
    private static boolean isAuthBypassed(String upperName) {
        switch (upperName) {
            case "AUTH":
            case "HELLO":
            case "PING":
            case "RESET":
            case "QUIT":
            case "COMMAND":
            case "OPTIONS":
                return true;
            default:
                return false;
        }
    }
}
