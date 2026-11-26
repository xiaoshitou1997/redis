package io.github.redis.core;

import io.github.redis.acl.AclManager;
import io.github.redis.command.RedisCommand;
import io.github.redis.persistence.PersistenceListener;
import io.github.redis.persistence.PersistenceManager;
import io.github.redis.storage.Storage;
import io.github.redis.storage.StorageManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Per-connection client context. Replaces the ad-hoc {@link CommandExecutionContext}.
 *
 * <p>A RedisClient owns the channel, exposes the current command's argv/argc, the
 * selected database index, the authenticated user, plus convenience accessors to
 * common services (storage, ACL, persistence).
 *
 * <p>Reply API mirrors Redis's addReplyXXX family — handlers write responses
 * directly via {@link #replyOK()}, {@link #replyError(String)}, etc.
 */
public class RedisClient {

    private static final int MAX_DATABASES = 16;
    private static final String CLIENT_ATTR_KEY = "redis.client";

    private final ChannelHandlerContext ctx;
    private final ServiceRegistry serviceRegistry;

    /** Current command argv. argv[0] is the command name. */
    private String[] argv;
    /** Current command argc (= argv.length for inline commands, may differ for pipelined). */
    private int argc;

    private int dbIndex = 0;
    private String authenticatedUser;

    /** Transaction state: when non-null, the client is inside MULTI/EXEC. */
    private List<String[]> queuedCommands;
    private boolean inTransaction;
    private boolean transactionDirty;
    private String transactionAbortReason;

    /** Blocking state: set by BLPOP-style commands before they return without replying. */
    private boolean blocked;
    private Runnable unblockHook;

    /** RESP protocol version negotiated via HELLO: 2 or 3 (default 2). */
    private volatile int protoVersion = 2;

    /** Set when this connection is the master→replica link (commands from master
     *  should not be re-propagated to other replicas). */
    private volatile boolean replica;

    public RedisClient(ChannelHandlerContext ctx, ServiceRegistry serviceRegistry) {
        this.ctx = ctx;
        this.serviceRegistry = serviceRegistry;
    }

    /**
     * Fetch or create the per-channel {@link RedisClient}. The same instance is
     * reused for every command on this channel — argv/argc are overwritten by
     * {@link #setCommand(String[], int)} before each dispatch.
     */
    public static RedisClient get(ChannelHandlerContext ctx) {
        io.netty.util.AttributeKey<RedisClient> key =
                io.netty.util.AttributeKey.valueOf(CLIENT_ATTR_KEY);
        RedisClient c = ctx.channel().attr(key).get();
        if (c == null) {
            io.netty.util.AttributeKey<ServiceRegistry> srKey =
                    io.netty.util.AttributeKey.valueOf("serviceRegistry");
            ServiceRegistry sr = ctx.channel().attr(srKey).get();
            if (sr == null) {
                throw new IllegalStateException("ServiceRegistry missing on channel");
            }
            c = new RedisClient(ctx, sr);
            ctx.channel().attr(key).set(c);
        }
        return c;
    }

    // ========== Command payload ==========

    /**
     * Set the current command's argument vector. Called by the dispatcher before
     * the handler runs.
     */
    public void setCommand(String[] argv, int argc) {
        this.argv = argv;
        this.argc = argc;
    }

    public String arg(int i) {
        return argv[i];
    }

    public int argc() {
        return argc;
    }

    /** argv[0] upper-cased; never null. */
    public String command() {
        return argv[0].toUpperCase();
    }

    // ========== Channel / DB / Auth ==========

    public Channel channel() {
        return ctx.channel();
    }

    public ChannelHandlerContext ctx() {
        return ctx;
    }

    public ServiceRegistry services() {
        return serviceRegistry;
    }

    public int dbIndex() {
        return dbIndex;
    }

    public void setDbIndex(int i) {
        if (i < 0 || i >= MAX_DATABASES) {
            throw new IllegalArgumentException("DB number must be 0.." + (MAX_DATABASES - 1));
        }
        this.dbIndex = i;
    }

    public String authenticatedUser() {
        return authenticatedUser;
    }

    public void setAuthenticatedUser(String user) {
        this.authenticatedUser = user;
    }

    // ========== Service accessors ==========

    public Storage storage() {
        StorageManager sm = serviceRegistry.require(StorageManager.class);
        return sm.getStorage(dbIndex);
    }

    public AclManager acl() {
        return serviceRegistry.get(AclManager.class);
    }

    public PersistenceListener persister() {
        PersistenceManager pm = serviceRegistry.get(PersistenceManager.class);
        return pm != null ? pm.getPersistenceListener() : null;
    }

    // ========== Transaction state (used by MULTI/EXEC in S3) ==========

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void beginTransaction() {
        this.inTransaction = true;
        this.queuedCommands = new ArrayList<>();
        this.transactionDirty = false;
        this.transactionAbortReason = null;
    }

    public void enqueueCommand(String[] argv) {
        if (queuedCommands != null) {
            queuedCommands.add(argv);
        }
    }

    public List<String[]> drainQueuedCommands() {
        List<String[]> q = queuedCommands;
        queuedCommands = null;
        inTransaction = false;
        return q;
    }

    public void markTransactionDirty(String reason) {
        this.transactionDirty = true;
        if (this.transactionAbortReason == null && reason != null) {
            this.transactionAbortReason = reason;
        }
    }

    public boolean isTransactionDirty() {
        return transactionDirty;
    }

    public String transactionAbortReason() {
        return transactionAbortReason;
    }

    /** Mark this client as blocked (no reply sent). Dispatcher checks this to skip persistence. */
    public void markBlocked() { this.blocked = true; }
    public void clearBlocked() { this.blocked = false; }
    public boolean isBlocked() { return blocked; }
    public Runnable getUnblockHook() { return unblockHook; }
    public void setUnblockHook(Runnable r) { this.unblockHook = r; }

    /** RESP protocol version negotiated via HELLO (2 or 3). */
    public int getProtoVersion() { return protoVersion; }
    public void setProtoVersion(int v) { this.protoVersion = (v == 3) ? 3 : 2; }

    /** Mark this connection as the master→replica link. */
    public void markReplica() { this.replica = true; }
    public boolean isReplica() { return replica; }

    // ========== Reply API ==========

    public void replyOK() {
        // Fast path: write the 5-byte "+OK\r\n" directly via a pooled buffer,
        // bypassing the RedisMessage type allocation. This is the most-called
        // reply on the hot path (SET, etc.).
        if (replyInterceptor != null) {
            write(new SimpleStringRedisMessage("OK"));
            return;
        }
        ByteBuf buf = ReplyBufferPool.DEFAULT.directBuffer(5, 5);
        buf.writeByte('+');
        buf.writeByte('O');
        buf.writeByte('K');
        buf.writeByte('\r');
        buf.writeByte('\n');
        ctx.writeAndFlush(buf);
    }

    /** Reply a simple-string status (e.g. "Background saving started"). */
    public void replyStatus(String status) {
        write(new SimpleStringRedisMessage(status));
    }

    public void replyError(String msg) {
        write(new ErrorRedisMessage(msg));
    }

    public void replySyntaxError() {
        replyError("ERR syntax error");
    }

    public void replyWrongArgc() {
        replyError("ERR wrong number of arguments for '" + command().toLowerCase() + "' command");
    }

    public void replyInteger(long n) {
        write(new IntegerRedisMessage(n));
    }

    public void replyBulk(String s) {
        if (s == null) {
            write(FullBulkStringRedisMessage.NULL_INSTANCE);
        } else {
            // Use pooled direct buffer (Phase-6 perf).
            write(new FullBulkStringRedisMessage(ReplyBufferPool.directLatin1(s)));
        }
    }

    public void replyBulk(byte[] bytes) {
        if (bytes == null) {
            write(FullBulkStringRedisMessage.NULL_INSTANCE);
        } else {
            write(new FullBulkStringRedisMessage(ReplyBufferPool.directBytes(bytes)));
        }
    }

    public void replyNil() {
        write(FullBulkStringRedisMessage.NULL_INSTANCE);
    }

    /**
     * Reply an array of arbitrary objects. Each element may be String, Number,
     * byte[], null (renders as nil bulk), or nested List. Boolean renders as 0/1.
     */
    public void replyArray(List<?> list) {
        if (list == null) {
            write(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        List<RedisMessage> children = new ArrayList<>(list.size());
        for (Object item : list) {
            children.add(toRedisMessage(item));
        }
        write(new ArrayRedisMessage(children));
    }

    /** Reply an empty array — useful for commands like KEYS with no matches. */
    public void replyEmptyArray() {
        write(new ArrayRedisMessage(new ArrayList<RedisMessage>(0)));
    }

    /**
     * Reply a boolean value. In RESP2 this becomes an integer (0/1); in RESP3
     * it uses the dedicated boolean type {@code #t}/{@code #f}.
     */
    public void replyBool(boolean b) {
        if (protoVersion == 3) {
            writeRaw(new io.github.redis.protocol.Resp3Encoder.BooleanReply(b));
        } else {
            write(new IntegerRedisMessage(b ? 1L : 0L));
        }
    }

    /**
     * Reply a big-number (RESP3 only). Falls back to bulk-string in RESP2.
     */
    public void replyBigNumber(String decimal) {
        if (protoVersion == 3) {
            write(new SimpleStringRedisMessage("(" + decimal));
        } else {
            replyBulk(decimal);
        }
    }

    /**
     * Reply a verbatim string with explicit encoding hint (RESP3 only).
     * Falls back to bulk-string in RESP2.
     */
    public void replyVerbatim(String encoding, String text) {
        if (protoVersion == 3) {
            write(new SimpleStringRedisMessage("=" + encoding + ":" + text));
        } else {
            replyBulk(text);
        }
    }

    /**
     * Reply a map. In RESP3 emits the dedicated map type {@code %<n>\r\n...};
     * in RESP2 flattens to a flat array [k1, v1, k2, v2, ...].
     */
    public void replyMap(java.util.Map<?, ?> map) {
        if (map == null) {
            replyNil();
            return;
        }
        if (protoVersion == 3) {
            writeRaw(new io.github.redis.protocol.Resp3Encoder.MapReply(map));
        } else {
            List<Object> flat = new ArrayList<>(map.size() * 2);
            for (Map.Entry<?, ?> e : map.entrySet()) {
                flat.add(e.getKey());
                flat.add(e.getValue());
            }
            replyArray(flat);
        }
    }

    /**
     * Reply a set. In RESP3 emits the dedicated set type {@code ~<n>\r\n...};
     * in RESP2 behaves like an array.
     */
    public void replySet(List<?> elements) {
        if (elements == null) {
            replyNil();
            return;
        }
        if (protoVersion == 3) {
            writeRaw(new io.github.redis.protocol.Resp3Encoder.SetReply(elements));
        } else {
            replyArray(elements);
        }
    }

    /**
     * Reply a double. In RESP3 uses {@code ,<float>\r\n}; in RESP2 emits the
     * value as a bulk string.
     */
    public void replyDouble(double d) {
        String s = (d == Math.rint(d) && !Double.isInfinite(d))
                ? Long.toString((long) d)
                : Double.toString(d);
        if (protoVersion == 3) {
            // Write the RESP3 double directly via a raw ByteBuf.
            byte[] bytes = s.getBytes(io.netty.util.CharsetUtil.US_ASCII);
            ByteBuf buf = ReplyBufferPool.DEFAULT.directBuffer(1 + bytes.length + 2);
            buf.writeByte(',');
            buf.writeBytes(bytes);
            buf.writeByte('\r'); buf.writeByte('\n');
            writeRaw(buf);
        } else {
            replyBulk(s);
        }
    }

    private RedisMessage toRedisMessage(Object item) {
        if (item == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        }
        if (item instanceof RedisMessage) {
            return (RedisMessage) item;
        }
        if (item instanceof String) {
            String s = (String) item;
            // Conventional: bare "OK"/"PONG"/"QUEUED" -> simple string, otherwise bulk.
            if ("OK".equals(s) || "PONG".equals(s) || "QUEUED".equals(s)) {
                return new SimpleStringRedisMessage(s);
            }
            // Latin-1 via pooled buffer (Phase-6 perf).
            return new FullBulkStringRedisMessage(ReplyBufferPool.directLatin1(s));
        }
        if (item instanceof byte[]) {
            return new FullBulkStringRedisMessage(ReplyBufferPool.directBytes((byte[]) item));
        }
        if (item instanceof Long) {
            return new IntegerRedisMessage((Long) item);
        }
        if (item instanceof Integer) {
            return new IntegerRedisMessage(((Integer) item).longValue());
        }
        if (item instanceof Boolean) {
            return new IntegerRedisMessage(((Boolean) item) ? 1L : 0L);
        }
        if (item instanceof List) {
            List<?> sub = (List<?>) item;
            List<RedisMessage> children = new ArrayList<>(sub.size());
            for (Object o : sub) {
                children.add(toRedisMessage(o));
            }
            return new ArrayRedisMessage(children);
        }
        // Fallback: stringify (Latin-1 via pooled buffer).
        String s = String.valueOf(item);
        return new FullBulkStringRedisMessage(ReplyBufferPool.directLatin1(s));
    }

    /** Optional reply interceptor — used by the Lua engine to capture replies. */
    public interface ReplyInterceptor {
        Object intercept(RedisMessage msg);
    }

    private volatile ReplyInterceptor replyInterceptor;

    public void setReplyInterceptor(ReplyInterceptor r) { this.replyInterceptor = r; }
    public ReplyInterceptor getReplyInterceptor() { return replyInterceptor; }

    private void write(RedisMessage msg) {
        if (replyInterceptor != null) {
            replyInterceptor.intercept(msg);
            return;
        }
        ctx.writeAndFlush(msg);
    }

    /** Write a raw object (RESP3 marker types, ByteBuf, etc.) bypassing the
     *  RedisMessage type system. */
    private void writeRaw(Object msg) {
        if (replyInterceptor != null && msg instanceof RedisMessage) {
            replyInterceptor.intercept((RedisMessage) msg);
            return;
        }
        ctx.writeAndFlush(msg);
    }
}
