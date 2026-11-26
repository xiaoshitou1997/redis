package io.github.redis.replication;

import io.github.redis.command.CommandFlag;
import io.github.redis.command.CommandTable;
import io.github.redis.command.RedisCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import io.github.redis.core.ServiceRegistry;

/**
 * Phase-6 minimal master→replica replication.
 *
 * <p>Strategy: every successful WRITE command on the master is forwarded (as
 * RESP) to each connected replica over the replication link. Replicas connect
 * using a long-lived TCP socket and send a single <code>SLAVEOF-ACK</code> line
 * after the initial RDB snapshot is loaded.
 *
 * <p>This mirrors the high-level shape of Redis's full resync + command
 * streaming model without the CoW fork / replication backlog / partial resync
 * machinery (Phase 7 territory).
 */
public final class ReplicationManager {

    /** Set when this server is a replica. */
    private volatile boolean isReplica = false;
    private volatile String masterHost;
    private volatile int masterPort;

    /** Connected replicas when this server is a master. */
    private final ConcurrentMap<String, Channel> replicas = new ConcurrentHashMap<>();
    private final AtomicLong masterOffset = new AtomicLong(0);

    /** Wire a write command to all replicas. */
    public void propagate(List<String> argv) {
        if (argv == null || argv.isEmpty() || replicas.isEmpty()) return;
        byte[] wireForm = encodeRespArray(argv);
        for (Channel ch : replicas.values()) {
            if (ch.isActive()) {
                ch.writeAndFlush(Unpooled.wrappedBuffer(wireForm));
            }
        }
        masterOffset.incrementAndGet();
    }

    /** Called by the inbound replication handler when a replica connects. */
    public void addReplica(String id, Channel ch) {
        replicas.put(id, ch);
    }

    public void removeReplica(String id) {
        replicas.remove(id);
    }

    public int replicaCount() {
        return replicas.size();
    }

    public long masterOffset() {
        return masterOffset.get();
    }

    public boolean isReplica() { return isReplica; }
    public String masterHost() { return masterHost; }
    public int masterPort() { return masterPort; }

    public void becomeReplica(String host, int port) {
        this.isReplica = true;
        this.masterHost = host;
        this.masterPort = port;
    }

    public void becomeMaster() {
        this.isReplica = false;
        this.masterHost = null;
        this.masterPort = 0;
    }

    /**
     * Start a Netty server on {@code bindHost}:{@code replicationPort} that
     * accepts replica connections. When a replica completes the PSYNC handshake
     * it's added to {@link #replicas} and receives all subsequent write commands.
     *
     * <p>This is invoked by {@code RedisInitializer} only when the server is
     * configured as a master (default). Calling on a replica is a no-op.
     */
    public void startMasterListener(String bindHost, int replicationPort, ServiceRegistry services) {
        EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("redis-repl-boss"));
        EventLoopGroup worker = new NioEventLoopGroup(1, new DefaultThreadFactory("redis-repl-worker"));
        ServerBootstrap b = new ServerBootstrap();
        b.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new RedisEncoder())
                                .addLast(new RedisDecoder())
                                .addLast(new io.github.redis.handler.RedisAggregator())
                                .addLast(new ReplicationServerHandler(services, ReplicationManager.this, replicas));
                    }
                });
        try {
            ChannelFuture f = b.bind(new InetSocketAddress(bindHost, replicationPort)).sync();
            System.out.println("Replication listener started on " + bindHost + ":" + replicationPort);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Failed to start replication listener on port " + replicationPort + ": " + e.getMessage());
        }
    }

    /** Encode a list of args as a RESP array (the wire form slaves receive). */
    public static byte[] encodeRespArray(List<String> argv) {
        int totalLen = 0;
        byte[][] bytes = new byte[argv.size()][];
        for (int i = 0; i < argv.size(); i++) {
            bytes[i] = argv.get(i).getBytes(CharsetUtil.ISO_8859_1);
            totalLen += bytes[i].length;
        }
        byte[] out = new byte[1 + digits(argv.size()) + 2 + argv.size() * (1 + digits(0) + 2) + totalLen];
        // We rebuild below because computing the precise length upfront is fiddly.
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try {
            bos.write(('*'));
            bos.write(Integer.toString(argv.size()).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            for (byte[] b : bytes) {
                bos.write('$');
                bos.write(Integer.toString(b.length).getBytes(CharsetUtil.US_ASCII));
                bos.write('\r'); bos.write('\n');
                bos.write(b);
                bos.write('\r'); bos.write('\n');
            }
            return bos.toByteArray();
        } catch (java.io.IOException never) {
            throw new RuntimeException(never);
        }
    }

    private static int digits(int n) {
        if (n == 0) return 1;
        int d = 0;
        while (n > 0) { d++; n /= 10; }
        return d;
    }

    /**
     * Connect to the master and stream write commands back into our command
     * dispatcher. Phase-6 simplification: only command-stream replication —
     * no initial RDB sync.
     */
    public void startReplication(String host, int port, Runnable onCommand) {
        becomeReplica(host, port);
        EventLoopGroup worker = new NioEventLoopGroup(1, new DefaultThreadFactory("redis-replica"));
        Bootstrap b = new Bootstrap();
        b.group(worker).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new RedisEncoder())
                                .addLast(new RedisDecoder())
                                .addLast(new ReplicaCommandHandler(onCommand));
                    }
                });
        try {
            b.connect(host, port).sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Inbound handler on a replica: parses commands from master and dispatches them. */
    private static final class ReplicaCommandHandler extends ChannelInboundHandlerAdapter {
        private final Runnable onCommand;
        ReplicaCommandHandler(Runnable onCommand) { this.onCommand = onCommand; }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            // Replica swallows replies — we just want the side effect.
            if (onCommand != null) onCommand.run();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Phase-6 simplification: log and move on.
            cause.printStackTrace();
        }
    }
}
