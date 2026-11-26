package io.github.redis.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class ClusterLink {

    private final ClusterNode remoteNode;
    private final ClusterNode localNode;
    private EventLoopGroup eventLoopGroup;
    private Channel channel;
    private final ClusterMessageHandler messageHandler;
    private volatile boolean connected = false;
    private volatile boolean reconnecting = false;

    public ClusterLink(ClusterNode localNode, ClusterNode remoteNode) {
        this.localNode = localNode;
        this.remoteNode = remoteNode;
        this.messageHandler = new ClusterMessageHandler(localNode, remoteNode);
    }

    public void connect() {
        if (connected || reconnecting) {
            return;
        }

        reconnecting = true;

        eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("cluster-link-" + remoteNode.getNodeId()));

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new RedisDecoder())
                                .addLast(new RedisBulkStringAggregator())
                                .addLast(new RedisArrayAggregator())
                                .addLast(new RedisEncoder())
                                .addLast(messageHandler);
                    }
                });

        bootstrap.connect(remoteNode.getHost(), remoteNode.getPort()).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                channel = future.channel();
                connected = true;
                reconnecting = false;
                System.out.println("Cluster link established: " + localNode.getAddress() + " -> " + remoteNode.getAddress());
                
                channel.closeFuture().addListener((ChannelFuture closeFuture) -> {
                    connected = false;
                    System.out.println("Cluster link closed: " + remoteNode.getAddress());
                    scheduleReconnect();
                });
            } else {
                connected = false;
                reconnecting = false;
                System.err.println("Failed to connect to cluster node: " + remoteNode.getAddress() + 
                                 ", error: " + future.cause().getMessage());
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (!connected && !reconnecting) {
            eventLoopGroup.schedule(this::connect, 5, TimeUnit.SECONDS);
        }
    }

    public void disconnect() {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
        connected = false;
    }

    public void sendMessage(ClusterMessage message) {
        if (!connected || channel == null || !channel.isActive()) {
            System.err.println("Cannot send message to " + remoteNode.getAddress() + ": not connected");
            return;
        }

        channel.writeAndFlush(message.toRedisCommand());
    }

    public boolean isConnected() {
        return connected;
    }

    public ClusterNode getRemoteNode() {
        return remoteNode;
    }

    public ClusterNode getLocalNode() {
        return localNode;
    }
}