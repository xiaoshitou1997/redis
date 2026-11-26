package io.github.redis.bootstrap;

import io.github.redis.config.RedisConfig;
import io.github.redis.handler.RedisInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Redis server bootstrap class
 * Initializes and starts Redis service with Netty
 */
public class Redis {

    /**
     * Boss thread group, accepts connections
     */
    private final EventLoopGroup bossGroup;

    /**
     * Worker thread group, handles I/O
     */
    private final EventLoopGroup workerGroup;

    /**
     * Server listening address
     */
    private final SocketAddress socketAddress;

    /**
     * Redis configuration
     */
    private final RedisConfig config;

    /**
     * Create Redis server instance (using default config)
     *
     * @param host listening host address
     * @param port listening port
     */
    public Redis(String host, int port) {
        this(host, port, null);
    }

    /**
     * Create Redis server instance
     *
     * @param host   listening host address
     * @param port   listening port
     * @param config Redis config (can be null, uses default)
     */
    public Redis(String host, int port, RedisConfig config) {
        this.config = config != null ? config : io.github.redis.config.ConfigLoader.createDefault();
        this.bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("redis-boss"));
        this.workerGroup = new NioEventLoopGroup(8, new DefaultThreadFactory("redis-worker"));
        this.socketAddress = new InetSocketAddress(host, port);
    }

    /**
     * Get configuration
     */
    public RedisConfig getConfig() {
        return config;
    }

    /**
     * Start Redis server
     * Blocks until server shuts down
     */

    private RedisInitializer redisInitializer;

    public void start() {
        try {
            // Create RedisInitializer (initializes persistence)
            redisInitializer = new RedisInitializer(config);

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(redisInitializer)
                    .bind(socketAddress)
                    .sync()
                    .channel()
                    .closeFuture()
                    .sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Redis server", e);
        } finally {
            shutdown();
        }
    }

    /**
     * Gracefully shutdown server
     */
    public void shutdown() {
        // Close persistence manager
        if (redisInitializer != null) {
            redisInitializer.shutdown();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}
