package io.github.redis.handler;

import io.github.redis.acl.AclManager;
import io.github.redis.cluster.ClusterManager;
import io.github.redis.command.CommandRegistration;
import io.github.redis.command.CommandTable;
import io.github.redis.command.handler.ScriptCommands;
import io.github.redis.config.RedisConfig;
import io.github.redis.core.BlockingCoordinator;
import io.github.redis.core.CommandDispatcher;
import io.github.redis.core.ServiceRegistry;
import io.github.redis.core.SimpleServiceRegistry;
import io.github.redis.memory.EvictionPolicy;
import io.github.redis.memory.MemoryTracker;
import io.github.redis.module.ModuleManager;
import io.github.redis.observability.ServerMetrics;
import io.github.redis.persistence.PersistenceManager;
import io.github.redis.pubsub.PubSubManager;
import io.github.redis.replication.ReplicationManager;
import io.github.redis.scripting.LuaScriptEngine;
import io.github.redis.security.TlsFactory;
import io.github.redis.storage.StorageManager;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;

import java.util.UUID;

/**
 * Netty channel pipeline initializer. Owns process-wide singletons
 * (storage, ACL, cluster, pub/sub, persistence, command table).
 *
 * <p>Pipeline: encoder -> decoder -> aggregator -> dispatcher.
 * The INFO-level LoggingHandler that used to live here has been removed
 * (it logged every byte, killing QPS).
 */
public class RedisInitializer extends ChannelInitializer<SocketChannel> {

    private final CommandTable commandTable;
    private final CommandParser commandParser;
    private final CommandDispatcher dispatcher;
    private final ServiceRegistry serviceRegistry;
    private final RedisConfig config;
    private final SslContext sslContext;
    private PersistenceManager persistenceManager;
    private StorageManager storageManager;

    public RedisInitializer(RedisConfig config) {
        this.config = config;
        this.serviceRegistry = new SimpleServiceRegistry();

        // Storage
        this.storageManager = new StorageManager();
        serviceRegistry.register(StorageManager.class, storageManager);

        // maxmemory enforcement — Phase 1. Wire tracker into every backing storage
        // so OOM errors fire regardless of which DB the write hits.
        long maxBytes = config.getMaxMemoryBytes();
        EvictionPolicy policy = EvictionPolicy.parse(config.getMaxMemoryPolicy());
        MemoryTracker tracker = new MemoryTracker(storageManager, maxBytes, policy);
        storageManager.setMemoryTracker(tracker);
        serviceRegistry.register(MemoryTracker.class, tracker);

        // ACL
        AclManager aclManager = new AclManager();
        // If requirepass is configured, bind it as the default user's password.
        // This is the Redis convention: requirepass is sugar for
        // "ACL SETUSER default on ><pass> ~* +@all".
        if (config.getRequirePass() != null && !config.getRequirePass().isEmpty()) {
            aclManager.createUser("default", true, config.getRequirePass(),
                    "on", "~*", "+@all");
        }
        serviceRegistry.register(AclManager.class, aclManager);

        // Cluster (kept dormant when clusterEnabled=false)
        String nodeId = UUID.randomUUID().toString().replace("-", "");
        ClusterManager clusterManager = ClusterManager.initialize(nodeId, config.getBind(), config.getPort());
        if (config.isClusterEnabled()) {
            clusterManager.enableCluster();
        }
        serviceRegistry.register(ClusterManager.class, clusterManager);

        // Pub/Sub
        PubSubManager pubSubManager = new PubSubManager();
        serviceRegistry.register(PubSubManager.class, pubSubManager);

        // Replication (Phase 6) — minimal: SLAVEOF role switch + master forward hook.
        ReplicationManager replicationManager = new ReplicationManager();
        serviceRegistry.register(ReplicationManager.class, replicationManager);

        // Phase 8: start the master-side replication listener if configured.
        if (config.getReplicationPort() > 0) {
            replicationManager.startMasterListener(config.getBind(), config.getReplicationPort(), serviceRegistry);
        }

        // Command table — register all known commands
        this.commandTable = new CommandTable();
        CommandRegistration.registerAll(commandTable);
        serviceRegistry.register(CommandTable.class, commandTable);

        // Single-threaded dispatcher — every command runs through this thread,
        // giving us Redis-style atomic execution.
        this.dispatcher = new CommandDispatcher();
        serviceRegistry.register(CommandDispatcher.class, dispatcher);

        // Blocking-command coordinator (BLPOP / BZPOPMIN etc.) uses the dispatcher.
        BlockingCoordinator blocking = new BlockingCoordinator(dispatcher);
        serviceRegistry.register(BlockingCoordinator.class, blocking);

        // Metrics — collects command stats, slow log, latency, INFO payload.
        ServerMetrics metrics = new ServerMetrics();
        serviceRegistry.register(ServerMetrics.class, metrics);

        // Module registry (Phase 8) — built-in json/search/timeseries registered.
        ModuleManager moduleManager = new ModuleManager();
        moduleManager.register(new ModuleManager.LoadedModule("json", 1));
        moduleManager.register(new ModuleManager.LoadedModule("search", 1));
        moduleManager.register(new ModuleManager.LoadedModule("timeseries", 1));
        serviceRegistry.register(ModuleManager.class, moduleManager);

        // Lua script engine (Phase 4). Wired to the command table so scripts
        // can call any registered command via redis.call().
        LuaScriptEngine lua = new LuaScriptEngine(serviceRegistry, commandTable);
        serviceRegistry.register(LuaScriptEngine.class, lua);
        ScriptCommands.setEngine(lua);

        // TLS context (built lazily; null when TLS is disabled).
        SslContext ssl = null;
        if (config.isTlsEnabled()) {
            try {
                ssl = TlsFactory.buildServerContext(config);
                System.out.println("TLS enabled on data port " + config.getPort());
            } catch (javax.net.ssl.SSLException e) {
                System.err.println("Failed to build SSL context: " + e.getMessage());
                e.printStackTrace();
            }
        }
        this.sslContext = ssl;

        // Persistence (wired to the new command table so isWriteCommand is flag-driven)
        persistenceManager = new PersistenceManager(config, storageManager, commandTable, serviceRegistry);
        persistenceManager.setDispatcher(dispatcher);
        persistenceManager.initialize();
        // RdbSaver needs the dispatcher to take consistent snapshots.
        if (persistenceManager.getRdbSaver() != null) {
            persistenceManager.getRdbSaver().setDispatcher(dispatcher);
        }
        serviceRegistry.register(PersistenceManager.class, persistenceManager);
        storageManager.setPersistenceManager(persistenceManager);
        persistenceManager.loadData();

        this.commandParser = new CommandParser(commandTable, persistenceManager.getPersistenceListener(), dispatcher, metrics);
    }

    public void shutdown() {
        if (dispatcher != null) dispatcher.shutdown();
        io.github.redis.core.BlockingCoordinator bc = serviceRegistry.get(io.github.redis.core.BlockingCoordinator.class);
        if (bc != null) bc.shutdown();
        if (persistenceManager != null) persistenceManager.shutdown();
        if (storageManager != null)     storageManager.shutdown();
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        io.netty.util.AttributeKey<ServiceRegistry> srKey =
                io.netty.util.AttributeKey.valueOf("serviceRegistry");
        channel.attr(srKey).set(serviceRegistry);

        channel.closeFuture().addListener(future -> {
            String clientId = channel.id().asLongText();
            PubSubManager mgr = serviceRegistry.get(PubSubManager.class);
            if (mgr != null) mgr.removeClient(clientId);
        });

        channel.pipeline()
                .addLast("encoder", new RedisEncoder())
                .addLast("decoder", new RedisDecoder())
                .addLast("aggregator", new RedisAggregator())
                .addLast("dispatcher", commandParser)
                .addLast("exception", new ExceptionHandler());

        // Insert SSL handler at the very head of the pipeline when TLS is enabled.
        // Must come BEFORE the codec handlers so it decrypts before RESP parsing.
        if (sslContext != null) {
            SSLEngine engine = sslContext.newEngine(channel.alloc());
            engine.setUseClientMode(false);
            channel.pipeline().addFirst("ssl", new SslHandler(engine));
        }
    }

    /** Attach a RESP3 encoder to the channel so subsequent replies can use
     *  the dedicated map/set/boolean/big-number wire types. Called by HELLO
     *  when a client negotiates proto=3. Idempotent — adding twice is a no-op. */
    public static void enableResp3(io.netty.channel.Channel ch) {
        if (ch.pipeline().get(io.github.redis.protocol.Resp3Encoder.class) == null) {
            // Insert just before the RedisEncoder so RESP3 types are encoded
            // to bytes before the legacy encoder would try (and fail) on them.
            ch.pipeline().addAfter("encoder", "resp3", new io.github.redis.protocol.Resp3Encoder());
        }
    }
}
