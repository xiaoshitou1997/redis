package io.github.redis.persistence;

import io.github.redis.command.CommandTable;
import io.github.redis.config.RedisConfig;
import io.github.redis.core.CommandDispatcher;
import io.github.redis.core.ServiceRegistry;
import io.github.redis.persistence.aof.AofReader;
import io.github.redis.persistence.aof.AofRewriter;
import io.github.redis.persistence.aof.AofWriter;
import io.github.redis.persistence.rdb.RdbSaver;
import io.github.redis.persistence.rdb.StorageSnapshot;
import io.github.redis.storage.Storage;
import io.github.redis.storage.StorageManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Persistence manager.
 *
 * <p>Owns the RDB saver, AOF writer, and the listener that hooks successful
 * writes back into the persistence subsystem.
 *
 * <p>Phase-1 features:
 * <ul>
 *   <li>AOF replay on startup (via {@link AofReader}).</li>
 *   <li>Manual AOF rewrite via {@link #rewriteAof()} (BGREWRITEAOF).</li>
 *   <li>Auto-rewrite check on every Nth write.</li>
 * </ul>
 */
public class PersistenceManager {

    private final RedisConfig config;
    private final StorageManager storageManager;
    private final CommandTable commandTable;
    private final ServiceRegistry serviceRegistry;

    private RdbSaver rdbSaver;
    private AofWriter aofWriter;
    private AofReader aofReader;
    private PersistenceListener persistenceListener;
    private CommandDispatcher dispatcher;

    private final AtomicBoolean rewriteInProgress = new AtomicBoolean(false);

    public PersistenceManager(RedisConfig config, StorageManager storageManager,
                              CommandTable commandTable, ServiceRegistry serviceRegistry) {
        this.config = config;
        this.storageManager = storageManager;
        this.commandTable = commandTable;
        this.serviceRegistry = serviceRegistry;
    }

    public void initialize() {
        rdbSaver = new RdbSaver(storageManager, config);
        rdbSaver.startPeriodicSave();

        aofWriter = new AofWriter(config);
        aofWriter.initialize();

        aofReader = new AofReader(commandTable, storageManager, serviceRegistry);

        persistenceListener = new PersistenceListener(aofWriter, rdbSaver, commandTable);

        System.out.println("Persistence manager initialized");
    }

    public void setDispatcher(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    /**
     * Load data at startup. AOF takes priority over RDB when both exist.
     */
    public void loadData() {
        boolean loaded = false;

        if (config.isAppendOnly()) {
            try {
                loaded = aofReader.replay(config);
            } catch (Exception e) {
                System.err.println("AOF replay failed: " + e.getMessage());
                e.printStackTrace();
            }
        }

        if (!loaded && rdbSaver != null) {
            loaded = rdbSaver.load();
        }

        if (!loaded) {
            System.out.println("No persistence file found, starting with empty database");
        }
    }

    public void saveData() {
        System.out.println("Saving data before shutdown...");
        if (aofWriter != null && aofWriter.isEnabled()) aofWriter.forceSync();
        if (rdbSaver != null) {
            // SAVE on shutdown can run on the dispatcher thread (single thread
            // blocking for a few ms is fine for an orderly shutdown).
            rdbSaver.save();
        }
        System.out.println("Data saved successfully");
    }

    public PersistenceListener getPersistenceListener() {
        return persistenceListener;
    }

    public RdbSaver getRdbSaver() { return rdbSaver; }
    public AofWriter getAofWriter() { return aofWriter; }

    /**
     * Rewrite the AOF from current state. Snapshot is taken on the dispatcher
     * thread for consistency; encoding and file replacement happen in-line
     * (the rewritten bytes are buffered first).
     *
     * @return true on success
     */
    public boolean rewriteAof() {
        if (!aofWriter.isEnabled()) return false;
        if (!rewriteInProgress.compareAndSet(false, true)) return false;

        try {
            // 1. Snapshot on dispatcher thread.
            Storage[] storages = getAllStorages();
            final StorageSnapshot[] box = new StorageSnapshot[1];
            if (dispatcher != null) {
                dispatcher.submitAndWait(() -> box[0] = new StorageSnapshot(storages));
            } else {
                box[0] = new StorageSnapshot(storages);
            }
            StorageSnapshot snap = box[0];

            // 2. Encode to a byte[] in memory (Phase 1 simplification; for large
            //    datasets we'd stream directly to the temp file).
            ByteArrayOutputStream tmp = new ByteArrayOutputStream();
            AofRewriter.write(tmp, snap);
            byte[] bytes = tmp.toByteArray();

            // 3. Atomically replace the AOF file via AofWriter.
            aofWriter.rewriteFrom(new ByteArrayInputStream(bytes));
            return true;
        } catch (Exception e) {
            System.err.println("AOF rewrite failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            rewriteInProgress.set(false);
        }
    }

    public boolean isRewriteInProgress() {
        return rewriteInProgress.get();
    }

    private Storage[] getAllStorages() {
        Storage[] out = new Storage[storageManager.getDatabaseCount()];
        for (int i = 0; i < out.length; i++) out[i] = storageManager.getStorage(i);
        return out;
    }

    public void shutdown() {
        if (aofWriter != null) aofWriter.shutdown();
        if (rdbSaver != null)  rdbSaver.shutdown();
        System.out.println("Persistence manager shut down");
    }
}
