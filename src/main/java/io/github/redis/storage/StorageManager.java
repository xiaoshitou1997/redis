package io.github.redis.storage;

import io.github.redis.memory.MemoryTracker;
import io.github.redis.persistence.PersistenceManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Storage manager. Owns N {@link MemoryStorage} instances (one per logical DB)
 * and a cleanup scheduler that periodically drops expired entries.
 *
 * <p>A {@link MemoryTracker} is shared across all storages so that maxmemory
 * is enforced consistently regardless of which DB the write targets.
 */
public class StorageManager {

    private static final int DEFAULT_DATABASE_COUNT = 16;
    private static final long CLEANUP_INTERVAL_SECONDS = 60;

    private final Storage[] databases;
    private final ScheduledExecutorService cleanupScheduler;
    private PersistenceManager persistenceManager;
    private MemoryTracker memoryTracker;

    public StorageManager() {
        this(new MemoryStorageFactory());
    }

    public StorageManager(StorageFactory storageFactory) {
        this.databases = new Storage[DEFAULT_DATABASE_COUNT];
        for (int i = 0; i < DEFAULT_DATABASE_COUNT; i++) {
            databases[i] = storageFactory.createStorage();
        }

        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "redis-storage-cleanup");
            t.setDaemon(true);
            return t;
        });
        startCleanupTask();
    }

    public Storage getStorage(int database) {
        if (database < 0 || database >= databases.length) {
            throw new IllegalArgumentException(
                    "Database number must be between 0 and " + (databases.length - 1));
        }
        return databases[database];
    }

    public Storage getDefaultStorage() { return getStorage(0); }

    public void clearDatabase(int database) { getStorage(database).clear(); }

    public void clearAll() {
        for (Storage s : databases) s.clear();
    }

    public int getDatabaseCount() { return databases.length; }

    /** Wire a memory tracker to every backing storage. */
    public void setMemoryTracker(MemoryTracker tracker) {
        this.memoryTracker = tracker;
        for (Storage s : databases) {
            if (s instanceof MemoryStorage) {
                ((MemoryStorage) s).setMemoryTracker(tracker);
            }
        }
    }

    public MemoryTracker getMemoryTracker() { return memoryTracker; }

    private void startCleanupTask() {
        cleanupScheduler.scheduleAtFixedRate(() -> {
            for (Storage s : databases) {
                if (s instanceof MemoryStorage) ((MemoryStorage) s).cleanupExpiredKeys();
            }
        }, CLEANUP_INTERVAL_SECONDS, CLEANUP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public void setPersistenceManager(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    public void shutdown() {
        if (persistenceManager != null) persistenceManager.saveData();
        if (cleanupScheduler != null && !cleanupScheduler.isShutdown()) {
            cleanupScheduler.shutdown();
            try {
                if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class MemoryStorageFactory implements StorageFactory {
        @Override
        public Storage createStorage() {
            return new MemoryStorage();
        }
    }
}