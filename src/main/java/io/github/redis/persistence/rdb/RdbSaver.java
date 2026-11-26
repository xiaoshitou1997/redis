package io.github.redis.persistence.rdb;

import io.github.redis.config.RedisConfig;
import io.github.redis.core.CommandDispatcher;
import io.github.redis.storage.Storage;
import io.github.redis.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RDB saver. Handles SAVE/BGSAVE and periodic background saves.
 *
 * <p><b>Snapshot consistency (Phase 1):</b> BGSAVE takes the snapshot on the
 * dispatcher thread (single-threaded, no concurrent mutations) and hands it
 * to a background saver thread that performs the (slow) disk I/O. This avoids
 * the original bug where BGSAVE iterated live mutating maps.
 *
 * <p>The atomic save uses the conventional Redis pattern: write to
 * {@code dump.rdb.tmp}, fsync, then atomically rename over {@code dump.rdb}.
 * On Linux {@code Files.move} with {@code ATOMIC_MOVE} is atomic; on Windows
 * it falls back to copy+delete which is not atomic but at least consistent
 * for crash recovery (the temp file is fully written before the move).
 */
public class RdbSaver {

    private static final Logger log = LoggerFactory.getLogger(RdbSaver.class);

    private final StorageManager storageManager;
    private final RedisConfig config;
    private final RdbEncoder encoder;
    private final RdbDecoder decoder;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean savingInProgress = new AtomicBoolean(false);
    private final AtomicLong lastSaveTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong changesSinceLastSave = new AtomicLong(0);

    /** Optional dispatcher used to take consistent snapshots on its thread. */
    private CommandDispatcher dispatcher;

    public RdbSaver(StorageManager storageManager, RedisConfig config) {
        this.storageManager = storageManager;
        this.config = config;
        this.encoder = new RdbEncoder();
        this.decoder = new RdbDecoder();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "redis-rdb-saver");
            t.setDaemon(true);
            return t;
        });
    }

    public void setDispatcher(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    /** Start the periodic save checker (every 1s). */
    public void startPeriodicSave() {
        scheduler.scheduleAtFixedRate(() -> {
            try { checkAndSave(); }
            catch (Exception e) {
                log.error("Error in periodic RDB save", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void checkAndSave() {
        List<RedisConfig.SaveRule> rules = config.getSaveRules();
        if (rules.isEmpty()) return;

        long elapsedSeconds = (System.currentTimeMillis() - lastSaveTime.get()) / 1000;
        long changes = changesSinceLastSave.get();
        for (RedisConfig.SaveRule rule : rules) {
            if (elapsedSeconds >= rule.getSeconds() && changes >= rule.getChanges()) {
                bgsave();
                break;
            }
        }
    }

    /**
     * Synchronous save. Caller MUST already be on the dispatcher thread (so the
     * snapshot is consistent with command execution).
     */
    public boolean save() {
        if (!savingInProgress.compareAndSet(false, true)) return false;
        try {
            return doSaveWithSnapshot(snapshotNow());
        } finally {
            savingInProgress.set(false);
        }
    }

    /**
     * Background save. Snapshot is taken on the dispatcher thread; disk I/O runs
     * on the saver scheduler.
     */
    public boolean bgsave() {
        if (!savingInProgress.compareAndSet(false, true)) return false;

        if (dispatcher == null) {
            // Fall back to in-thread snapshot + background write.
            final StorageSnapshot snap = snapshotNow();
            scheduler.execute(() -> {
                try { doSaveWithSnapshot(snap); }
                catch (Exception e) { log.error("Background RDB save failed", e); }
                finally { savingInProgress.set(false); }
            });
            return true;
        }

        // Take the snapshot on the dispatcher thread so we see a consistent
        // view; then hand it to the saver scheduler for I/O.
        final StorageSnapshot[] box = new StorageSnapshot[1];
        dispatcher.submitAndWait(() -> box[0] = snapshotNow());
        final StorageSnapshot snap = box[0];

        scheduler.execute(() -> {
            try { doSaveWithSnapshot(snap); }
            catch (Exception e) { log.error("Background RDB save failed", e); }
            finally { savingInProgress.set(false); }
        });
        return true;
    }

    private StorageSnapshot snapshotNow() {
        return new StorageSnapshot(getAllStorages());
    }

    private boolean doSaveWithSnapshot(StorageSnapshot snap) {
        try {
            Path dirPath = Paths.get(config.getDir());
            if (!Files.exists(dirPath)) Files.createDirectories(dirPath);

            Path filePath = dirPath.resolve(config.getDbfilename());
            Path tempPath = dirPath.resolve(config.getDbfilename() + ".tmp");

            try (FileOutputStream fos = new FileOutputStream(tempPath.toFile())) {
                encoder.encode(fos, snap);
                fos.flush();
                fos.getFD().sync();
            }

            // Atomic rename — works on Linux/macOS, copy-on-write on Windows.
            try {
                Files.move(tempPath, filePath, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.move(tempPath, filePath, StandardCopyOption.REPLACE_EXISTING);
            }

            lastSaveTime.set(System.currentTimeMillis());
            changesSinceLastSave.set(0);
            log.info("RDB saved successfully to: {}", filePath);
            return true;
        } catch (IOException e) {
            log.error("Failed to save RDB", e);
            return false;
        }
    }

    public boolean load() {
        try {
            Path filePath = Paths.get(config.getDir(), config.getDbfilename());
            if (!Files.exists(filePath)) {
                log.info("RDB file not found: {}", filePath);
                return false;
            }
            try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
                decoder.decode(fis, getAllStorages());
            }
            log.info("RDB loaded successfully from: {}", filePath);
            return true;
        } catch (IOException e) {
            log.error("Failed to load RDB", e);
            return false;
        }
    }

    public void recordChange() {
        changesSinceLastSave.incrementAndGet();
    }

    public long getChangesSinceLastSave() {
        return changesSinceLastSave.get();
    }

    public long getLastSaveTime() {
        return lastSaveTime.get();
    }

    private Storage[] getAllStorages() {
        int dbCount = storageManager.getDatabaseCount();
        Storage[] storages = new Storage[dbCount];
        for (int i = 0; i < dbCount; i++) storages[i] = storageManager.getStorage(i);
        return storages;
    }

    public boolean isSaving() { return savingInProgress.get(); }

    public void shutdown() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
