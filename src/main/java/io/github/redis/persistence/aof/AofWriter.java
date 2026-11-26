package io.github.redis.persistence.aof;

import io.github.redis.config.RedisConfig;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
 * AOF writer. Appends RESP-encoded commands to {@code appendonly.aof} with
 * one of three fsync strategies: {@code always}, {@code everysec}, {@code no}.
 *
 * <p>Phase-1 features added:
 * <ul>
 *   <li>Tracks {@link #currentSizeBytes} so the auto-rewrite trigger can decide
 *       when to fire (percent-growth based).</li>
 *   <li>{@link #rewriteFrom(java.io.InputStream)} atomically replaces the AOF
 *       with a rewritten stream produced by {@link AofRewriter}.</li>
 *   <li>Public {@link #getCurrentSizeBytes()} for monitoring.</li>
 * </ul>
 */
public class AofWriter {

    private final RedisConfig config;
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private FileOutputStream fileOutputStream;
    private BufferedWriter bufferedWriter;
    private final Object writeLock = new Object();
    private ScheduledExecutorService fsyncScheduler;
    private final AtomicBoolean pendingFsync = new AtomicBoolean(false);
    private final AtomicLong currentSizeBytes = new AtomicLong(0);
    private final AtomicLong rewriteBaseSize = new AtomicLong(0);

    public AofWriter(RedisConfig config) {
        this.config = config;
    }

    public boolean initialize() {
        if (!config.isAppendOnly()) return false;

        try {
            Path dirPath = Paths.get(config.getDir());
            if (!Files.exists(dirPath)) Files.createDirectories(dirPath);

            Path filePath = dirPath.resolve(config.getAppendFilename());
            fileOutputStream = new FileOutputStream(filePath.toFile(), true);
            bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8), 8192);

            // Track current file size for auto-rewrite decisions.
            long existing = Files.exists(filePath) ? Files.size(filePath) : 0;
            currentSizeBytes.set(existing);
            rewriteBaseSize.set(existing);

            enabled.set(true);

            String fsyncStrategy = config.getAppendFsync();
            if ("everysec".equals(fsyncStrategy)) startPeriodicFsync();

            System.out.println("AOF writer initialized: " + filePath + " (size=" + existing + ")");
            return true;
        } catch (IOException e) {
            System.err.println("Failed to initialize AOF writer: " + e.getMessage());
            e.printStackTrace();
            enabled.set(false);
            return false;
        }
    }

    public void writeCommand(List<String> commandTokens) {
        if (!enabled.get() || bufferedWriter == null) return;
        synchronized (writeLock) {
            try {
                String resp = convertToResp(commandTokens);
                bufferedWriter.write(resp);
                bufferedWriter.flush();
                currentSizeBytes.addAndGet(resp.getBytes(StandardCharsets.UTF_8).length);

                String fsyncStrategy = config.getAppendFsync();
                if ("always".equals(fsyncStrategy)) {
                    fileOutputStream.getFD().sync();
                } else if ("everysec".equals(fsyncStrategy)) {
                    pendingFsync.set(true);
                }
            } catch (IOException e) {
                System.err.println("Failed to write command to AOF: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private String convertToResp(List<String> tokens) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(tokens.size()).append("\r\n");
        for (String token : tokens) {
            byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
            sb.append("$").append(bytes.length).append("\r\n").append(token).append("\r\n");
        }
        return sb.toString();
    }

    private void startPeriodicFsync() {
        fsyncScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "redis-aof-fsync");
            t.setDaemon(true);
            return t;
        });
        fsyncScheduler.scheduleAtFixedRate(() -> {
            if (pendingFsync.compareAndSet(true, false)) {
                try {
                    synchronized (writeLock) {
                        if (fileOutputStream != null) fileOutputStream.getFD().sync();
                    }
                } catch (IOException e) {
                    System.err.println("Failed to fsync AOF file: " + e.getMessage());
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void forceSync() {
        if (!enabled.get() || fileOutputStream == null) return;
        synchronized (writeLock) {
            try {
                bufferedWriter.flush();
                fileOutputStream.getFD().sync();
                pendingFsync.set(false);
            } catch (IOException e) {
                System.err.println("Failed to force sync AOF: " + e.getMessage());
            }
        }
    }

    /**
     * Replace the AOF file with the contents of {@code rewrittenStream}. The
     * caller is responsible for producing a valid AOF (typically via
     * {@link AofRewriter}).
     *
     * <p>Sequence: close current writer, atomically swap files, reopen.
     */
    public void rewriteFrom(InputStream rewrittenStream) throws IOException {
        if (!enabled.get()) {
            // Even if disabled at runtime, allow the rewrite to be flushed to disk.
        }
        synchronized (writeLock) {
            // Close current writer cleanly.
            try {
                if (bufferedWriter != null) bufferedWriter.flush();
                if (fileOutputStream != null) {
                    fileOutputStream.getFD().sync();
                    fileOutputStream.close();
                }
            } catch (IOException ignored) { }

            Path dirPath = Paths.get(config.getDir());
            Path filePath = dirPath.resolve(config.getAppendFilename());
            Path tempPath = dirPath.resolve(config.getAppendFilename() + ".tmp");

            // Copy rewritten bytes into the temp file and fsync.
            try (FileOutputStream tmp = new FileOutputStream(tempPath.toFile())) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = rewrittenStream.read(buf)) != -1) tmp.write(buf, 0, n);
                tmp.flush();
                tmp.getFD().sync();
            }
            try {
                Files.move(tempPath, filePath, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.move(tempPath, filePath, StandardCopyOption.REPLACE_EXISTING);
            }

            // Reopen for append.
            fileOutputStream = new FileOutputStream(filePath.toFile(), true);
            bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8), 8192);
            long newSize = Files.size(filePath);
            currentSizeBytes.set(newSize);
            rewriteBaseSize.set(newSize);
            System.out.println("AOF rewrite complete; new size = " + newSize + " bytes");
        }
    }

    public long getCurrentSizeBytes() { return currentSizeBytes.get(); }
    public long getRewriteBaseSize() { return rewriteBaseSize.get(); }

    /**
     * Auto-rewrite trigger check. Returns true if growth exceeds the configured
     * percentage and minimum-size thresholds (Redis defaults: 100% and 64MB).
     */
    public boolean shouldAutoRewrite() {
        long base = rewriteBaseSize.get();
        long current = currentSizeBytes.get();
        if (base == 0) return current >= 64L * 1024 * 1024;
        double growthPct = ((double) (current - base) / base) * 100.0;
        return growthPct >= 100.0 && current >= 64L * 1024 * 1024;
    }

    public void shutdown() {
        enabled.set(false);
        synchronized (writeLock) {
            try {
                if (bufferedWriter != null) bufferedWriter.flush();
                if (fileOutputStream != null) {
                    fileOutputStream.getFD().sync();
                    fileOutputStream.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing AOF writer: " + e.getMessage());
            }
        }
        if (fsyncScheduler != null && !fsyncScheduler.isShutdown()) {
            fsyncScheduler.shutdown();
            try {
                if (!fsyncScheduler.awaitTermination(2, TimeUnit.SECONDS)) fsyncScheduler.shutdownNow();
            } catch (InterruptedException e) {
                fsyncScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isEnabled() { return enabled.get(); }
}
