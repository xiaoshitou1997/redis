package io.github.redis.persistence.aof;

import io.github.redis.command.RedisCommand;
import io.github.redis.command.CommandTable;
import io.github.redis.core.RedisClient;
import io.github.redis.storage.StorageManager;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.AttributeKey;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * AOF reader / replayer.
 *
 * <p>Phase-1 redesign: replays the AOF by directly dispatching each parsed
 * command through the {@link CommandTable}, bypassing the Netty pipeline. We
 * use a synthetic {@link EmbeddedChannel} so the existing {@link RedisClient}
 * initialisation (which looks up a {@code ServiceRegistry} attribute on the
 * channel) works unchanged.
 *
 * <p>The reader is deliberately permissive: malformed lines or unknown
 * commands are logged and skipped (Redis's behaviour is to abort on
 * truncated AOF, but for Phase 1 we prioritise getting the server bootable).
 */
public class AofReader {

    private final CommandTable commandTable;
    private final StorageManager storageManager;
    private final io.github.redis.core.ServiceRegistry serviceRegistry;

    public AofReader(CommandTable commandTable, StorageManager storageManager,
                     io.github.redis.core.ServiceRegistry serviceRegistry) {
        this.commandTable = commandTable;
        this.storageManager = storageManager;
        this.serviceRegistry = serviceRegistry;
    }

    /**
     * Replay the AOF file referenced by {@code config}. Returns true if any
     * commands were applied.
     */
    public boolean replay(io.github.redis.config.RedisConfig config) {
        if (!config.isAppendOnly()) return false;

        Path filePath = Paths.get(config.getDir(), config.getAppendFilename());
        if (!Files.exists(filePath)) {
            System.out.println("AOF file not found: " + filePath);
            return false;
        }

        // Synthetic channel carries the service registry so RedisClient can be built.
        EmbeddedChannel channel = new EmbeddedChannel();
        AttributeKey<io.github.redis.core.ServiceRegistry> srKey =
                AttributeKey.valueOf("serviceRegistry");
        channel.attr(srKey).set(serviceRegistry);

        // Construct a replay client tied to the synthetic channel's pipeline head.
        io.netty.channel.ChannelHandlerContext ctx = channel.pipeline().firstContext();
        if (ctx == null) {
            // Pipeline is empty (no handlers attached) — register a placeholder
            // inbound handler so a context exists.
            channel.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter());
            ctx = channel.pipeline().firstContext();
        }
        RedisClient client = new RedisClient(ctx, serviceRegistry);

        long total = 0;
        long applied = 0;
        long skipped = 0;

        try (InputStream raw = new BufferedInputStream(Files.newInputStream(filePath))) {
            PushbackInputStream in = new PushbackInputStream(raw, 1);
            while (true) {
                int b = in.read();
                if (b == -1) break;
                if (b == '*') {
                    List<String> tokens = readArray(in);
                    if (tokens == null || tokens.isEmpty()) continue;
                    total++;
                    if (apply(client, tokens)) applied++;
                    else skipped++;
                } else if (b == '\r' || b == '\n' || b == ' ' || b == '\t') {
                    continue;
                } else {
                    drainLine(in);
                }
            }
        } catch (IOException e) {
            System.err.println("AOF replay I/O error: " + e.getMessage());
        }

        System.out.println("AOF replay: " + applied + "/" + total + " commands applied ("
                + skipped + " skipped) from " + filePath);
        return applied > 0;
    }

    /** Parse a RESP array starting after the leading '*'. */
    private List<String> readArray(PushbackInputStream in) throws IOException {
        int count = readIntLine(in);
        if (count < 0) return null;
        List<String> tokens = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int b = in.read();
            if (b != '$') {
                drainLine(in);
                return null;
            }
            int len = readIntLine(in);
            if (len < 0) {
                tokens.add(null);
                continue;
            }
            byte[] buf = new byte[len];
            int off = 0;
            while (off < len) {
                int n = in.read(buf, off, len - off);
                if (n == -1) throw new IOException("truncated bulk string");
                off += n;
            }
            // Consume trailing CRLF.
            in.read(); in.read();
            tokens.add(new String(buf, StandardCharsets.UTF_8));
        }
        return tokens;
    }

    private int readIntLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') break;
                sb.append((char) b).append((char) next);
            } else {
                sb.append((char) b);
            }
        }
        try { return Integer.parseInt(sb.toString().trim()); }
        catch (NumberFormatException e) { return -1; }
    }

    private void drainLine(InputStream in) throws IOException {
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') return;
        }
    }

    /** Dispatch one tokenised command. Returns true if a known command ran. */
    private boolean apply(RedisClient client, List<String> tokens) {
        String name = tokens.get(0).toUpperCase();
        RedisCommand cmd = commandTable.get(name);
        if (cmd == null) {
            System.err.println("AOF replay: unknown command '" + name + "' — skipped");
            return false;
        }
        String[] argv = tokens.toArray(new String[0]);
        if (!cmd.checkArity(argv.length)) {
            System.err.println("AOF replay: arity mismatch for '" + name + "' — skipped");
            return false;
        }
        client.setCommand(argv, argv.length);
        try {
            cmd.getHandler().execute(client);
            return true;
        } catch (RuntimeException e) {
            System.err.println("AOF replay: command '" + name + "' failed: " + e.getMessage());
            return false;
        }
    }

    /** Tiny pushback wrapper to peek one byte ahead. */
    private static final class PushbackInputStream extends java.io.PushbackInputStream {
        PushbackInputStream(InputStream in, int size) { super(in, size); }
    }
}
