package io.github.redis.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.CharsetUtil;

/**
 * Phase-6 performance: pooled direct {@link ByteBuf}s for reply encoding.
 *
 * <p>Previously every reply allocated a fresh heap buffer via
 * {@code Unpooled.copiedBuffer(...)}, which put noticeable GC pressure under
 * load (~10M short-lived buffers per minute of benchmark). This class wraps
 * Netty's {@link PooledByteBufAllocator} so the dispatcher can write replies
 * into reusable direct buffers — Netty returns them to the pool automatically
 * once they're flushed.
 *
 * <p>Netty's pooled allocator is thread-local cached, so contention is
 * negligible in the single-threaded dispatcher model.
 */
public final class ReplyBufferPool {

    /** Shared pooled allocator — Netty already tunes it for the JVM. */
    public static final ByteBufAllocator DEFAULT = PooledByteBufAllocator.DEFAULT;

    private ReplyBufferPool() {}

    /** Allocate a direct buffer holding the given Latin-1 string. */
    public static ByteBuf directLatin1(String s) {
        if (s == null) return null;
        // Encode to a thread-local byte[] then copy into a pooled buffer.
        byte[] bytes = s.getBytes(CharsetUtil.ISO_8859_1);
        ByteBuf buf = DEFAULT.directBuffer(bytes.length, bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    /** Allocate a direct buffer holding the given raw bytes. */
    public static ByteBuf directBytes(byte[] bytes) {
        if (bytes == null) return null;
        ByteBuf buf = DEFAULT.directBuffer(bytes.length, bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    /**
     * Hex-encode an integer directly into a pooled buffer (used by SHA1, hash
     * slot, etc. — avoids intermediate String allocation).
     */
    public static ByteBuf directHexLong(long v) {
        // Max 16 hex chars for a long.
        ByteBuf buf = DEFAULT.directBuffer(16, 16);
        boolean started = false;
        for (int shift = 60; shift >= 0; shift -= 4) {
            int nibble = (int) ((v >>> shift) & 0xF);
            if (nibble == 0 && !started && shift > 0) continue;
            started = true;
            buf.writeByte(nibble < 10 ? '0' + nibble : 'a' + nibble - 10);
        }
        if (!started) buf.writeByte('0');
        return buf;
    }
}
