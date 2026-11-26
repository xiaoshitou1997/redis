package io.github.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.Map;

/**
 * Phase-7 RESP3 wire-format encoder.
 *
 * <p>Inserted into the Netty pipeline for clients that negotiated protocol
 * version 3 via HELLO. Emits the RESP3 type bytes for new reply shapes that
 * Netty's built-in {@code RedisEncoder} can't produce:
 *
 * <ul>
 *   <li>{@code %<n>\r\n...} — map</li>
 *   <li>{@code ~<n>\r\n...} — set</li>
 *   <li>{@code #t\r\n} / {@code #f\r\n} — boolean</li>
 *   <li>{@code (<digits>\r\n} — big number</li>
 *   <li>{@code =<len>\r\n<3-char-format>:<payload>\r\n} — verbatim string</li>
 *   <li>{@code _\r\n} — null (RESP3 uses underscore, not {@code $-1})</li>
 * </ul>
 *
 * <p>For RESP2 replies (simple strings, integers, errors, bulk strings,
 * arrays), this encoder falls through to the standard encoding so the
 * existing reply paths keep working unchanged.
 *
 * <p>Wire format reference: https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md
 */
@ChannelHandler.Sharable
public class Resp3Encoder extends ChannelOutboundHandlerAdapter {

    /** Marker object: a RESP3 map. Put one into the pipeline to emit a map reply. */
    public static final class MapReply {
        public final java.util.Map<?, ?> entries;
        public MapReply(java.util.Map<?, ?> entries) { this.entries = entries; }
    }

    /** Marker object: a RESP3 set (semantically like an array but distinct type). */
    public static final class SetReply {
        public final List<?> elements;
        public SetReply(List<?> elements) { this.elements = elements; }
    }

    /** Marker object: a RESP3 boolean. */
    public static final class BooleanReply {
        public final boolean value;
        public BooleanReply(boolean v) { this.value = v; }
    }

    /** Marker object: a RESP3 big number. */
    public static final class BigNumberReply {
        public final String decimal;
        public BigNumberReply(String d) { this.decimal = d; }
    }

    /** Marker object: a RESP3 verbatim string. */
    public static final class VerbatimReply {
        public final String format; // exactly 3 chars, e.g. "txt"
        public final String text;
        public VerbatimReply(String format, String text) { this.format = format; this.text = text; }
    }

    /** Marker object: a RESP3 null (uses {@code _\r\n}). */
    public static final class NullReply {}

    private static final byte[] CRLF = new byte[] { '\r', '\n' };

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        try {
            ByteBuf out = encode(ctx.alloc(), msg);
            if (out != null) {
                ctx.write(out, promise);
            } else {
                // Unknown type — fall through to the next handler (e.g. Netty's
                // RedisEncoder) so legacy {@link io.netty.handler.codec.redis.RedisMessage}
                // types continue to work.
                ctx.write(msg, promise);
            }
        } catch (Throwable t) {
            promise.setFailure(t);
        }
    }

    /** Encode one reply object into a buffer. Returns null if the message is a
     *  legacy Netty RedisMessage that should be handled downstream. */
    private ByteBuf encode(ByteBufAllocator alloc, Object msg) {
        if (msg instanceof MapReply) {
            return encodeMap(alloc, ((MapReply) msg).entries);
        }
        if (msg instanceof SetReply) {
            return encodeSet(alloc, ((SetReply) msg).elements);
        }
        if (msg instanceof BooleanReply) {
            ByteBuf buf = alloc.buffer(4, 4);
            buf.writeByte('#');
            buf.writeByte(((BooleanReply) msg).value ? 't' : 'f');
            buf.writeBytes(CRLF);
            return buf;
        }
        if (msg instanceof BigNumberReply) {
            String d = ((BigNumberReply) msg).decimal;
            byte[] bytes = d.getBytes(CharsetUtil.US_ASCII);
            ByteBuf buf = alloc.buffer(1 + bytes.length + 2);
            buf.writeByte('(');
            buf.writeBytes(bytes);
            buf.writeBytes(CRLF);
            return buf;
        }
        if (msg instanceof VerbatimReply) {
            VerbatimReply v = (VerbatimReply) msg;
            byte[] textBytes = v.text.getBytes(CharsetUtil.ISO_8859_1);
            int totalLen = 3 + 1 + textBytes.length; // format + ":" + payload
            byte[] lenAscii = Integer.toString(totalLen).getBytes(CharsetUtil.US_ASCII);
            ByteBuf buf = alloc.buffer(1 + lenAscii.length + 2 + totalLen + 2);
            buf.writeByte('=');
            buf.writeBytes(lenAscii);
            buf.writeBytes(CRLF);
            buf.writeBytes(v.format.getBytes(CharsetUtil.US_ASCII));
            buf.writeByte(':');
            buf.writeBytes(textBytes);
            buf.writeBytes(CRLF);
            return buf;
        }
        if (msg instanceof NullReply) {
            ByteBuf buf = alloc.buffer(3, 3);
            buf.writeByte('_');
            buf.writeBytes(CRLF);
            return buf;
        }
        return null;
    }

    private ByteBuf encodeMap(ByteBufAllocator alloc, Map<?, ?> entries) {
        // We need the size first, then alternating keys/values as RESP-encoded values.
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try {
            bos.write('%');
            bos.write(Integer.toString(entries.size()).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            for (Map.Entry<?, ?> e : entries.entrySet()) {
                writeValue(bos, e.getKey());
                writeValue(bos, e.getValue());
            }
            return alloc.buffer(bos.size(), bos.size()).writeBytes(bos.toByteArray());
        } catch (java.io.IOException never) {
            throw new RuntimeException(never);
        }
    }

    private ByteBuf encodeSet(ByteBufAllocator alloc, List<?> elements) {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try {
            bos.write('~');
            bos.write(Integer.toString(elements.size()).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            for (Object o : elements) writeValue(bos, o);
            return alloc.buffer(bos.size(), bos.size()).writeBytes(bos.toByteArray());
        } catch (java.io.IOException never) {
            throw new RuntimeException(never);
        }
    }

    /** Write a single value as a RESP2/3 bulk string / integer / nested map / etc. */
    @SuppressWarnings("unchecked")
    private void writeValue(java.io.ByteArrayOutputStream bos, Object o) throws java.io.IOException {
        if (o == null) {
            bos.write('_'); bos.write('\r'); bos.write('\n');
        } else if (o instanceof Boolean) {
            bos.write('#'); bos.write(((Boolean) o) ? 't' : 'f'); bos.write('\r'); bos.write('\n');
        } else if (o instanceof Number) {
            long n = ((Number) o).longValue();
            bos.write(':');
            bos.write(Long.toString(n).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
        } else if (o instanceof String) {
            byte[] b = ((String) o).getBytes(CharsetUtil.ISO_8859_1);
            bos.write('$');
            bos.write(Integer.toString(b.length).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            bos.write(b);
            bos.write('\r'); bos.write('\n');
        } else if (o instanceof byte[]) {
            byte[] b = (byte[]) o;
            bos.write('$');
            bos.write(Integer.toString(b.length).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            bos.write(b);
            bos.write('\r'); bos.write('\n');
        } else if (o instanceof Map) {
            // Inline map.
            Map<?, ?> m = (Map<?, ?>) o;
            bos.write('%');
            bos.write(Integer.toString(m.size()).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            for (Map.Entry<?, ?> e : m.entrySet()) {
                writeValue(bos, e.getKey());
                writeValue(bos, e.getValue());
            }
        } else if (o instanceof List) {
            List<?> l = (List<?>) o;
            bos.write('~');
            bos.write(Integer.toString(l.size()).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            for (Object x : l) writeValue(bos, x);
        } else {
            byte[] b = String.valueOf(o).getBytes(CharsetUtil.ISO_8859_1);
            bos.write('$');
            bos.write(Integer.toString(b.length).getBytes(CharsetUtil.US_ASCII));
            bos.write('\r'); bos.write('\n');
            bos.write(b);
            bos.write('\r'); bos.write('\n');
        }
    }
}
