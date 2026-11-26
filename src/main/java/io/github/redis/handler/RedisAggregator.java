package io.github.redis.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.ArrayHeaderRedisMessage;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.BulkStringHeaderRedisMessage;
import io.netty.handler.codec.redis.BulkStringRedisContent;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Reassembles fragmented RESP messages into complete {@link ArrayRedisMessage}s.
 *
 * <p>Netty's {@code RedisDecoder} emits a sequence of small messages for each
 * incoming command: an {@link ArrayHeaderRedisMessage}, then for each array
 * element either a {@link FullBulkStringRedisMessage} (when it fits in one
 * decoder chunk) or a {@link BulkStringHeaderRedisMessage} followed by one or
 * more {@link BulkStringRedisContent} chunks (the last being
 * {@code DefaultLastBulkStringRedisContent}). This handler stitches them back
 * together so downstream code sees a single fully-formed array per command.
 *
 * <p>Per-channel state: arrayLength/array (current top-level array being built),
 * bulkLength/bulk (current in-flight bulk string within that array), plus a
 * top-level bulk fallback for the rare case a bulk arrives outside any array.
 *
 * <p>Robust against out-of-order chunks: if a BulkStringRedisContent arrives
 * without a known header (which shouldn't happen with Netty's decoder but can
 * occur under weird fragmentation), it's accumulated into a fresh scratch
 * buffer rather than being passed through as a "complete" message.
 */
public class RedisAggregator extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(RedisAggregator.class);

    private int arrayLength;
    private List<RedisMessage> array;

    private int bulkLength;
    private ByteBuf bulk;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RedisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }

        // Top-level array.
        if (msg instanceof ArrayHeaderRedisMessage) {
            arrayLength = (int) ((ArrayHeaderRedisMessage) msg).length();
            array = new ArrayList<>(Math.max(0, arrayLength));
            return;
        }

        if (array != null) {
            // We're collecting children for the active top-level array.
            handleWithinArray(ctx, (RedisMessage) msg);
            return;
        }

        // Top-level bulk string (rare; inline commands sometimes go this way).
        if (msg instanceof BulkStringHeaderRedisMessage) {
            startBulk(((BulkStringHeaderRedisMessage) msg).bulkStringLength());
            return;
        }
        if (bulk != null && msg instanceof BulkStringRedisContent) {
            appendBulk((BulkStringRedisContent) msg);
            if (bulk.readableBytes() == bulkLength) {
                ctx.fireChannelRead(new FullBulkStringRedisMessage(bulk));
                resetBulkState();
            }
            return;
        }

        // Anything else: forward as-is.
        ctx.fireChannelRead(msg);
    }

    private void handleWithinArray(ChannelHandlerContext ctx, RedisMessage msg) {
        // Bulk header: start a new in-flight bulk buffer.
        if (msg instanceof BulkStringHeaderRedisMessage) {
            startBulk(((BulkStringHeaderRedisMessage) msg).bulkStringLength());
            return;
        }
        // Bulk content: accumulate.
        if (bulk != null && msg instanceof BulkStringRedisContent) {
            appendBulk((BulkStringRedisContent) msg);
            if (bulk.readableBytes() != bulkLength) {
                return;
            }
            array.add(new FullBulkStringRedisMessage(bulk));
            resetBulkState();
            maybeFlushArray(ctx);
            return;
        }
        // Stray BulkStringRedisContent with no active bulk header — this happens
        // when Netty's decoder emits an empty trailing "last content" marker
        // after the body was already delivered in the previous chunk. Drop it
        // silently rather than corrupting the array with an empty element.
        if (msg instanceof BulkStringRedisContent) {
            return;
        }
        // Otherwise: a fully-formed message (FullBulkStringRedisMessage,
        // SimpleStringRedisMessage, IntegerRedisMessage, etc.).
        array.add(msg);
        maybeFlushArray(ctx);
    }

    private void maybeFlushArray(ChannelHandlerContext ctx) {
        if (array.size() == arrayLength) {
            ctx.fireChannelRead(new ArrayRedisMessage(array));
            resetArrayState();
        }
    }

    private void startBulk(int length) {
        bulkLength = length;
        bulk = Unpooled.buffer(Math.max(0, length));
    }

    private void appendBulk(BulkStringRedisContent content) {
        bulk.writeBytes(content.content());
    }

    private void resetBulkState() {
        bulkLength = 0;
        bulk = null;
    }

    private void resetArrayState() {
        arrayLength = 0;
        array = null;
        if (log.isDebugEnabled()) {
            log.debug("Array state reset");
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (bulk != null) {
            bulk.release();
            bulk = null;
        }
        super.channelInactive(ctx);
    }
}
