package io.github.redis.pubsub;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PubSubManager {

    private final Map<String, Set<String>> channelSubscribers = new ConcurrentHashMap<>();
    private final Map<String, ChannelHandlerContext> clientContexts = new ConcurrentHashMap<>();

    public void subscribe(String channel, String clientId, ChannelHandlerContext ctx) {
        channelSubscribers.computeIfAbsent(channel, k -> ConcurrentHashMap.newKeySet()).add(clientId);
        clientContexts.put(clientId, ctx);

        sendSubscribeResponse(ctx, channel);
    }

    public boolean unsubscribe(String channel, String clientId) {
        Set<String> subscribers = channelSubscribers.get(channel);
        if (subscribers != null) {
            subscribers.remove(clientId);
            if (subscribers.isEmpty()) {
                channelSubscribers.remove(channel);
            }

            ChannelHandlerContext ctx = clientContexts.get(clientId);
            if (ctx != null) {
                sendUnsubscribeResponse(ctx, channel);
            }
            return true;
        }
        return false;
    }

    public List<String> unsubscribeAll(String clientId) {
        List<String> unsubscribedChannels = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : channelSubscribers.entrySet()) {
            String channel = entry.getKey();
            Set<String> subscribers = entry.getValue();

            if (subscribers.remove(clientId)) {
                unsubscribedChannels.add(channel);
                if (subscribers.isEmpty()) {
                    channelSubscribers.remove(channel);
                }
            }
        }

        ChannelHandlerContext ctx = clientContexts.remove(clientId);
        if (ctx != null) {
            for (String channel : unsubscribedChannels) {
                sendUnsubscribeResponse(ctx, channel);
            }
        }

        return unsubscribedChannels;
    }

    public long publish(String channel, String message) {
        Set<String> subscribers = channelSubscribers.get(channel);
        if (subscribers == null || subscribers.isEmpty()) {
            return 0L;
        }

        long count = 0;
        for (String clientId : subscribers) {
            ChannelHandlerContext ctx = clientContexts.get(clientId);
            if (ctx != null && ctx.channel().isActive()) {
                sendPublishMessage(ctx, channel, message);
                count++;
            }
        }

        return count;
    }

    public void removeClient(String clientId) {
        unsubscribeAll(clientId);
    }

    private void sendSubscribeResponse(ChannelHandlerContext ctx, String channel) {
        List<RedisMessage> messages = new ArrayList<>();
        messages.add(FullBulkStringRedisMessage.NULL_INSTANCE);
        messages.add(createBulkString("subscribe"));
        messages.add(createBulkString(channel));
        messages.add(createBulkString(String.valueOf(getSubscriptionCount(ctx))));

        sendArrayMessage(ctx, messages);
    }

    private void sendUnsubscribeResponse(ChannelHandlerContext ctx, String channel) {
        List<RedisMessage> messages = new ArrayList<>();
        messages.add(FullBulkStringRedisMessage.NULL_INSTANCE);
        messages.add(createBulkString("unsubscribe"));
        messages.add(createBulkString(channel));
        messages.add(createBulkString(String.valueOf(getSubscriptionCount(ctx))));

        sendArrayMessage(ctx, messages);
    }

    private void sendPublishMessage(ChannelHandlerContext ctx, String channel, String message) {
        List<RedisMessage> messages = new ArrayList<>();
        messages.add(FullBulkStringRedisMessage.NULL_INSTANCE);
        messages.add(createBulkString("message"));
        messages.add(createBulkString(channel));
        messages.add(createBulkString(message));

        sendArrayMessage(ctx, messages);
    }

    private void sendArrayMessage(ChannelHandlerContext ctx, List<RedisMessage> messages) {
        ctx.writeAndFlush(new ArrayRedisMessage(messages));
    }

    private RedisMessage createBulkString(String content) {
        ByteBuf buffer = Unpooled.copiedBuffer(content, CharsetUtil.UTF_8);
        return new FullBulkStringRedisMessage(buffer);
    }

    private int getSubscriptionCount(ChannelHandlerContext ctx) {
        String clientId = ctx.channel().id().asLongText();
        int count = 0;
        for (Set<String> subscribers : channelSubscribers.values()) {
            if (subscribers.contains(clientId)) {
                count++;
            }
        }
        return count;
    }
}
