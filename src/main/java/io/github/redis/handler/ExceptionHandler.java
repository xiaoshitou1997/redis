package io.github.redis.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * 异常处理器
 * 捕获管道中的异常并返回错误响应给客户端
 */
@ChannelHandler.Sharable
public class ExceptionHandler extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(ExceptionHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception caught in pipeline", cause);
        String errorMessage = cause.getMessage();
        if (errorMessage == null || errorMessage.isEmpty()) {
            errorMessage = "Internal server error";
        }
        ctx.writeAndFlush(new ErrorRedisMessage("ERR " + errorMessage));
        // 不关闭连接，允许客户端继续发送命令
    }
}
