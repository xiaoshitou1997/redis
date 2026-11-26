package io.github.redis.command;

import io.github.redis.core.RedisClient;

/**
 * Redis-style command handler.
 *
 * <p>Each command is a function that receives a fully-populated {@link RedisClient}
 * (with argv / argc already set) and produces replies by calling
 * {@code c.replyXXX(...)} directly. No return value, no Object boxing, no schema.
 *
 * <p>This intentionally mirrors Redis's C function-pointer dispatch model:
 * <pre>
 *   void setCommand(client *c);
 * </pre>
 */
@FunctionalInterface
public interface CommandHandler {

    /**
     * Execute the command bound to the current client.
     *
     * @param c client context; argv[0] is the command name, argv[1..argc-1] are the arguments
     */
    void execute(RedisClient c);
}
