package io.github.redis.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-wide registry of all known {@link RedisCommand}s.
 *
 * <p>Lookup is by upper-case command name. This mirrors Redis's {@code redisCommandTable[]}
 * — a flat lookup table keyed by name, no schema framework, no parameter declarations.
 *
 * <p>For subcommand families (CLIENT, CONFIG, ACL, CLUSTER, SCRIPT, MODULE, COMMAND),
 * only the parent command is registered here; the handler dispatches internally on argv[1].
 */
public final class CommandTable {

    private final Map<String, RedisCommand> commands = new ConcurrentHashMap<>();

    /**
     * Register a fully-built command.
     */
    public void register(RedisCommand command) {
        if (command == null || command.getName() == null || command.getName().isEmpty()) {
            throw new IllegalArgumentException("command and its name must be non-empty");
        }
        commands.put(command.getName().toUpperCase(), command);
    }

    /**
     * Convenience for inline registration.
     */
    public void register(String name, int arity, Set<CommandFlag> flags,
                         List<KeySpec> keySpecs, CommandHandler handler) {
        register(new RedisCommand(name, arity, flags, keySpecs, handler));
    }

    /**
     * Look up by name. Returns null if unknown.
     */
    public RedisCommand get(String name) {
        if (name == null) {
            return null;
        }
        return commands.get(name.toUpperCase());
    }

    public boolean has(String name) {
        return name != null && commands.containsKey(name.toUpperCase());
    }

    public Set<String> names() {
        return new HashSet<>(commands.keySet());
    }

    public Collection<RedisCommand> all() {
        return Collections.unmodifiableCollection(commands.values());
    }

    /**
     * All commands whose handler runs atomically (Redis-style): currently every
     * registered command. Kept as a separate accessor to leave room for future
     * flag-based filtering (e.g. only WRITE commands).
     */
    public List<RedisCommand> commandsWithFlag(CommandFlag flag) {
        List<RedisCommand> out = new ArrayList<>();
        for (RedisCommand c : commands.values()) {
            if (c.hasFlag(flag)) {
                out.add(c);
            }
        }
        return out;
    }

    public int size() {
        return commands.size();
    }
}
