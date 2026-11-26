package io.github.redis.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Immutable Redis command definition.
 *
 * <p>Concrete counterpart of Redis's C {@code struct redisCommand}. Carries everything
 * the dispatcher needs to validate and route a command:
 * <ul>
 *   <li>{@code name}        — upper-case command name</li>
 *   <li>{@code arity}       — exact argc if positive; minimum argc (incl. name) if negative</li>
 *   <li>{@code flags}       — WRITE / READONLY / ADMIN / FAST / ...</li>
 *   <li>{@code keySpecs}    — declarative key extraction for ACL/cluster/stats</li>
 *   <li>{@code handler}     — function that runs the command</li>
 * </ul>
 *
 * <p>Intentionally no subcommand map, no metadata, no schema framework. Subcommands
 * are dispatched by the parent handler on argv[1].
 */
public final class RedisCommand {

    private final String name;
    private final int arity;
    private final Set<CommandFlag> flags;
    private final List<KeySpec> keySpecs;
    private final CommandHandler handler;

    public RedisCommand(String name, int arity, Set<CommandFlag> flags,
                        List<KeySpec> keySpecs, CommandHandler handler) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("command name required");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler required for command: " + name);
        }
        this.name = name.toUpperCase();
        this.arity = arity;
        this.flags = (flags == null || flags.isEmpty())
                ? Collections.<CommandFlag>emptySet()
                : Collections.unmodifiableSet(EnumSet.copyOf(flags));
        this.keySpecs = (keySpecs == null || keySpecs.isEmpty())
                ? Collections.<KeySpec>emptyList()
                : Collections.unmodifiableList(new ArrayList<>(keySpecs));
        this.handler = handler;
    }

    public String getName() {
        return name;
    }

    public int getArity() {
        return arity;
    }

    public Set<CommandFlag> getFlags() {
        return flags;
    }

    public List<KeySpec> getKeySpecs() {
        return keySpecs;
    }

    public CommandHandler getHandler() {
        return handler;
    }

    public boolean hasFlag(CommandFlag flag) {
        return flags.contains(flag);
    }

    /** Short-circuit arity check matching Redis's getKeysFromCommand semantics. */
    public boolean checkArity(int argc) {
        if (arity > 0) {
            return argc == arity;
        }
        return argc >= -arity;
    }

    /**
     * Extract every key touched by this command, given current argv.
     * Used for ACL checks, cluster slot routing, slowlog, etc.
     */
    public List<String> extractKeys(String[] argv, int argc) {
        if (keySpecs.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<>();
        for (KeySpec spec : keySpecs) {
            spec.appendKeys(argv, argc, keys);
        }
        return keys;
    }

    @Override
    public String toString() {
        return "RedisCommand{" + name + ", arity=" + arity + ", flags=" + flags + "}";
    }
}
