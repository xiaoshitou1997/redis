package io.github.redis.persistence;

import io.github.redis.command.CommandFlag;
import io.github.redis.command.CommandTable;
import io.github.redis.command.RedisCommand;
import io.github.redis.persistence.aof.AofWriter;
import io.github.redis.persistence.rdb.RdbSaver;

import java.util.List;

/**
 * Persistence listener.
 *
 * <p>Notifies AOF writer and RDB saver whenever a write command succeeds. Whether
 * a command is a write is determined by looking it up in the {@link CommandTable}
 * and checking the {@link CommandFlag#WRITE} flag — this fixes the original bug
 * where the switch statement missed every list/set/hash/zset mutation command.
 */
public class PersistenceListener {

    private final AofWriter aofWriter;
    private final RdbSaver rdbSaver;
    private final CommandTable commandTable;

    public PersistenceListener(AofWriter aofWriter, RdbSaver rdbSaver, CommandTable commandTable) {
        this.aofWriter = aofWriter;
        this.rdbSaver = rdbSaver;
        this.commandTable = commandTable;
    }

    /**
     * Record a successful write command. Called by the dispatcher AFTER the handler
     * has executed without throwing.
     */
    public void onWriteCommand(List<String> commandTokens) {
        if (commandTokens == null || commandTokens.isEmpty()) return;
        String name = commandTokens.get(0).toUpperCase();
        RedisCommand cmd = commandTable == null ? null : commandTable.get(name);
        if (cmd == null || !cmd.hasFlag(CommandFlag.WRITE)) return;

        if (aofWriter != null && aofWriter.isEnabled()) {
            aofWriter.writeCommand(commandTokens);
        }
        if (rdbSaver != null) {
            rdbSaver.recordChange();
        }
    }
}
