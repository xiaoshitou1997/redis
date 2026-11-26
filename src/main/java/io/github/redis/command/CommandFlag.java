package io.github.redis.command;

public enum CommandFlag {
    WRITE("write", "Write command"),
    READONLY("readonly", "Read-only command"),
    DENYOOM("denyoom", "Reject command when out of memory"),
    ADMIN("admin", "Administrative command"),
    NOSCRIPT("noscript", "Cannot be used in scripts"),
    RANDOM("random", "Random result"),
    LOADING("loading", "Allowed during loading"),
    STALE("stale", "Allowed on stale replica"),
    SKIP_MONITOR("skip_monitor", "Not recorded in MONITOR"),
    ASKING("asking", "Cluster: accept during migration"),
    FAST("fast", "Fast O(1) or O(log N) command"),
    NO_MANDATORY_KEYS("no_mandatory_keys", "No mandatory keys required"),
    PROTECTED("protected", "Protected command like AUTH"),
    MOVABLE_KEYS("movablekeys", "Keys are movable"),
    INTERLEAVED_KEYS("interleaved_keys", "Keys are interleaved with arguments"),
    MODULE("module", "Module command"),
    MODULE_GETKEYS("module_getkeys", "Module provides key extraction"),
    NO_ASYNC_LOADING("no_async_loading", "Not allowed during async loading"),
    PUBSUB("pubsub", "Pub/Sub command"),
    BLOCKING("blocking", "Command may block the client"),
    DANGEROUS("dangerous", "Potentially dangerous command"),
    CONNECTION("connection", "Connection-related command"),
    TRANSACTION("transaction", "Transaction-related command"),
    SCRIPTING("scripting", "Scripting-related command");

    private final String code;
    private final String description;

    CommandFlag(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
