package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.scripting.LuaScriptEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Script commands backed by the real {@link LuaScriptEngine} (Phase 4).
 *
 * <p>Implemented: EVAL / EVALSHA / EVAL_RO / EVALSHA_RO (treated as EVAL),
 * SCRIPT EXISTS / FLUSH / LOAD / KILL.
 */
public final class ScriptCommands {

    private static volatile LuaScriptEngine engine;

    public static void setEngine(LuaScriptEngine e) {
        engine = e;
    }

    private ScriptCommands() {}

    public static void eval(RedisClient c) { evalImpl(c, false); }
    public static void evalsha(RedisClient c) { evalImpl(c, true); }
    public static void eval_ro(RedisClient c) { evalImpl(c, false); }
    public static void evalsha_ro(RedisClient c) { evalImpl(c, true); }

    private static void evalImpl(RedisClient c, boolean bySha) {
        // EVAL script numkeys key [key ...] arg [arg ...]
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        if (engine == null) {
            c.replyError("ERR Lua scripting not available in this build");
            return;
        }
        String script = c.arg(1);
        Integer numKeys = Args.parseInt(c, 2);
        if (numKeys == null) return;
        if (numKeys < 0 || 2 + numKeys >= c.argc()) { c.replyError("ERR Number of keys can't be negative"); return; }
        List<String> keys = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++) keys.add(c.arg(3 + i));
        List<String> argv = new ArrayList<>();
        for (int j = 3 + numKeys; j < c.argc(); j++) argv.add(c.arg(j));

        String sha1 = bySha ? script : null;
        try {
            Object result;
            if (bySha) result = engine.evalsha(sha1, null, keys, argv, c);
            else       result = engine.eval(script, keys, argv, c);
            // Convert to wire reply.
            dispatchReply(c, result);
        } catch (IllegalArgumentException e) {
            // Includes NOSCRIPT + compile errors.
            c.replyError(e.getMessage());
        } catch (RuntimeException e) {
            c.replyError("ERR " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
    }

    /** Lua returns a status-reply table {ok="..."} → simple-string. */
    @SuppressWarnings("unchecked")
    private static void dispatchReply(RedisClient c, Object result) {
        if (result == null) { c.replyNil(); return; }
        if (result instanceof LuaScriptEngine.StatusReply) {
            c.replyStatus(((LuaScriptEngine.StatusReply) result).value);
            return;
        }
        if (result instanceof String) {
            c.replyBulk((String) result);
            return;
        }
        if (result instanceof Long || result instanceof Integer) {
            c.replyInteger(((Number) result).longValue());
            return;
        }
        if (result instanceof Boolean) {
            c.replyInteger(((Boolean) result) ? 1 : 0);
            return;
        }
        if (result instanceof List) {
            c.replyArray((List<?>) result);
            return;
        }
        c.replyBulk(result.toString());
    }

    public static void script(RedisClient c) {
        if (engine == null) {
            c.replyError("ERR Lua scripting not available in this build");
            return;
        }
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "EXISTS": {
                List<Object> out = new ArrayList<>(c.argc() - 2);
                for (int j = 2; j < c.argc(); j++) {
                    out.add(engine.exists(c.arg(j)) ? 1L : 0L);
                }
                c.replyArray(out);
                return;
            }
            case "FLUSH":
                engine.flushAll();
                c.replyOK();
                return;
            case "LOAD": {
                if (c.argc() != 3) { c.replyWrongArgc(); return; }
                try {
                    String sha1 = engine.load(c.arg(2));
                    c.replyBulk(sha1);
                } catch (IllegalArgumentException e) {
                    c.replyError(e.getMessage());
                }
                return;
            }
            case "KILL":
                c.replyError("NOTBUSY No scripts in execution right now.");
                return;
            default:
                c.replyError("ERR Unknown SCRIPT subcommand: " + sub);
        }
    }
}
