package io.github.redis.scripting;

import io.github.redis.command.CommandTable;
import io.github.redis.command.RedisCommand;
import io.github.redis.core.RedisClient;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Phase-4 Lua script engine.
 *
 * <p>Exposes the canonical Redis Lua globals:
 * <ul>
 *   <li>{@code redis.call(cmd, ...args)} — runs the command, throws on error</li>
 *   <li>{@code redis.pcall(cmd, ...args)} — same but returns an error table</li>
 *   <li>{@code redis.status_reply(str)} / {@code redis.error_reply(str)}</li>
 *   <li>{@code redis.sha1hex(str)}</li>
 *   <li>{@code KEYS} — table of keys passed to EVAL</li>
 *   <li>{@code ARGV} — table of args passed to EVAL</li>
 * </ul>
 *
 * <p>Replies from {@code redis.call} are converted to Lua values matching Redis
 * conventions: bulk → string; integer → number; array → table; nil → false;
 * error → raises Lua error.
 *
 * <p>State lookup uses a synthetic {@link EmbeddedChannel} so the existing
 * {@link RedisClient} initialisation finds the {@code ServiceRegistry} attr.
 */
public final class LuaScriptEngine {

    private final org.luaj.vm2.Globals globals;
    private final io.github.redis.core.ServiceRegistry serviceRegistry;
    private final CommandTable commandTable;
    private final Map<String, LuaValue> scriptCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final int maxCacheSize;

    public LuaScriptEngine(io.github.redis.core.ServiceRegistry services, CommandTable table) {
        this(services, table, 10000);
    }

    public LuaScriptEngine(io.github.redis.core.ServiceRegistry services, CommandTable table, int maxCacheSize) {
        this.serviceRegistry = services;
        this.commandTable = table;
        this.maxCacheSize = maxCacheSize;
        this.globals = JsePlatform.standardGlobals();
        this.globals.load(new RedisLibrary());
    }

    /** Run a script by source. Returns a Java object ready for reply conversion. */
    public Object eval(String script, List<String> keys, List<String> argv, RedisClient invokingClient) {
        String sha1 = sha1hex(script);
        return evalsha(sha1, script, keys, argv, invokingClient);
    }

    public Object evalsha(String sha1, String scriptOrNull, List<String> keys, List<String> argv, RedisClient invokingClient) {
        LuaValue chunk = scriptCache.get(sha1);
        if (chunk == null) {
            if (scriptOrNull == null) {
                throw new IllegalArgumentException("NOSCRIPT No matching script. Please use EVAL.");
            }
            try {
                chunk = globals.load(scriptOrNull, "script");
            } catch (LuaError e) {
                throw new IllegalArgumentException("ERR Error compiling script: " + e.getMessage());
            }
            if (scriptCache.size() >= maxCacheSize) {
                scriptCache.clear();
            }
            scriptCache.put(sha1, chunk);
        }

        // Set up KEYS and ARGV globals.
        LuaTable keysTable = new LuaTable();
        if (keys != null) for (int i = 0; i < keys.size(); i++) keysTable.set(i + 1, LuaValue.valueOf(keys.get(i)));
        LuaTable argvTable = new LuaTable();
        if (argv != null) for (int i = 0; i < argv.size(); i++) argvTable.set(i + 1, LuaValue.valueOf(argv.get(i)));
        globals.set("KEYS", keysTable);
        globals.set("ARGV", argvTable);
        // Stash the calling client on a thread-local so redis.call can find it.
        CLIENT_HOLDER.set(invokingClient);
        try {
            LuaValue result = chunk.call();
            return luaToJava(result);
        } catch (LuaError e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            CLIENT_HOLDER.remove();
        }
    }

    public boolean flushAll() {
        scriptCache.clear();
        return true;
    }

    public boolean exists(String sha1) {
        return scriptCache.containsKey(sha1);
    }

    public String load(String script) {
        String sha1 = sha1hex(script);
        try {
            LuaValue chunk = globals.load(script, "script");
            scriptCache.put(sha1, chunk);
        } catch (LuaError e) {
            throw new IllegalArgumentException("ERR Error compiling script: " + e.getMessage());
        }
        return sha1;
    }

    public int loadedCount() {
        return scriptCache.size();
    }

    // ===== redis.* library =====

    private static final ThreadLocal<RedisClient> CLIENT_HOLDER = new ThreadLocal<>();

    private final class RedisLibrary extends TwoArgFunction {
        @Override
        public LuaValue call(LuaValue modname, LuaValue env) {
            LuaTable redis = new LuaTable();
            redis.set("call", new CallFn(false));
            redis.set("pcall", new CallFn(true));
            redis.set("status_reply", new StatusReplyFn());
            redis.set("error_reply", new ErrorReplyFn());
            redis.set("sha1hex", new Sha1HexFn());
            redis.set("LOG_DEBUG", LuaValue.valueOf(0));
            redis.set("LOG_VERBOSE", LuaValue.valueOf(1));
            redis.set("LOG_NOTICE", LuaValue.valueOf(2));
            redis.set("LOG_WARNING", LuaValue.valueOf(3));
            redis.set("replicate_commands", new OneArgFunction() {
                public LuaValue call(LuaValue a) { return LuaValue.TRUE; }
            });
            env.set("redis", redis);
            return redis;
        }
    }

    /** redis.call / redis.pcall — registered as VarArgFunction to capture all args. */
    private final class CallFn extends VarArgFunction {
        private final boolean protectedMode;
        CallFn(boolean p) { this.protectedMode = p; }

        @Override
        public Varargs invoke(Varargs args) {
            // args.arg1() = command name, arg(2..) = command arguments.
            List<String> argv = new ArrayList<>();
            argv.add(args.arg(1).checkjstring());
            int n = args.narg();
            for (int i = 2; i <= n; i++) {
                argv.add(luaToString(args.arg(i)));
            }
            return runRedisCall(argv, protectedMode);
        }
    }

    /** Run a parsed Redis command from within Lua. */
    private LuaValue runRedisCall(List<String> argv, boolean protectedMode) {
        RedisClient client = CLIENT_HOLDER.get();
        if (client == null) {
            if (protectedMode) return errorTable("ERR no client bound to current Lua thread");
            throw new LuaError("ERR no client bound to current Lua thread");
        }

        RedisCommand command = commandTable.get(argv.get(0));
        if (command == null) {
            String msg = "ERR Unknown Redis command called from script: " + argv.get(0);
            if (protectedMode) return errorTable(msg);
            throw new LuaError(msg);
        }
        if (!command.checkArity(argv.size())) {
            String msg = "ERR Wrong number of args calling Redis command from script: " + argv.get(0);
            if (protectedMode) return errorTable(msg);
            throw new LuaError(msg);
        }

        // The capture bucket records whatever reply the command writes.
        final java.util.concurrent.atomic.AtomicReference<Object> bucket = new java.util.concurrent.atomic.AtomicReference<>();
        final java.util.concurrent.atomic.AtomicReference<String> errorRef = new java.util.concurrent.atomic.AtomicReference<>();

        // Synthetic channel just so we have a pipeline to attach contexts to.
        final EmbeddedChannel ec = new EmbeddedChannel();
        ec.attr(AttributeKey.<io.github.redis.core.ServiceRegistry>valueOf("serviceRegistry"))
          .set(serviceRegistry);
        ec.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter());
        io.netty.channel.ChannelHandlerContext ecCtx = ec.pipeline().firstContext();

        final String[] argvArray = argv.toArray(new String[0]);
        RedisClient scriptClient = new RedisClient(ecCtx, serviceRegistry) {
            @Override public void replyError(String msg) {
                errorRef.set(msg);
                throw new RuntimeException(msg);
            }
            @Override public void replySyntaxError() { replyError("ERR syntax error"); }
            @Override public void replyWrongArgc() {
                replyError("ERR wrong number of arguments for '" + argvArray[0].toLowerCase() + "' command");
            }
        };
        scriptClient.setDbIndex(client.dbIndex());
        scriptClient.setAuthenticatedUser(client.authenticatedUser());
        scriptClient.setCommand(argvArray, argvArray.length);
        // Install the reply interceptor — captures every replyXXX call into bucket.
        scriptClient.setReplyInterceptor(msg -> {
            Object parsed = parseReplyMessage(msg);
            bucket.set(parsed);
            return parsed;
        });

        try {
            command.getHandler().execute(scriptClient);
            return javaToLua(bucket.get());
        } catch (RuntimeException re) {
            String msg = re.getMessage() == null ? re.getClass().getSimpleName() : re.getMessage();
            if (errorRef.get() != null) msg = errorRef.get();
            if (protectedMode) return errorTable(msg);
            throw new LuaError(msg);
        } finally {
            ec.finishAndReleaseAll();
        }
    }

    @SuppressWarnings("unchecked")
    private Object parseReplyMessage(Object msg) {
        if (msg == null) return null;
        if (msg instanceof io.netty.handler.codec.redis.SimpleStringRedisMessage) {
            return ((io.netty.handler.codec.redis.SimpleStringRedisMessage) msg).content();
        }
        if (msg instanceof io.netty.handler.codec.redis.ErrorRedisMessage) {
            throw new RuntimeException(((io.netty.handler.codec.redis.ErrorRedisMessage) msg).content());
        }
        if (msg instanceof io.netty.handler.codec.redis.IntegerRedisMessage) {
            return ((io.netty.handler.codec.redis.IntegerRedisMessage) msg).value();
        }
        if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage b = (FullBulkStringRedisMessage) msg;
            if (b.isNull()) return null;
            // Latin-1 to preserve binary safety.
            return b.content().toString(CharsetUtil.ISO_8859_1);
        }
        if (msg instanceof ArrayRedisMessage) {
            ArrayRedisMessage arr = (ArrayRedisMessage) msg;
            List<Object> out = new ArrayList<>();
            if (arr.children() != null) {
                for (Object child : arr.children()) {
                    out.add(parseReplyMessage(child));
                }
            }
            return out;
        }
        if (msg instanceof RedisMessage) {
            return msg.toString();
        }
        return msg.toString();
    }

    private static String luaToString(LuaValue v) {
        if (v.isstring()) return v.checkjstring();
        if (v.isint())    return Long.toString(v.checkint());
        if (v.isnumber()) {
            double d = v.checkdouble();
            if (d == Math.rint(d)) return Long.toString((long) d);
            return Double.toString(d);
        }
        if (v.isboolean()) return v.checkboolean() ? "true" : "false";
        return v.tojstring();
    }

    private static LuaValue errorTable(String msg) {
        LuaTable t = new LuaTable();
        t.set("err", LuaValue.valueOf(msg));
        return t;
    }

    private static final class StatusReplyFn extends OneArgFunction {
        @Override public LuaValue call(LuaValue arg) {
            LuaTable t = new LuaTable();
            t.set("ok", arg);
            return t;
        }
    }

    private static final class ErrorReplyFn extends OneArgFunction {
        @Override public LuaValue call(LuaValue arg) {
            return errorTable(arg.checkjstring());
        }
    }

    private static final class Sha1HexFn extends OneArgFunction {
        @Override public LuaValue call(LuaValue arg) {
            return LuaValue.valueOf(sha1hex(arg.checkjstring()));
        }
    }

    // ===== Lua <-> Java conversions =====

    private static Object luaToJava(LuaValue v) {
        if (v == null || v.isnil()) return null;
        if (v.isboolean()) return v.checkboolean();
        if (v.isint()) return v.checkint();
        if (v.isnumber()) {
            double d = v.checkdouble();
            if (d == Math.rint(d)) return (long) d;
            return Double.toString(d);
        }
        if (v.isstring()) return v.checkjstring();
        if (v.istable()) {
            LuaTable t = v.checktable();
            // Status / error reply tables: {ok="..."} or {err="..."}
            LuaValue okField = t.get("ok");
            if (!okField.isnil()) return new StatusReply(okField.checkjstring());
            LuaValue errField = t.get("err");
            if (!errField.isnil()) throw new RuntimeException(errField.checkjstring());
            int n = t.length();
            List<Object> list = new ArrayList<>(n);
            for (int i = 1; i <= n; i++) list.add(luaToJava(t.get(i)));
            return list;
        }
        return v.tojstring();
    }

    /** Marker for {ok="..."} status replies so dispatchReply can send a simple string. */
    public static final class StatusReply {
        public final String value;
        public StatusReply(String v) { this.value = v; }
    }

    @SuppressWarnings("unchecked")
    private static LuaValue javaToLua(Object value) {
        if (value == null) return LuaValue.FALSE; // Redis convention: nil → false in Lua
        if (value instanceof String) {
            String s = (String) value;
            if ("OK".equals(s)) {
                LuaTable t = new LuaTable();
                t.set("ok", LuaValue.valueOf("OK"));
                return t;
            }
            return LuaValue.valueOf(s);
        }
        if (value instanceof byte[]) return LuaValue.valueOf((byte[]) value);
        if (value instanceof Long)  return LuaValue.valueOf((Long) value);
        if (value instanceof Integer) return LuaValue.valueOf((Integer) value);
        if (value instanceof Boolean) return LuaValue.valueOf((Boolean) value);
        if (value instanceof List) {
            List<?> l = (List<?>) value;
            LuaTable t = new LuaTable();
            for (int i = 0; i < l.size(); i++) t.set(i + 1, javaToLua(l.get(i)));
            return t;
        }
        return LuaValue.valueOf(value.toString());
    }

    private static final class ReplyBucket {
        Object value;

        void simple(String s) { this.value = s; }
        void error(String msg) { this.value = msg; throw new RuntimeException(msg); }
        void integer(long n) { this.value = (Long) n; }
        void bulk(String s) { this.value = s; }
        void bulk(String[] bytes) {}
        void nil() { this.value = null; }
        @SuppressWarnings("unchecked")
        void array(List<?> list) { this.value = list; }
    }

    /** Compute the SHA1 hex digest (lowercase) used as EVALSHA key. */
    public static String sha1hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(Character.forDigit((b >> 4) & 0xF, 16))
                                   .append(Character.forDigit(b & 0xF, 16));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
