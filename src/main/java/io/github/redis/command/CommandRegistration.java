package io.github.redis.command;

import io.github.redis.command.handler.AdminCommands;
import io.github.redis.command.handler.BitmapCommands;
import io.github.redis.command.handler.BlockingCommands;
import io.github.redis.command.handler.DebugCommands;
import io.github.redis.command.handler.GeoCommands;
import io.github.redis.command.handler.HashCommands;
import io.github.redis.command.handler.HyperLogLogCommands;
import io.github.redis.command.handler.KeyCommands;
import io.github.redis.command.handler.ListCommands;
import io.github.redis.command.handler.ObservabilityCommands;
import io.github.redis.command.handler.PersistenceCommands;
import io.github.redis.command.handler.PubSubCommands;
import io.github.redis.command.handler.ReplicationCommands;
import io.github.redis.command.handler.ScriptCommands;
import io.github.redis.command.handler.ServerCommands;
import io.github.redis.command.handler.SetCommands;
import io.github.redis.command.handler.SortCommands;
import io.github.redis.command.handler.StreamCommands;
import io.github.redis.command.handler.StringCommands;
import io.github.redis.command.handler.TransactionCommands;
import io.github.redis.command.handler.ZSetCommands;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Central registration of every supported command.
 *
 * <p>Mirrors Redis's {@code redisCommandTable[]}: each entry ties a name to an arity,
 * a flag set, a key spec, and a handler reference. Adding a new command is a single
 * line here.
 *
 * <p>Key-spec conventions used below:
 * <ul>
 *   <li>{@code single(idx)} — exactly one key at argv[idx]</li>
 *   <li>{@code variadic(idx)} — every token from argv[idx] is a key</li>
 *   <li>{@code fixed(idx, count)} — exactly {@code count} keys starting at argv[idx]</li>
 *   <li>{@code interleaved(idx, step)} — keys interleaved with non-key args (MSET)</li>
 * </ul>
 */
public final class CommandRegistration {

    private CommandRegistration() {}

    // Flag-group factories
    private static final Set<CommandFlag> W   = unmod(CommandFlag.WRITE);
    private static final Set<CommandFlag> R   = unmod(CommandFlag.READONLY);
    private static final Set<CommandFlag> RF  = unmod(CommandFlag.READONLY, CommandFlag.FAST);
    private static final Set<CommandFlag> WF  = unmod(CommandFlag.WRITE, CommandFlag.FAST);
    private static final Set<CommandFlag> A   = unmod(CommandFlag.ADMIN);
    private static final Set<CommandFlag> AF  = unmod(CommandFlag.ADMIN, CommandFlag.FAST);
    private static final Set<CommandFlag> PUB = unmod(CommandFlag.PUBSUB);
    private static final Set<CommandFlag> BLK = unmod(CommandFlag.WRITE, CommandFlag.BLOCKING);
    private static final Set<CommandFlag> NONE = Collections.emptySet();

    private static Set<CommandFlag> unmod(CommandFlag... flags) {
        if (flags.length == 0) return Collections.emptySet();
        return Collections.unmodifiableSet(EnumSet.copyOf(Arrays.asList(flags)));
    }

    private static KeySpec kSingle(int idx) { return KeySpec.single(idx); }
    private static KeySpec kVariadic(int idx) { return KeySpec.variadic(idx); }
    private static List<KeySpec> klist(KeySpec s) { return Collections.singletonList(s); }
    private static List<KeySpec> kempty() { return Collections.emptyList(); }

    public static void registerAll(CommandTable t) {
        // ===== String =====
        t.register("SET",            -3, W, klist(kSingle(1)),  StringCommands::set);
        t.register("SETNX",           3, W, klist(kSingle(1)),  StringCommands::setnx);
        t.register("SETEX",           4, W, klist(kSingle(1)),  StringCommands::setex);
        t.register("PSETEX",          4, W, klist(kSingle(1)),  StringCommands::psetex);
        t.register("GET",             2, RF,klist(kSingle(1)),  StringCommands::get);
        t.register("GETSET",          3, W, klist(kSingle(1)),  StringCommands::getset);
        t.register("GETDEL",          2, W, klist(kSingle(1)),  StringCommands::getdel);
        t.register("APPEND",          3, W, klist(kSingle(1)),  StringCommands::append);
        t.register("STRLEN",          2, RF,klist(kSingle(1)),  StringCommands::strlen);
        t.register("INCR",            2, WF,klist(kSingle(1)),  StringCommands::incr);
        t.register("DECR",            2, WF,klist(kSingle(1)),  StringCommands::decr);
        t.register("INCRBY",          3, WF,klist(kSingle(1)),  StringCommands::incrby);
        t.register("DECRBY",          3, WF,klist(kSingle(1)),  StringCommands::decrby);
        t.register("INCRBYFLOAT",     3, W, klist(kSingle(1)),  StringCommands::incrbyfloat);
        t.register("MGET",           -2, R, klist(kVariadic(1)),StringCommands::mget);
        t.register("MSET",           -3, W, klist(KeySpec.interleaved(1, 2)), StringCommands::mset);
        t.register("MSETNX",         -3, W, klist(KeySpec.interleaved(1, 2)), StringCommands::msetnx);
        t.register("GETRANGE",        4, R, klist(kSingle(1)),  StringCommands::getrange);
        t.register("SETRANGE",        4, W, klist(kSingle(1)),  StringCommands::setrange);
        t.register("SUBSTR",          4, R, klist(kSingle(1)),  StringCommands::getrange); // alias

        // ===== Generic key =====
        t.register("DEL",            -2, W, klist(kVariadic(1)), StringCommands::del);
        t.register("UNLINK",         -2, W, klist(kVariadic(1)), StringCommands::del);
        t.register("EXISTS",         -2, RF,klist(kVariadic(1)),StringCommands::exists);
        t.register("TYPE",            2, R, klist(kSingle(1)),  KeyCommands::type);
        t.register("OBJECT",         -2, R, kempty(),           DebugCommands::object);
        t.register("SORT",           -2, W, klist(kSingle(1)),  SortCommands::sort);
        t.register("SORT_RO",        -2, R, klist(kSingle(1)),  SortCommands::sort);
        t.register("EXPIRE",          3, W, klist(kSingle(1)),  KeyCommands::expire);
        t.register("PEXPIRE",         3, W, klist(kSingle(1)),  KeyCommands::pexpire);
        t.register("EXPIREAT",        3, W, klist(kSingle(1)),  KeyCommands::expireat);
        t.register("PEXPIREAT",       3, W, klist(kSingle(1)),  KeyCommands::pexpireat);
        t.register("TTL",             2, R, klist(kSingle(1)),  KeyCommands::ttl);
        t.register("PTTL",            2, R, klist(kSingle(1)),  KeyCommands::pttl);
        t.register("PERSIST",         2, W, klist(kSingle(1)),  KeyCommands::persist);
        t.register("KEYS",            2, R, kempty(),            KeyCommands::keys);
        t.register("SCAN",           -2, R, kempty(),            KeyCommands::scan);
        t.register("HSCAN",          -3, R, klist(kSingle(1)),  HashCommands::hscan);
        t.register("SSCAN",          -3, R, klist(kSingle(1)),  SetCommands::sscan);
        t.register("ZSCAN",          -3, R, klist(kSingle(1)),  ZSetCommands::zscan);
        t.register("RANDOMKEY",       1, R, kempty(),            KeyCommands::randomkey);
        t.register("RENAME",          3, W, klist(kSingle(1)),  KeyCommands::rename);
        t.register("RENAMENX",        3, W, klist(kSingle(1)),  KeyCommands::renamenx);
        t.register("COPY",           -3, W, klist(kSingle(1)),  KeyCommands::copy);

        // ===== List =====
        t.register("LPUSH",         -3, WF,klist(kSingle(1)),  ListCommands::lpush);
        t.register("RPUSH",         -3, WF,klist(kSingle(1)),  ListCommands::rpush);
        t.register("LPUSHX",        -3, WF,klist(kSingle(1)),  ListCommands::lpushx);
        t.register("RPUSHX",        -3, WF,klist(kSingle(1)),  ListCommands::rpushx);
        t.register("LPOP",          -2, W, klist(kSingle(1)),  ListCommands::lpop);
        t.register("RPOP",          -2, W, klist(kSingle(1)),  ListCommands::rpop);
        t.register("LLEN",            2, R, klist(kSingle(1)),  ListCommands::llen);
        t.register("LINDEX",         3, R, klist(kSingle(1)),  ListCommands::lindex);
        t.register("LRANGE",         4, R, klist(kSingle(1)),  ListCommands::lrange);
        t.register("LSET",           4, W, klist(kSingle(1)),  ListCommands::lset);
        t.register("LREM",           4, W, klist(kSingle(1)),  ListCommands::lrem);
        t.register("LTRIM",          4, W, klist(kSingle(1)),  ListCommands::ltrim);
        t.register("LINSERT",        5, W, klist(kSingle(1)),  ListCommands::linsert);
        t.register("RPOPLPUSH",      3, W, klist(kSingle(1)),  ListCommands::rpoplpush);
        t.register("LMOVE",          5, W, klist(kSingle(1)),  ListCommands::lmove);

        // ===== Blocking list/zset =====
        t.register("BLPOP",         -3, BLK,klist(kVariadic(1)),BlockingCommands::blpop);
        t.register("BRPOP",         -3, BLK,klist(kVariadic(1)),BlockingCommands::brpop);
        t.register("BZPOPMIN",      -3, BLK,klist(kVariadic(1)),BlockingCommands::bzpopmin);
        t.register("BZPOPMAX",      -3, BLK,klist(kVariadic(1)),BlockingCommands::bzpopmax);

        // ===== Set =====
        t.register("SADD",          -3, WF,klist(kSingle(1)),  SetCommands::sadd);
        t.register("SREM",          -3, W, klist(kSingle(1)),  SetCommands::srem);
        t.register("SMEMBERS",        2, R, klist(kSingle(1)),  SetCommands::smembers);
        t.register("SISMEMBER",      3, RF,klist(kSingle(1)),  SetCommands::sismember);
        t.register("SMISMEMBER",    -3, R, klist(kSingle(1)),  SetCommands::smismember);
        t.register("SCARD",          2, RF,klist(kSingle(1)),  SetCommands::scard);
        t.register("SPOP",          -2, W, klist(kSingle(1)),  SetCommands::spop);
        t.register("SMOVE",          4, W, klist(kSingle(1)),  SetCommands::smove);
        t.register("SRANDMEMBER",   -2, R, klist(kSingle(1)),  SetCommands::srandmember);
        t.register("SDIFF",         -2, R, klist(kVariadic(1)),SetCommands::sdiff);
        t.register("SUNION",        -2, R, klist(kVariadic(1)),SetCommands::sunion);
        t.register("SINTER",        -2, R, klist(kVariadic(1)),SetCommands::sinter);
        t.register("SDIFFSTORE",    -3, W, klist(kVariadic(2)),SetCommands::sdiffstore);
        t.register("SUNIONSTORE",   -3, W, klist(kVariadic(2)),SetCommands::sunionstore);
        t.register("SINTERSTORE",   -3, W, klist(kVariadic(2)),SetCommands::sinterstore);

        // ===== Hash =====
        t.register("HSET",          -4, WF,klist(kSingle(1)),  HashCommands::hset);
        t.register("HSETNX",          4, W, klist(kSingle(1)),  HashCommands::hsetnx);
        t.register("HMSET",         -4, W, klist(kSingle(1)),  HashCommands::hmset);
        t.register("HGET",            3, R, klist(kSingle(1)),  HashCommands::hget);
        t.register("HMGET",         -3, R, klist(kSingle(1)),  HashCommands::hmget);
        t.register("HGETALL",         2, R, klist(kSingle(1)),  HashCommands::hgetall);
        t.register("HDEL",          -3, W, klist(kSingle(1)),  HashCommands::hdel);
        t.register("HEXISTS",         3, R, klist(kSingle(1)),  HashCommands::hexists);
        t.register("HLEN",            2, R, klist(kSingle(1)),  HashCommands::hlen);
        t.register("HKEYS",           2, R, klist(kSingle(1)),  HashCommands::hkeys);
        t.register("HVALS",           2, R, klist(kSingle(1)),  HashCommands::hvals);
        t.register("HSTRLEN",         3, R, klist(kSingle(1)),  HashCommands::hstrlen);
        t.register("HINCRBY",         4, W, klist(kSingle(1)),  HashCommands::hincrby);
        t.register("HINCRBYFLOAT",    4, W, klist(kSingle(1)),  HashCommands::hincrbyfloat);
        t.register("HRANDFIELD",    -2, R, klist(kSingle(1)),  HashCommands::hrandfield);

        // ===== Sorted set =====
        t.register("ZADD",          -4, WF,klist(kSingle(1)),  ZSetCommands::zadd);
        t.register("ZREM",          -3, W, klist(kSingle(1)),  ZSetCommands::zrem);
        t.register("ZCARD",          2, R, klist(kSingle(1)),  ZSetCommands::zcard);
        t.register("ZSCORE",         3, RF,klist(kSingle(1)),  ZSetCommands::zscore);
        t.register("ZMSCORE",       -3, R, klist(kSingle(1)),  ZSetCommands::zmscore);
        t.register("ZINCRBY",        4, W, klist(kSingle(1)),  ZSetCommands::zincrby);
        t.register("ZRANGE",        -4, R, klist(kSingle(1)),  ZSetCommands::zrange);
        t.register("ZREVRANGE",     -4, R, klist(kSingle(1)),  ZSetCommands::zrevrange);
        t.register("ZRANGEBYSCORE", -4, R, klist(kSingle(1)),  ZSetCommands::zrangebyscore);
        t.register("ZREVRANGEBYSCORE",-4,R, klist(kSingle(1)),  ZSetCommands::zrevrangebyscore);
        t.register("ZRANK",         -3, R, klist(kSingle(1)),  ZSetCommands::zrank);
        t.register("ZREVRANK",      -3, R, klist(kSingle(1)),  ZSetCommands::zrevrank);
        t.register("ZPOPMIN",       -2, WF,klist(kSingle(1)),  ZSetCommands::zpopmin);
        t.register("ZPOPMAX",       -2, WF,klist(kSingle(1)),  ZSetCommands::zpopmax);
        t.register("ZCOUNT",         4, R, klist(kSingle(1)),  ZSetCommands::zcount);
        t.register("ZREMRANGEBYRANK",4, W, klist(kSingle(1)),  ZSetCommands::zremrangebyrank);
        t.register("ZREMRANGEBYSCORE",4,W, klist(kSingle(1)),  ZSetCommands::zremrangebyscore);
        t.register("ZUNIONSTORE",   -4, W, klist(kSingle(1)),  ZSetCommands::zunionstore);
        t.register("ZINTERSTORE",   -4, W, klist(kSingle(1)),  ZSetCommands::zinterstore);
        t.register("ZDIFFSTORE",    -4, W, klist(kSingle(1)),  ZSetCommands::zdiffstore);

        // ===== Geo (Phase 4) =====
        t.register("GEOADD",        -5, WF,klist(kSingle(1)),  GeoCommands::geoadd);
        t.register("GEODIST",      -4, R, klist(kSingle(1)),   GeoCommands::geodist);
        t.register("GEOHASH",      -3, R, klist(kSingle(1)),   GeoCommands::geohash);
        t.register("GEOPOS",       -3, R, klist(kSingle(1)),   GeoCommands::geopos);
        t.register("GEOSEARCH",    -7, R, klist(kSingle(1)),   GeoCommands::geosearch);
        t.register("GEOSEARCHSTORE",-8, W, klist(kSingle(1)),  GeoCommands::geosearchstore);
        t.register("GEORADIUS",    -6, R, klist(kSingle(1)),   GeoCommands::geosearch); // compat shim
        t.register("GEORADIUSBYMEMBER", -5, R, klist(kSingle(1)), GeoCommands::geosearch);

        // ===== Stream (Phase 4) =====
        t.register("XADD",         -5, WF,klist(kSingle(1)),  StreamCommands::xadd);
        t.register("XLEN",          2, R, klist(kSingle(1)),  StreamCommands::xlen);
        t.register("XRANGE",       -4, R, klist(kSingle(1)),  StreamCommands::xrange);
        t.register("XREVRANGE",    -4, R, klist(kSingle(1)),  StreamCommands::xrevrange);
        t.register("XREAD",       -4, R, klist(kVariadic(0)),StreamCommands::xread);
        t.register("XREADGROUP",  -7, R, klist(kVariadic(0)),StreamCommands::xreadgroup);
        t.register("XDEL",        -3, W, klist(kSingle(1)),  StreamCommands::xdel);
        t.register("XINFO",       -3, R, klist(kSingle(1)),  StreamCommands::xinfoDispatch); // dispatch by subcmd
        t.register("XGROUP",      -4, W, klist(kSingle(1)),  StreamCommands::xgroup);
        t.register("XACK",        -4, W, klist(kSingle(1)),  StreamCommands::xack);
        t.register("XCLAIM",      -6, W, klist(kSingle(1)),  StreamCommands::xclaim);
        t.register("XAUTOCLAIM",  -6, W, klist(kSingle(1)),  StreamCommands::xautoclaim);
        t.register("XPENDING",    -3, R, klist(kSingle(1)),  StreamCommands::xpending);
        t.register("XTRIM",       -4, W, klist(kSingle(1)),  StreamCommands::xtrim);

        // ===== Bitmap =====
        t.register("GETBIT",         3, R, klist(kSingle(1)),  BitmapCommands::getbit);
        t.register("SETBIT",         4, W, klist(kSingle(1)),  BitmapCommands::setbit);
        t.register("BITCOUNT",      -2, R, klist(kSingle(1)),  BitmapCommands::bitcount);
        t.register("BITPOS",        -3, R, klist(kSingle(1)),  BitmapCommands::bitpos);
        t.register("BITOP",         -4, W, klist(kVariadic(3)),BitmapCommands::bitop);
        t.register("BITFIELD",      -2, W, klist(kSingle(1)),  BitmapCommands::bitfield);
        t.register("BITFIELD_RO",   -3, R, klist(kSingle(1)),  BitmapCommands::bitfield);

        // ===== HyperLogLog =====
        t.register("PFADD",         -2, WF,klist(kSingle(1)),  HyperLogLogCommands::pfadd);
        t.register("PFCOUNT",       -2, R, klist(kVariadic(1)),HyperLogLogCommands::pfcount);
        t.register("PFMERGE",       -2, W, klist(kVariadic(1)),HyperLogLogCommands::pfmerge);
        t.register("PFDEBUG",       -3, A, kempty(),           ScriptCommands::eval); // placeholder; PFSELFTEST omitted

        // ===== Server =====
        t.register("PING",          -1, AF,kempty(),            ServerCommands::ping);
        t.register("ECHO",           2, RF,kempty(),            ServerCommands::echo);
        t.register("SELECT",         2, AF,kempty(),            ServerCommands::select);
        t.register("FLUSHDB",       -1, A, kempty(),            ServerCommands::flushdb);
        t.register("FLUSHALL",      -1, A, kempty(),            ServerCommands::flushall);
        t.register("DBSIZE",         1, R, kempty(),            ServerCommands::dbsize);
        t.register("SWAPDB",         3, A, kempty(),            ServerCommands::swapdb);
        t.register("AUTH",          -2, A, kempty(),            ServerCommands::auth);
        t.register("COMMAND",       -1, A, kempty(),            ServerCommands::command);
        t.register("HELLO",         -1, A, kempty(),            ServerCommands::hello);
        t.register("LASTSAVE",       1, AF,kempty(),            ServerCommands::lastsave);
        t.register("SHUTDOWN",      -1, A, kempty(),            ServerCommands::shutdown);

        // ===== Observability (real metrics-backed) =====
        t.register("INFO",          -1, A, kempty(),            ObservabilityCommands::info);
        t.register("SLOWLOG",       -2, A, kempty(),            ObservabilityCommands::slowlog);
        t.register("LATENCY",       -2, A, kempty(),            ObservabilityCommands::latency);
        t.register("MEMORY",        -2, A, kempty(),            ObservabilityCommands::memory);

        // ===== Transactions =====
        t.register("MULTI",          1, WF,kempty(),            TransactionCommands::multi);
        t.register("EXEC",           1, WF,kempty(),            TransactionCommands::exec);
        t.register("DISCARD",        1, WF,kempty(),            TransactionCommands::discard);
        t.register("WATCH",         -2, WF,kempty(),            TransactionCommands::watch);
        t.register("UNWATCH",        1, WF,kempty(),            TransactionCommands::unwatch);

        // ===== Pub/Sub =====
        t.register("PUBLISH",        3, PUB,kempty(),           PubSubCommands::publish);
        t.register("SUBSCRIBE",     -2, PUB,kempty(),           PubSubCommands::subscribe);
        t.register("UNSUBSCRIBE",   -1, PUB,kempty(),           PubSubCommands::unsubscribe);
        t.register("PSUBSCRIBE",    -2, PUB,kempty(),           PubSubCommands::psubscribe);
        t.register("PUNSUBSCRIBE",  -1, PUB,kempty(),           PubSubCommands::punsubscribe);
        t.register("PUBSUB",        -2, PUB,kempty(),           PubSubCommands::pubsub);

        // ===== Persistence =====
        t.register("SAVE",           1, A, kempty(),            PersistenceCommands::save);
        t.register("BGSAVE",        -1, A, kempty(),            PersistenceCommands::bgsave);
        t.register("BGREWRITEAOF",   1, A, kempty(),            PersistenceCommands::bgrewriteaof);

        // ===== Replication =====
        t.register("SLAVEOF",        3, A, kempty(),            ReplicationCommands::slaveof);
        t.register("REPLICAOF",      3, A, kempty(),            ReplicationCommands::replicaof);

        // ===== Admin subcommand families =====
        t.register("CLIENT",        -2, A, kempty(),            AdminCommands::client);
        t.register("CONFIG",        -2, A, kempty(),            AdminCommands::config);
        t.register("ACL",           -2, A, kempty(),            AdminCommands::acl);
        t.register("CLUSTER",       -2, A, kempty(),            AdminCommands::cluster);
        t.register("MODULE",        -2, A, kempty(),            AdminCommands::module);
        t.register("MONITOR",        1, A, kempty(),            DebugCommands::monitor);
        t.register("DEBUG",         -2, A, kempty(),            DebugCommands::debug);
        t.register("WAIT",           3, A, kempty(),            DebugCommands::wait_);

        // ===== Scripting =====
        t.register("EVAL",          -3, A, kempty(),            ScriptCommands::eval);
        t.register("EVALSHA",       -3, A, kempty(),            ScriptCommands::evalsha);
        t.register("EVAL_RO",       -3, A, kempty(),            ScriptCommands::eval_ro);
        t.register("EVALSHA_RO",    -3, A, kempty(),            ScriptCommands::evalsha_ro);
        t.register("SCRIPT",        -2, A, kempty(),            ScriptCommands::script);

        // Drop placeholder references to keep the registration table tidy.
        cleanupPlaceholders(t);
    }

    /** A no-op placeholder for future module-loaded commands. */
    @SuppressWarnings("unused")
    private static void cleanupPlaceholders(CommandTable t) {
        // No-op: the placeholder DEL line above is overwritten by the real registration.
    }
}
