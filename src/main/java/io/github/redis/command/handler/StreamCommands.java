package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.encoding.Encoding;
import io.github.redis.encoding.StreamCarrier;
import io.github.redis.storage.RedisValue;
import io.github.redis.storage.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis Stream commands.
 *
 * <p>Implemented (Phase 4):
 * <ul>
 *   <li>XADD key [NOMKSTREAM] [MAXLEN [~|=] count | MINID [~|=] id] *|id field value [...]</li>
 *   <li>XLEN key</li>
 *   <li>XRANGE key start end [COUNT count]</li>
 *   <li>XREVRANGE key end start [COUNT count]</li>
 *   <li>XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]</li>
 *   <li>XDEL key id [id ...]</li>
 *   <li>XINFO STREAM key [FULL [COUNT count]]</li>
 *   <li>XINFO GROUPS key</li>
 *   <li>XINFO CONSUMERS key group</li>
 *   <li>XGROUP CREATE key group id [MKSTREAM]</li>
 *   <li>XGROUP SETID key group id</li>
 *   <li>XGROUP DESTROY key group</li>
 *   <li>XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]</li>
 *   <li>XACK key group id [id ...]</li>
 *   <li>XPENDING key group</li>
 *   <li>XTRIM key [MAXLEN [~|=] count | MINID [~|=] id]</li>
 * </ul>
 */
public final class StreamCommands {

    private StreamCommands() {}

    // ===== XADD =====

    public static void xadd(RedisClient c) {
        // XADD key [NOMKSTREAM] [MAXLEN [~|=] count | MINID [~|=] id] *|id field value [field value ...]
        if (c.argc() < 5) { c.replyWrongArgc(); return; }
        int j = 1;
        String key = c.arg(j++);
        boolean nomkstream = false;
        int maxLen = -1; boolean maxLenApprox = false;
        String minId = null; boolean minIdApprox = false;

        // Parse options until we hit the ID
        while (j < c.argc()) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("NOMKSTREAM")) { nomkstream = true; j++; continue; }
            if (opt.equals("MAXLEN")) {
                j++;
                if (j < c.argc() && (c.arg(j).equals("~") || c.arg(j).equals("="))) {
                    maxLenApprox = c.arg(j).equals("~");
                    j++;
                }
                if (j >= c.argc()) { c.replySyntaxError(); return; }
                Integer n = Args.parseInt(c, j);
                if (n == null) return;
                maxLen = n;
                j++;
                continue;
            }
            if (opt.equals("MINID")) {
                j++;
                if (j < c.argc() && (c.arg(j).equals("~") || c.arg(j).equals("="))) {
                    minIdApprox = c.arg(j).equals("~");
                    j++;
                }
                if (j >= c.argc()) { c.replySyntaxError(); return; }
                minId = c.arg(j);
                j++;
                continue;
            }
            break; // ID token
        }

        if (j >= c.argc()) { c.replySyntaxError(); return; }
        String idArg = c.arg(j++);
        if ((c.argc() - j) % 2 != 0 || j == c.argc()) { c.replySyntaxError(); return; }
        List<String> fieldValues = new ArrayList<>();
        for (; j < c.argc(); j++) fieldValues.add(c.arg(j));

        StreamCarrier stream = getOrCreateStream(c, key, nomkstream);
        if (stream == null) { c.replyNil(); return; }

        // Compute ID
        StreamCarrier.StreamId id;
        if (idArg.equals("*")) {
            id = nextAutoId(stream);
        } else {
            id = StreamCarrier.StreamId.parse(idArg);
            if (id == null) {
                c.replyError("ERR Invalid stream ID specified as stream command argument"); return;
            }
            if (id.compareTo(stream.lastId()) <= 0 && stream.size() > 0) {
                c.replyError("ERR The ID specified in XADD is equal or smaller than the target stream top item"); return;
            }
            // Allow partial IDs "<ms>-*" → "<ms>-0"
            if (!idArg.equals(id.toString()) && idArg.indexOf('-') < 0) {
                // pure-ms form: should become ms-0
                id = new StreamCarrier.StreamId(id.ms, 0);
            }
        }

        stream.append(id, fieldValues);

        // Apply trimming
        if (maxLen >= 0) stream.trim(maxLen, maxLenApprox);
        if (minId != null) {
            StreamCarrier.StreamId minSid = StreamCarrier.StreamId.parse(minId);
            if (minSid != null) trimMinId(stream, minSid);
        }

        // Wake any clients blocked on XREAD for this key
        io.github.redis.core.BlockingCoordinator bc =
                c.services().get(io.github.redis.core.BlockingCoordinator.class);
        if (bc != null) bc.notifyPushedTo(key);

        c.replyBulk(id.toString());
    }

    private static StreamCarrier.StreamId nextAutoId(StreamCarrier s) {
        long now = System.currentTimeMillis();
        StreamCarrier.StreamId last = s.lastId();
        if (now > last.ms) return new StreamCarrier.StreamId(now, 0);
        return new StreamCarrier.StreamId(last.ms, last.seq + 1);
    }

    private static void trimMinId(StreamCarrier s, StreamCarrier.StreamId minId) {
        // Remove entries whose id < minId
        for (int i = s.size() - 1; i >= 0; i--) {
            // No direct delete by index; iterate from head via range
            break;
        }
        // Implementation: collect then delete (simpler than writing an iterator mutation).
        List<StreamCarrier.Entry> toRemove = new ArrayList<>();
        for (StreamCarrier.Entry e : s.range(new StreamCarrier.StreamId(0, 0),
                new StreamCarrier.StreamId(minId.ms, minId.seq > 0 ? minId.seq - 1 : 0),
                Integer.MAX_VALUE, false)) {
            if (e.id.compareTo(minId) < 0) toRemove.add(e);
        }
        for (StreamCarrier.Entry e : toRemove) s.delete(e.id.toString());
    }

    // ===== XLEN =====

    public static void xlen(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        c.replyInteger(s == null ? 0 : s.size());
    }

    // ===== XRANGE / XREVRANGE =====

    public static void xrange(RedisClient c) { rangeImpl(c, false); }
    public static void xrevrange(RedisClient c) { rangeImpl(c, true); }

    private static void rangeImpl(RedisClient c, boolean reverse) {
        // XRANGE key start end [COUNT count]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        if (s == null) { c.replyEmptyArray(); return; }
        StreamCarrier.StreamId start = parseBound(c.arg(2), true);
        StreamCarrier.StreamId end = parseBound(c.arg(3), false);
        if (start == null || end == null) { c.replyError("ERR Invalid stream ID"); return; }
        int count = -1;
        if (c.argc() >= 6 && c.arg(4).equalsIgnoreCase("COUNT")) {
            Integer n = Args.parseInt(c, 5);
            if (n == null) return;
            count = n;
        }
        List<StreamCarrier.Entry> entries = s.range(start, end, count, reverse);
        List<Object> out = new ArrayList<>(entries.size());
        for (StreamCarrier.Entry e : entries) {
            out.add(entryToReply(e));
        }
        c.replyArray(out);
    }

    private static StreamCarrier.StreamId parseBound(String s, boolean isStart) {
        if (s.equals("-")) return new StreamCarrier.StreamId(0, 0);
        if (s.equals("+")) return new StreamCarrier.StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
        if (s.equals("$")) return new StreamCarrier.StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
        StreamCarrier.StreamId id = StreamCarrier.StreamId.parse(s);
        if (id == null) return null;
        // Allow "<ms>" form for start (means ms-0) and end (means ms-UINT64_MAX)
        if (s.indexOf('-') < 0) {
            if (isStart) return new StreamCarrier.StreamId(id.ms, 0);
            else        return new StreamCarrier.StreamId(id.ms, Long.MAX_VALUE);
        }
        return id;
    }

    // ===== XDEL =====

    public static void xdel(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        if (s == null) { c.replyInteger(0); return; }
        long deleted = 0;
        for (int j = 2; j < c.argc(); j++) {
            if (s.delete(c.arg(j))) deleted++;
        }
        c.replyInteger(deleted);
    }

    // ===== XINFO (top-level dispatcher) =====

    public static void xinfoDispatch(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        switch (sub) {
            case "STREAM":    xinfoStream(c);    return;
            case "GROUPS":    xinfoGroups(c);    return;
            case "CONSUMERS": xinfoConsumers(c); return;
            case "HELP":
                c.replyArray(Arrays.asList(
                        "XINFO <subcommand> [<arg> ...]. Subcommands are:",
                        "STREAM <key> [FULL [COUNT count]]",
                        "GROUPS <key>",
                        "CONSUMERS <key> <group>",
                        "HELP -- Print this help."));
                return;
            default:
                c.replyError("ERR Unknown XINFO subcommand: " + sub);
        }
    }

    // ===== XINFO =====

    // ===== XCLAIM =====

    public static void xclaim(RedisClient c) {
        // XCLAIM key group consumer min_idle_time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
        if (c.argc() < 6) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        if (s == null) { c.replyEmptyArray(); return; }
        StreamCarrier.ConsumerGroup g = s.getGroup(c.arg(2));
        if (g == null) { c.replyEmptyArray(); return; }
        String destConsumer = c.arg(3);
        long minIdleTime;
        try { minIdleTime = Long.parseLong(c.arg(4)); }
        catch (NumberFormatException e) { c.replyError("ERR Invalid min-idle-time"); return; }

        boolean justId = false;
        boolean force = false;
        long idleOverride = -1;
        List<String> ids = new ArrayList<>();
        for (int j = 5; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            switch (opt) {
                case "JUSTID":    justId = true; break;
                case "FORCE":     force = true; break;
                case "IDLE":      if (j+1 < c.argc()) { idleOverride = Args.parseLong(c, j+1); j++; } break;
                case "RETRYCOUNT": if (j+1 < c.argc()) { j++; } break; // accepted but no-op
                case "TIME":      if (j+1 < c.argc()) { j++; } break; // accepted but no-op
                case "LASTID":    if (j+1 < c.argc()) { j++; } break; // accepted but no-op
                default:          ids.add(c.arg(j));
            }
        }

        StreamCarrier.Consumer destCons = g.getOrCreateConsumer(destConsumer);
        long now = System.currentTimeMillis();
        List<Object> out = new ArrayList<>();
        for (String idStr : ids) {
            StreamCarrier.PelEntry pe = force ? g.pending.get(idStr) : null;
            if (pe == null) {
                pe = g.pending.get(idStr);
                if (pe == null) continue;
                if (now - pe.deliveryTime < minIdleTime) continue;
            }
            StreamCarrier.Entry e = s.get(idStr);
            if (e == null) continue;
            // Move the PEL entry from previous consumer to destConsumer.
            StreamCarrier.Consumer prevCons = g.consumers.get(pe.consumer);
            if (prevCons != null) prevCons.pendingIds.remove(idStr);
            pe.consumer = destConsumer;
            pe.deliveryTime = (idleOverride > 0) ? now - idleOverride : now;
            pe.deliveryCount++;
            destCons.pendingIds.add(idStr);
            if (justId) {
                out.add(idStr);
            } else {
                out.add(entryToReply(e));
            }
        }
        c.replyArray(out);
    }

    // ===== XAUTOCLAIM =====

    public static void xautoclaim(RedisClient c) {
        // XAUTOCLAIM key group consumer min_idle_time start [COUNT count] [JUSTID]
        if (c.argc() < 6) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        if (s == null) { c.replyEmptyArray(); return; }
        StreamCarrier.ConsumerGroup g = s.getGroup(c.arg(2));
        if (g == null) { c.replyError("NOGROUP No such consumer group"); return; }
        String destConsumer = c.arg(3);
        long minIdleTime;
        try { minIdleTime = Long.parseLong(c.arg(4)); }
        catch (NumberFormatException e) { c.replyError("ERR Invalid min-idle-time"); return; }
        StreamCarrier.StreamId start = StreamCarrier.StreamId.parse(c.arg(5));
        if (start == null) { c.replyError("ERR Invalid stream ID"); return; }

        int count = 100;
        boolean justId = false;
        for (int j = 6; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("COUNT") && j + 1 < c.argc()) {
                Integer n = Args.parseInt(c, j + 1); if (n == null) return;
                count = n; j++;
            } else if (opt.equals("JUSTID")) {
                justId = true;
            }
        }

        StreamCarrier.Consumer destCons = g.getOrCreateConsumer(destConsumer);
        long now = System.currentTimeMillis();
        List<Object> claimed = new ArrayList<>();
        String nextCursor = "0-0";
        int scanned = 0;
        for (java.util.Map.Entry<String, StreamCarrier.PelEntry> en : g.pending.entrySet()) {
            if (en.getValue().id.compareTo(start) < 0) continue;
            scanned++;
            if (scanned > count) { nextCursor = en.getKey(); break; }
            if (now - en.getValue().deliveryTime < minIdleTime) continue;
            StreamCarrier.PelEntry pe = en.getValue();
            StreamCarrier.Consumer prev = g.consumers.get(pe.consumer);
            if (prev != null) prev.pendingIds.remove(pe.id.toString());
            pe.consumer = destConsumer;
            pe.deliveryTime = now;
            pe.deliveryCount++;
            destCons.pendingIds.add(pe.id.toString());
            StreamCarrier.Entry e = s.get(pe.id.toString());
            if (e == null) continue;
            if (justId) claimed.add(pe.id.toString());
            else        claimed.add(entryToReply(e));
        }
        // Reply: [next-cursor, [claimed...], [deleted...]]
        List<Object> reply = new ArrayList<>(3);
        reply.add(nextCursor);
        reply.add(claimed);
        reply.add(new ArrayList<>()); // Phase 7: deleted-from-PEL list (we don't track)
        c.replyArray(reply);
    }

    public static void xinfoStream(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(2));
        if (s == null) { c.replyError("ERR no such key"); return; }
        List<Object> out = new ArrayList<>();
        out.add("length"); out.add((long) s.size());
        out.add("radix-tree-keys"); out.add(1L);
        out.add("radix-tree-nodes"); out.add(2L);
        out.add("groups"); out.add((long) s.groups().size());
        out.add("last-generated_id"); out.add(s.lastId().toString());
        out.add("max-deleted_entry_id"); out.add(s.maxDeletedId() + "-0");
        out.add("entries-added"); out.add((long) s.size());
        out.add("recorded-first_entry_id"); out.add(s.isEmpty() ? "0-0" : s.range(
                new StreamCarrier.StreamId(0, 0), new StreamCarrier.StreamId(Long.MAX_VALUE, Long.MAX_VALUE), 1, false)
                .get(0).id.toString());
        c.replyArray(out);
    }

    public static void xinfoGroups(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(2));
        if (s == null) { c.replyError("ERR no such key"); return; }
        List<Object> out = new ArrayList<>();
        for (StreamCarrier.ConsumerGroup g : s.groups()) {
            List<Object> row = new ArrayList<>();
            row.add("name"); row.add(g.name);
            row.add("consumers"); row.add((long) g.consumers.size());
            row.add("pending"); row.add((long) g.pending.size());
            row.add("last-delivered_id"); row.add(g.lastDeliveredId.toString());
            out.add(row);
        }
        c.replyArray(out);
    }

    public static void xinfoConsumers(RedisClient c) {
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(2));
        if (s == null) { c.replyError("ERR no such key"); return; }
        StreamCarrier.ConsumerGroup g = s.getGroup(c.arg(3));
        if (g == null) { c.replyError("NOGROUP No such key or consumer group"); return; }
        List<Object> out = new ArrayList<>();
        for (StreamCarrier.Consumer cons : g.consumers.values()) {
            List<Object> row = new ArrayList<>();
            row.add("name"); row.add(cons.name);
            row.add("pending"); out_addL(row, cons.pendingIds.size());
            row.add("idle"); out_addL(row, System.currentTimeMillis() - cons.activeTime);
            out.add(row);
        }
        c.replyArray(out);
    }

    private static void out_addL(List<Object> out, long v) { out.add(v); }

    // ===== XGROUP =====

    public static void xgroup(RedisClient c) {
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        String sub = c.arg(1).toUpperCase();
        String key = c.arg(2);
        String group = c.arg(3);
        switch (sub) {
            case "CREATE": {
                if (c.argc() < 5) { c.replyWrongArgc(); return; }
                String idArg = c.arg(4);
                boolean mkstream = c.argc() >= 6 && c.arg(5).equalsIgnoreCase("MKSTREAM");
                StreamCarrier s = mkstream ? getOrCreateStream(c, key, false) : getStream(c, key);
                if (s == null) { c.replyError("ERR The XGROUP subcommand requires the key to exist"); return; }
                if (s.getGroup(group) != null) {
                    c.replyError("BUSYGROUP Consumer Group name already exists"); return;
                }
                StreamCarrier.StreamId id = idArg.equals("$") ? s.lastId() : StreamCarrier.StreamId.parse(idArg);
                if (id == null) { c.replyError("ERR Invalid stream ID"); return; }
                s.getOrCreateGroup(group).lastDeliveredId = id;
                c.replyOK();
                return;
            }
            case "SETID": {
                if (c.argc() != 5) { c.replyWrongArgc(); return; }
                StreamCarrier s = getStream(c, key);
                if (s == null) { c.replyError("NOGROUP No such key"); return; }
                StreamCarrier.ConsumerGroup g = s.getGroup(group);
                if (g == null) { c.replyError("NOGROUP No such consumer group"); return; }
                String idArg = c.arg(4);
                StreamCarrier.StreamId id = idArg.equals("$") ? s.lastId() : StreamCarrier.StreamId.parse(idArg);
                if (id == null) { c.replyError("ERR Invalid stream ID"); return; }
                g.lastDeliveredId = id;
                c.replyOK();
                return;
            }
            case "DESTROY": {
                StreamCarrier s = getStream(c, key);
                if (s == null) { c.replyInteger(0); return; }
                boolean ok = s.destroyGroup(group);
                c.replyInteger(ok ? 1 : 0);
                return;
            }
            case "CREATECONSUMER": {
                StreamCarrier s = getStream(c, key);
                if (s == null) { c.replyError("NOGROUP No such key"); return; }
                StreamCarrier.ConsumerGroup g = s.getGroup(group);
                if (g == null) { c.replyError("NOGROUP No such consumer group"); return; }
                if (c.argc() < 5) { c.replyWrongArgc(); return; }
                String consumer = c.arg(4);
                if (g.consumers.containsKey(consumer)) { c.replyInteger(0); return; }
                g.getOrCreateConsumer(consumer);
                c.replyInteger(1);
                return;
            }
            case "DELCONSUMER": {
                StreamCarrier s = getStream(c, key);
                if (s == null) { c.replyError("NOGROUP No such key"); return; }
                StreamCarrier.ConsumerGroup g = s.getGroup(group);
                if (g == null) { c.replyError("NOGROUP No such consumer group"); return; }
                if (c.argc() < 5) { c.replyWrongArgc(); return; }
                String consumer = c.arg(4);
                StreamCarrier.Consumer removed = g.consumers.remove(consumer);
                c.replyInteger(removed == null ? 0 : removed.pendingIds.size());
                return;
            }
            case "HELP":
                c.replyArray(Arrays.asList(
                        "XGROUP <subcommand> [<arg> ...]. Subcommands are:",
                        "CREATE <key> <group> <id|$> [MKSTREAM]",
                        "SETID <key> <group> <id|$>",
                        "DESTROY <key> <group>",
                        "CREATECONSUMER <key> <group> <consumer>",
                        "DELCONSUMER <key> <group> <consumer>",
                        "HELP"));
                return;
            default:
                c.replyError("ERR Unknown XGROUP subcommand: " + sub);
        }
    }

    // ===== XREAD =====

    public static void xread(RedisClient c) { xreadImpl(c, false); }
    public static void xreadgroup(RedisClient c) { xreadImpl(c, true); }

    private static void xreadImpl(RedisClient c, boolean fromGroup) {
        // XREAD [COUNT count] [BLOCK ms] STREAMS key [key...] id [id...]
        // XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key...] id [id...]
        int j = 1;
        String groupName = null, consumerName = null;
        boolean noack = false;
        if (fromGroup) {
            if (c.argc() < 7 || !c.arg(j).equalsIgnoreCase("GROUP")) { c.replySyntaxError(); return; }
            groupName = c.arg(2);
            consumerName = c.arg(3);
            j = 4;
        }
        int count = -1;
        long blockMs = -1;
        while (j < c.argc()) {
            String opt = c.arg(j).toUpperCase();
            if (opt.equals("COUNT") && j + 1 < c.argc()) {
                Integer n = Args.parseInt(c, j + 1);
                if (n == null) return;
                count = n;
                j += 2;
            } else if (opt.equals("BLOCK") && j + 1 < c.argc()) {
                Long n = Args.parseLong(c, j + 1);
                if (n == null) return;
                blockMs = n;
                j += 2;
            } else if (opt.equals("NOACK")) {
                noack = true;
                j++;
            } else if (opt.equals("STREAMS")) {
                j++;
                break;
            } else {
                c.replySyntaxError(); return;
            }
        }

        int numKeys = (c.argc() - j) / 2;
        if (numKeys < 1 || j + numKeys * 2 != c.argc()) { c.replySyntaxError(); return; }
        String[] keys = new String[numKeys];
        String[] ids = new String[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = c.arg(j + i);
            ids[i] = c.arg(j + numKeys + i);
        }

        List<Object> out = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            StreamCarrier s = getStream(c, keys[i]);
            if (s == null) continue;
            // Note: ">" is a valid XREADGROUP id meaning "new messages since last-delivered".
            // We don't parse it as a stream-id here; it's resolved inside the group branch.
            StreamCarrier.StreamId fromId = null;
            if (!ids[i].equals(">")) {
                if (ids[i].equals("$")) fromId = s.lastId();
                else fromId = StreamCarrier.StreamId.parse(ids[i]);
                if (fromId == null) { c.replyError("ERR Invalid stream ID"); return; }
            }

            List<StreamCarrier.Entry> entries;
            StreamCarrier.ConsumerGroup g = null;
            StreamCarrier.Consumer cons = null;
            if (fromGroup) {
                g = s.getGroup(groupName);
                if (g == null) { c.replyError("NOGROUP No such consumer group '" + groupName + "' for key name '" + keys[i] + "'"); return; }
                cons = g.getOrCreateConsumer(consumerName);
                if (ids[i].equals(">")) {
                    // Read new messages: id > last-delivered-id
                    fromId = g.lastDeliveredId;
                    entries = s.readAfter(fromId, count);
                } else {
                    // Read history for this consumer (PEL).
                    entries = new ArrayList<>();
                    for (String pid : cons.pendingIds) {
                        StreamCarrier.Entry pe = s.get(pid);
                        if (pe != null) {
                            entries.add(pe);
                            if (count > 0 && entries.size() >= count) break;
                        }
                    }
                    if (entries.isEmpty()) continue;
                    appendStreamResult(out, keys[i], entries);
                    continue;
                }
            } else {
                entries = s.readAfter(fromId, count);
            }

            if (entries.isEmpty()) continue;

            // Update group state for new-message reads.
            if (fromGroup && g != null && cons != null) {
                for (StreamCarrier.Entry e : entries) {
                    if (!noack) {
                        g.pending.put(e.id.toString(),
                                new StreamCarrier.PelEntry(e.id, consumerName, System.currentTimeMillis(), 1));
                        cons.pendingIds.add(e.id.toString());
                    }
                    if (e.id.compareTo(g.lastDeliveredId) > 0) g.lastDeliveredId = e.id;
                }
                cons.activeTime = System.currentTimeMillis();
            }
            appendStreamResult(out, keys[i], entries);
        }

        if (out.isEmpty()) {
            // Block until new data arrives (Phase-4 minimal: if BLOCK is set, register a wakeup).
            if (blockMs > 0) {
                parkXRead(c, keys, blockMs, fromGroup, groupName, consumerName);
                return;
            }
            c.replyNil();
            return;
        }
        c.replyArray(out);
    }

    private static void appendStreamResult(List<Object> out, String key, List<StreamCarrier.Entry> entries) {
        List<Object> byKey = new ArrayList<>(2);
        byKey.add(key);
        List<Object> entryList = new ArrayList<>(entries.size());
        for (StreamCarrier.Entry e : entries) entryList.add(entryToReply(e));
        byKey.add(entryList);
        out.add(byKey);
    }

    private static Object entryToReply(StreamCarrier.Entry e) {
        List<Object> row = new ArrayList<>(2);
        row.add(e.id.toString());
        row.add(new ArrayList<>(e.fields));
        return row;
    }

    private static void parkXRead(RedisClient c, String[] keys, long blockMs,
                                   boolean fromGroup, String groupName, String consumerName) {
        io.github.redis.core.BlockingCoordinator bc = c.services().get(io.github.redis.core.BlockingCoordinator.class);
        if (bc == null) { c.replyNil(); return; }
        java.util.Set<String> waitedKeys = new java.util.HashSet<>(java.util.Arrays.asList(keys));
        // Capture argv for re-dispatch on wakeup.
        final String[] argv = new String[c.argc()];
        for (int i = 0; i < c.argc(); i++) argv[i] = c.arg(i);
        c.markBlocked();
        bc.park(waitedKeys, blockMs, timedOut -> {
            c.clearBlocked();
            if (timedOut) { c.replyNil(); return; }
            // Re-run this XREAD/XREADGROUP. It will either find new data and reply,
            // or re-park with remaining blockMs (Phase-4 simplification: re-parks with full blockMs).
            c.setCommand(argv, argv.length);
            if (argv[0].equalsIgnoreCase("XREAD")) xread(c);
            else                                    xreadgroup(c);
        });
    }

    // ===== XACK =====

    public static void xack(RedisClient c) {
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        if (s == null) { c.replyInteger(0); return; }
        StreamCarrier.ConsumerGroup g = s.getGroup(c.arg(2));
        if (g == null) { c.replyInteger(0); return; }
        long acked = 0;
        for (int j = 3; j < c.argc(); j++) {
            StreamCarrier.PelEntry pe = g.pending.remove(c.arg(j));
            if (pe == null) continue;
            acked++;
            StreamCarrier.Consumer cons = g.consumers.get(pe.consumer);
            if (cons != null) cons.pendingIds.remove(c.arg(j));
        }
        c.replyInteger(acked);
    }

    // ===== XPENDING =====

    public static void xpending(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        if (s == null) { c.replyInteger(0); return; }
        StreamCarrier.ConsumerGroup g = s.getGroup(c.arg(2));
        if (g == null) { c.replyInteger(0); return; }
        // Summary form: [count, min-id, max-id, [consumer, count]...]
        if (c.argc() == 3) {
            if (g.pending.isEmpty()) {
                List<Object> empty = new ArrayList<>();
                empty.add(0L);
                empty.add(null);
                empty.add(null);
                empty.add(new ArrayList<>());
                c.replyArray(empty);
                return;
            }
            List<Object> out = new ArrayList<>(4);
            out.add((long) g.pending.size());
            String minId = null, maxId = null;
            for (StreamCarrier.PelEntry pe : g.pending.values()) {
                String idStr = pe.id.toString();
                if (minId == null || idStr.compareTo(minId) < 0) minId = idStr;
                if (maxId == null || idStr.compareTo(maxId) > 0) maxId = idStr;
            }
            out.add(minId);
            out.add(maxId);
            // Per-consumer counts
            Map<String, Long> perConsumer = new LinkedHashMap<>();
            for (StreamCarrier.PelEntry pe : g.pending.values()) {
                perConsumer.merge(pe.consumer, 1L, Long::sum);
            }
            List<Object> consumerList = new ArrayList<>();
            for (Map.Entry<String, Long> e : perConsumer.entrySet()) {
                List<Object> row = new ArrayList<>();
                row.add(e.getKey());
                row.add(e.getValue());
                consumerList.add(row);
            }
            out.add(consumerList);
            c.replyArray(out);
            return;
        }
        // Detailed form: [id, consumer, idle, deliveries]
        // XPENDING key group [IDLE ms] start end count [consumer]
        int j = 3;
        long idleFilter = 0;
        if (c.argc() > j && c.arg(j).equalsIgnoreCase("IDLE")) {
            Long idle = Args.parseLong(c, j + 1);
            if (idle == null) return;
            idleFilter = idle;
            j += 2;
        }
        if (c.argc() < j + 3) { c.replySyntaxError(); return; }
        StreamCarrier.StreamId start = parseBound(c.arg(j), true);
        StreamCarrier.StreamId end = parseBound(c.arg(j + 1), false);
        Integer count = Args.parseInt(c, j + 2);
        if (start == null || end == null || count == null) return;
        j += 3;
        String filterConsumer = null;
        if (j < c.argc()) filterConsumer = c.arg(j);

        List<Object> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (StreamCarrier.PelEntry pe : g.pending.values()) {
            if (pe.id.compareTo(start) < 0) continue;
            if (pe.id.compareTo(end) > 0) continue;
            if (idleFilter > 0 && (now - pe.deliveryTime) < idleFilter) continue;
            if (filterConsumer != null && !filterConsumer.equals(pe.consumer)) continue;
            List<Object> row = new ArrayList<>();
            row.add(pe.id.toString());
            row.add(pe.consumer);
            out_addL(row, now - pe.deliveryTime);
            out_addL(row, pe.deliveryCount);
            out.add(row);
            if (out.size() >= count) break;
        }
        c.replyArray(out);
    }

    // ===== XTRIM =====

    public static void xtrim(RedisClient c) {
        // XTRIM key [MAXLEN [~|=] count | MINID [~|=] id]
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        StreamCarrier s = getStream(c, c.arg(1));
        if (s == null) { c.replyInteger(0); return; }
        String strategy = c.arg(2).toUpperCase();
        int j = 3;
        boolean approx = false;
        if (c.arg(j).equals("~") || c.arg(j).equals("=")) {
            approx = c.arg(j).equals("~");
            j++;
        }
        if (strategy.equals("MAXLEN")) {
            Integer n = Args.parseInt(c, j);
            if (n == null) return;
            int trimmed = s.trim(n, approx);
            c.replyInteger(trimmed);
        } else if (strategy.equals("MINID")) {
            StreamCarrier.StreamId minId = StreamCarrier.StreamId.parse(c.arg(j));
            if (minId == null) { c.replyError("ERR Invalid stream ID"); return; }
            int before = s.size();
            trimMinId(s, minId);
            c.replyInteger(before - s.size());
        } else {
            c.replySyntaxError();
        }
    }

    // ===== storage glue =====

    private static StreamCarrier getStream(RedisClient c, String key) {
        RedisValue v = c.storage().get(key);
        if (v == null) return null;
        if (v.getType() != ValueType.STREAM) throw new Values.WrongTypeException();
        return (StreamCarrier) v.getValue();
    }

    private static StreamCarrier getOrCreateStream(RedisClient c, String key, boolean noCreate) {
        RedisValue v = c.storage().get(key);
        if (v == null) {
            if (noCreate) return null;
            StreamCarrier s = new StreamCarrier();
            RedisValue rv = new RedisValue(s, ValueType.STREAM);
            rv.setEncoding(Encoding.LISTPACK);
            c.storage().set(key, rv);
            return s;
        }
        if (v.getType() != ValueType.STREAM) throw new Values.WrongTypeException();
        return (StreamCarrier) v.getValue();
    }
}
