package io.github.redis.persistence.aof;

import io.github.redis.persistence.rdb.StorageSnapshot;
import io.github.redis.storage.ValueType;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AOF rewriter.
 *
 * <p>Produces a minimal AOF representation of a {@link StorageSnapshot} by emitting
 * the smallest set of commands needed to reconstruct the state. For each entry:
 * <ul>
 *   <li>STRING → {@code SET key value}</li>
 *   <li>LIST   → {@code RPUSH key e1 e2 e3}</li>
 *   <li>SET    → {@code SADD key m1 m2 m3}</li>
 *   <li>HASH   → {@code HSET key f1 v1 f2 v2}</li>
 *   <li>ZSET   → {@code ZADD key s1 m1 s2 m2}</li>
 *   <li>TTL    → appended as {@code PEXPIREAT key millis}</li>
 * </ul>
 *
 * <p>All tokens are RESP-encoded as bulk strings to support binary-safe keys/values.
 */
public final class AofRewriter {

    private AofRewriter() {}

    /** Encode and write the canonical AOF for the given snapshot. */
    public static void write(OutputStream out, StorageSnapshot snap) throws IOException {
        BufferedWriter w = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        // SELECT per-DB preamble is implied by interleaving SELECT commands.
        for (int db = 0; db < snap.getDbCount(); db++) {
            List<StorageSnapshot.Entry> entries = snap.entriesFor(db);
            if (entries == null || entries.isEmpty()) continue;

            writeArray(w, "SELECT", Integer.toString(db));
            for (StorageSnapshot.Entry e : entries) {
                writeEntry(w, e);
            }
        }
        w.flush();
    }

    @SuppressWarnings("unchecked")
    private static void writeEntry(BufferedWriter w, StorageSnapshot.Entry e) throws IOException {
        String key = e.key;
        Object value = e.value;
        switch (e.type) {
            case STRING:
                writeArray(w, "SET", key, value == null ? "" : value.toString());
                break;
            case LIST:
                if (value instanceof List) {
                    List<String> l = (List<String>) value;
                    if (l.isEmpty()) break;
                    String[] args = new String[l.size() + 1];
                    args[0] = key;
                    for (int i = 0; i < l.size(); i++) args[i + 1] = l.get(i);
                    writeArray(w, prepend("RPUSH", args));
                }
                break;
            case SET:
                if (value instanceof io.github.redis.encoding.IntSet) {
                    io.github.redis.encoding.IntSet is = (io.github.redis.encoding.IntSet) value;
                    if (is.isEmpty()) break;
                    long[] arr = is.toArray();
                    String[] args = new String[arr.length + 1];
                    args[0] = key;
                    for (int i = 0; i < arr.length; i++) args[i + 1] = Long.toString(arr[i]);
                    writeArray(w, prepend("SADD", args));
                } else if (value instanceof Set) {
                    Set<String> s = (Set<String>) value;
                    if (s.isEmpty()) break;
                    String[] args = new String[s.size() + 1];
                    args[0] = key;
                    int i = 1;
                    for (String m : s) args[i++] = m;
                    writeArray(w, prepend("SADD", args));
                }
                break;
            case HASH:
                if (value instanceof io.github.redis.encoding.Listpack) {
                    io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) value;
                    if (lp.isEmpty()) break;
                    String[] args = new String[lp.size() * 2 + 1];
                    args[0] = key;
                    int i = 1;
                    for (io.github.redis.encoding.Listpack.Entry en : lp.all()) {
                        io.github.redis.encoding.Listpack.FieldValue fv = (io.github.redis.encoding.Listpack.FieldValue) en;
                        args[i++] = fv.field;
                        args[i++] = fv.value;
                    }
                    writeArray(w, prepend("HSET", args));
                } else if (value instanceof Map) {
                    Map<String, String> h = (Map<String, String>) value;
                    if (h.isEmpty()) break;
                    String[] args = new String[h.size() * 2 + 1];
                    args[0] = key;
                    int i = 1;
                    for (Map.Entry<String, String> en : h.entrySet()) {
                        args[i++] = en.getKey();
                        args[i++] = en.getValue();
                    }
                    writeArray(w, prepend("HSET", args));
                }
                break;
            case SORTED_SET:
                if (value instanceof io.github.redis.encoding.Listpack) {
                    io.github.redis.encoding.Listpack lp = (io.github.redis.encoding.Listpack) value;
                    if (lp.isEmpty()) break;
                    String[] args = new String[lp.size() * 2 + 1];
                    args[0] = key;
                    int i = 1;
                    for (io.github.redis.encoding.Listpack.Entry en : lp.all()) {
                        io.github.redis.encoding.Listpack.ScoreMember sm = (io.github.redis.encoding.Listpack.ScoreMember) en;
                        args[i++] = formatScore(sm.score);
                        args[i++] = sm.member;
                    }
                    writeArray(w, prepend("ZADD", args));
                } else if (value instanceof Map) {
                    Map<String, Double> z = (Map<String, Double>) value;
                    if (z.isEmpty()) break;
                    String[] args = new String[z.size() * 2 + 1];
                    args[0] = key;
                    int i = 1;
                    for (Map.Entry<String, Double> en : z.entrySet()) {
                        args[i++] = formatScore(en.getValue());
                        args[i++] = en.getKey();
                    }
                    writeArray(w, prepend("ZADD", args));
                }
                break;
            case STREAM: {
                io.github.redis.encoding.StreamCarrier s = (io.github.redis.encoding.StreamCarrier) value;
                if (s.isEmpty()) break;
                for (io.github.redis.encoding.StreamCarrier.Entry se : s.range(
                        new io.github.redis.encoding.StreamCarrier.StreamId(0, 0),
                        new io.github.redis.encoding.StreamCarrier.StreamId(Long.MAX_VALUE, Long.MAX_VALUE),
                        Integer.MAX_VALUE, false)) {
                    String[] args = new String[se.fields.size() + 2];
                    args[0] = key;
                    args[1] = se.id.toString();
                    for (int i = 0; i < se.fields.size(); i++) args[i + 2] = se.fields.get(i);
                    writeArray(w, prepend("XADD", args));
                }
                break;
            }
            default:
                break;
        }
        if (e.expireTimeMillis != null) {
            writeArray(w, "PEXPIREAT", key, Long.toString(e.expireTimeMillis));
        }
    }

    private static String[] prepend(String head, String[] tail) {
        String[] out = new String[tail.length + 1];
        out[0] = head;
        System.arraycopy(tail, 0, out, 1, tail.length);
        return out;
    }

    private static void writeArray(BufferedWriter w, String... tokens) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(tokens.length).append("\r\n");
        for (String t : tokens) {
            byte[] bytes = t.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(bytes.length).append("\r\n");
            sb.append(t).append("\r\n");
        }
        w.write(sb.toString());
    }

    private static String formatScore(double d) {
        if (d == Math.rint(d) && !Double.isInfinite(d)) return Long.toString((long) d);
        return Double.toString(d);
    }
}
