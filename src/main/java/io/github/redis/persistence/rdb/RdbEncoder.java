package io.github.redis.persistence.rdb;

import io.github.redis.encoding.IntSet;
import io.github.redis.encoding.Listpack;
import io.github.redis.encoding.StreamCarrier;
import io.github.redis.storage.ValueType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RDB file encoder.
 *
 * <p>Two entry points:
 * <ul>
 *   <li>{@link #encode(OutputStream, StorageSnapshot)} — preferred; serialises a
 *       precomputed snapshot taken atomically on the dispatcher thread.</li>
 *   <li>{@link #encode(OutputStream, io.github.redis.storage.Storage[])} — legacy;
 *       iterates live storages (kept for ad-hoc / SAVE-from-dispatcher paths).</li>
 * </ul>
 *
 * <p>Format (Phase 3): magic "REDIS0001" + per-DB {SELECTDB; entries...} + EOF.
 * Each entry is type-byte + length-prefixed key + type-specific payload.
 *
 * <p>Phase-3 carrier support: STRING / LIST (LinkedList) / SET (IntSet or HashSet)
 * / HASH (Listpack or HashMap) / ZSET (Listpack or TreeMap).
 */
public class RdbEncoder {

    public void encode(OutputStream out, io.github.redis.storage.Storage[] storages) throws IOException {
        encode(out, new StorageSnapshot(storages));
    }

    public void encode(OutputStream out, StorageSnapshot snapshot) throws IOException {
        writeHeader(out);
        for (int db = 0; db < snapshot.getDbCount(); db++) {
            List<StorageSnapshot.Entry> entries = snapshot.entriesFor(db);
            if (entries == null || entries.isEmpty()) continue;

            out.write(RdbConstants.OPCODE_SELECTDB);
            writeLength(out, db);

            for (StorageSnapshot.Entry e : entries) {
                writeEntry(out, e.key, e.value, e.type, e.expireTimeMillis);
            }
        }
        writeEof(out);
    }

    private void writeHeader(OutputStream out) throws IOException {
        out.write(RdbConstants.RDB_MAGIC.getBytes(StandardCharsets.UTF_8));
        out.write(RdbConstants.RDB_VERSION.getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("unchecked")
    private void writeEntry(OutputStream out, String key, Object value, ValueType type, Long expireTime) throws IOException {
        if (expireTime != null) {
            long ttl = expireTime - System.currentTimeMillis();
            if (ttl <= 0) return; // already expired; skip
            out.write(RdbConstants.EXPIRETIME_MS);
            writeLong(out, expireTime);
        }

        switch (type) {
            case STRING: {
                out.write(RdbConstants.TYPE_STRING);
                writeString(out, key);
                writeString(out, value instanceof String ? (String) value : String.valueOf(value));
                break;
            }
            case LIST: {
                out.write(RdbConstants.TYPE_LIST);
                writeString(out, key);
                List<String> list = (List<String>) value;
                writeLength(out, list.size());
                for (String elem : list) writeString(out, elem);
                break;
            }
            case SET: {
                out.write(RdbConstants.TYPE_SET);
                writeString(out, key);
                if (value instanceof IntSet) {
                    IntSet is = (IntSet) value;
                    long[] arr = is.toArray();
                    writeLength(out, arr.length);
                    for (long m : arr) writeString(out, Long.toString(m));
                } else {
                    Set<String> set = (Set<String>) value;
                    writeLength(out, set.size());
                    for (String member : set) writeString(out, member);
                }
                break;
            }
            case HASH: {
                out.write(RdbConstants.TYPE_HASH);
                writeString(out, key);
                if (value instanceof Listpack) {
                    Listpack lp = (Listpack) value;
                    writeLength(out, lp.size());
                    for (Listpack.Entry e : lp.all()) {
                        Listpack.FieldValue fv = (Listpack.FieldValue) e;
                        writeString(out, fv.field);
                        writeString(out, fv.value);
                    }
                } else {
                    Map<String, String> hash = (Map<String, String>) value;
                    writeLength(out, hash.size());
                    for (Map.Entry<String, String> e : hash.entrySet()) {
                        writeString(out, e.getKey());
                        writeString(out, e.getValue());
                    }
                }
                break;
            }
            case SORTED_SET: {
                out.write(RdbConstants.TYPE_ZSET);
                writeString(out, key);
                if (value instanceof Listpack) {
                    Listpack lp = (Listpack) value;
                    writeLength(out, lp.size());
                    for (Listpack.Entry e : lp.all()) {
                        Listpack.ScoreMember sm = (Listpack.ScoreMember) e;
                        writeString(out, sm.member);
                        writeString(out, Double.toString(sm.score));
                    }
                } else {
                    Map<String, Double> zset = (Map<String, Double>) value;
                    writeLength(out, zset.size());
                    for (Map.Entry<String, Double> e : zset.entrySet()) {
                        writeString(out, e.getKey());
                        writeString(out, Double.toString(e.getValue()));
                    }
                }
                break;
            }
            case STREAM: {
                out.write(RdbConstants.TYPE_STREAM);
                writeString(out, key);
                StreamCarrier s = (StreamCarrier) value;
                writeLength(out, s.size());
                for (StreamCarrier.Entry e : s.range(
                        new StreamCarrier.StreamId(0, 0),
                        new StreamCarrier.StreamId(Long.MAX_VALUE, Long.MAX_VALUE),
                        Integer.MAX_VALUE, false)) {
                    writeString(out, e.id.toString());
                    writeLength(out, e.fields.size());
                    for (String fv : e.fields) writeString(out, fv);
                }
                break;
            }
            default:
                return;
        }
    }

    private void writeString(OutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        writeLength(out, bytes.length);
        out.write(bytes);
    }

    private void writeLength(OutputStream out, int length) throws IOException {
        if (length < 64) {
            out.write(length);
        } else if (length < 16384) {
            out.write((length >> 8) | 0x40);
            out.write(length & 0xFF);
        } else {
            out.write(0x80);
            writeInt(out, length);
        }
    }

    private void writeInt(OutputStream out, int value) throws IOException {
        out.write((value >> 24) & 0xFF);
        out.write((value >> 16) & 0xFF);
        out.write((value >> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private void writeLong(OutputStream out, long value) throws IOException {
        out.write((int) ((value >> 56) & 0xFF));
        out.write((int) ((value >> 48) & 0xFF));
        out.write((int) ((value >> 40) & 0xFF));
        out.write((int) ((value >> 32) & 0xFF));
        out.write((int) ((value >> 24) & 0xFF));
        out.write((int) ((value >> 16) & 0xFF));
        out.write((int) ((value >> 8) & 0xFF));
        out.write((int) (value & 0xFF));
    }

    private void writeEof(OutputStream out) throws IOException {
        out.write(RdbConstants.OPCODE_EOF);
        writeInt(out, 0);
    }
}
