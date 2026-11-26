package io.github.redis.persistence.rdb;

import io.github.redis.encoding.Encoding;
import io.github.redis.encoding.StreamCarrier;
import io.github.redis.storage.Storage;
import io.github.redis.storage.ValueType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * RDB file decoder. Reads the format produced by {@link RdbEncoder} and
 * reconstructs each storage. Supports STRING / LIST / SET / HASH / ZSET.
 */
public class RdbDecoder {

    private InputStream inputStream;
    private int currentDatabase = 0;

    public void decode(InputStream inputStream, Storage[] storages) throws IOException {
        this.inputStream = inputStream;
        readHeader();

        Long pendingExpire = null;
        while (true) {
            int opcode = inputStream.read();
            if (opcode == -1) break;

            if (opcode == RdbConstants.OPCODE_SELECTDB) {
                currentDatabase = readLength();
            } else if (opcode == RdbConstants.OPCODE_EOF) {
                readInt(); // checksum
                break;
            } else if (opcode == RdbConstants.EXPIRETIME_MS) {
                pendingExpire = readLong();
            } else if (opcode == RdbConstants.EXPIRETIME_SEC) {
                int secs = readInt();
                pendingExpire = System.currentTimeMillis() + secs * 1000L;
            } else {
                int type = opcode;
                readTypedValue(storages, type, pendingExpire);
                pendingExpire = null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readTypedValue(Storage[] storages, int type, Long expireTimeMillis) throws IOException {
        String key = readString();
        Long ttlArg = null;
        if (expireTimeMillis != null) {
            long ttl = expireTimeMillis - System.currentTimeMillis();
            if (ttl <= 0) return; // already expired
            ttlArg = ttl;
        }

        Storage storage = (currentDatabase >= 0 && currentDatabase < storages.length)
                ? storages[currentDatabase] : null;
        if (storage == null) {
            // Drain the rest of this entry to keep the stream aligned.
            skipValue(type);
            return;
        }

        switch (type) {
            case RdbConstants.TYPE_STRING: {
                String value = readString();
                storage.set(key, value, ValueType.STRING, ttlArg);
                return;
            }
            case RdbConstants.TYPE_LIST: {
                int count = readLength();
                LinkedList<String> list = new LinkedList<>();
                for (int i = 0; i < count; i++) list.addLast(readString());
                storage.set(key, list, ValueType.LIST, ttlArg);
                return;
            }
            case RdbConstants.TYPE_SET: {
                int count = readLength();
                Set<String> set = new HashSet<>();
                for (int i = 0; i < count; i++) set.add(readString());
                storage.set(key, set, ValueType.SET, ttlArg);
                return;
            }
            case RdbConstants.TYPE_HASH: {
                int count = readLength();
                Map<String, String> hash = new HashMap<>();
                for (int i = 0; i < count; i++) {
                    String field = readString();
                    String value = readString();
                    hash.put(field, value);
                }
                storage.set(key, hash, ValueType.HASH, ttlArg);
                return;
            }
            case RdbConstants.TYPE_ZSET: {
                int count = readLength();
                TreeMap<String, Double> zset = new TreeMap<>();
                for (int i = 0; i < count; i++) {
                    String member = readString();
                    Double score = Double.parseDouble(readString());
                    zset.put(member, score);
                }
                storage.set(key, zset, ValueType.SORTED_SET, ttlArg);
                return;
            }
            case RdbConstants.TYPE_STREAM: {
                int count = readLength();
                StreamCarrier s = new StreamCarrier();
                for (int i = 0; i < count; i++) {
                    String idStr = readString();
                    int nFields = readLength();
                    List<String> fields = new java.util.ArrayList<>(nFields);
                    for (int k = 0; k < nFields; k++) fields.add(readString());
                    StreamCarrier.StreamId id = StreamCarrier.StreamId.parse(idStr);
                    if (id != null) s.append(id, fields);
                }
                io.github.redis.storage.RedisValue rv =
                        new io.github.redis.storage.RedisValue(s, ValueType.STREAM);
                rv.setEncoding(Encoding.LISTPACK);
                storage.set(key, rv);
                return;
            }
            default:
                skipValue(type);
        }
    }

    /** Best-effort drain for unknown types (assumes a single string follows). */
    private void skipValue(int type) throws IOException {
        readString();
    }

    private void readHeader() throws IOException {
        byte[] magic = new byte[5];
        int read = inputStream.read(magic);
        if (read != 5 || !RdbConstants.RDB_MAGIC.equals(new String(magic, StandardCharsets.UTF_8))) {
            throw new IOException("Invalid RDB file: magic number mismatch");
        }
        byte[] version = new byte[4];
        if (inputStream.read(version) != 4) {
            throw new IOException("Invalid RDB file: version not found");
        }
    }

    private String readString() throws IOException {
        int length = readLength();
        byte[] bytes = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            int read = inputStream.read(bytes, totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected end of file while reading string");
            }
            totalRead += read;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private int readLength() throws IOException {
        int firstByte = inputStream.read();
        if (firstByte == -1) {
            throw new IOException("Unexpected end of file while reading length");
        }
        int encoding = (firstByte >> 6) & 0x03;
        if (encoding == 0) {
            return firstByte & 0x3F;
        } else if (encoding == 1) {
            int secondByte = inputStream.read();
            if (secondByte == -1) {
                throw new IOException("Unexpected end of file while reading length");
            }
            return ((firstByte & 0x3F) << 8) | secondByte;
        } else {
            return readInt();
        }
    }

    private int readInt() throws IOException {
        int b1 = inputStream.read();
        int b2 = inputStream.read();
        int b3 = inputStream.read();
        int b4 = inputStream.read();
        if (b1 == -1 || b2 == -1 || b3 == -1 || b4 == -1) {
            throw new IOException("Unexpected end of file while reading int");
        }
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    private long readLong() throws IOException {
        long b1 = inputStream.read();
        long b2 = inputStream.read();
        long b3 = inputStream.read();
        long b4 = inputStream.read();
        long b5 = inputStream.read();
        long b6 = inputStream.read();
        long b7 = inputStream.read();
        long b8 = inputStream.read();
        if (b1 == -1 || b2 == -1 || b3 == -1 || b4 == -1 ||
                b5 == -1 || b6 == -1 || b7 == -1 || b8 == -1) {
            throw new IOException("Unexpected end of file while reading long");
        }
        return (b1 << 56) | (b2 << 48) | (b3 << 40) | (b4 << 32) |
                (b5 << 24) | (b6 << 16) | (b7 << 8) | b8;
    }
}
