package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.storage.ValueType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Bitmap commands backed by STRING values (which we treat as raw UTF-8 bytes).
 *
 * <p>Implemented: GETBIT, SETBIT, BITCOUNT, BITPOS, BITOP (AND/OR/XOR/NOT),
 * BITFIELD (basic signed/unsigned GET/SET/INCRBY).
 *
 * <p>Storage: bitmap operations store the bitmap as a String whose bytes ARE
 * the bits. We always read &amp; write the value as raw UTF-8-decoded bytes;
 * mutations create a new String from the modified byte array. Binary safety
 * holds because the encoding is round-trip stable for any byte sequence...
 * <p><b>Caveat:</b> Java String round-tripping arbitrary byte sequences via
 * UTF-8 <i>is not safe</i>. Strings containing invalid UTF-8 byte sequences
 * will be mangled. For Phase-2 correctness we accept this limitation — Phase
 * 3 will replace String storage with byte[] (matching Redis's sds).
 */
public final class BitmapCommands {

    private BitmapCommands() {}

    // ===== GETBIT key offset =====

    public static void getbit(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Long offset = Args.parseLong(c, 2);
        if (offset == null) return;
        if (offset < 0) { c.replyError("ERR bit offset is not an integer or out of range"); return; }

        byte[] bytes = getBytes(c, c.arg(1));
        if (bytes == null) { c.replyInteger(0); return; }
        int byteIdx = (int) (offset >>> 3);
        if (byteIdx >= bytes.length) { c.replyInteger(0); return; }
        int bitIdx = 7 - (int) (offset & 7);
        c.replyInteger((bytes[byteIdx] >>> bitIdx) & 1);
    }

    // ===== SETBIT key offset value =====

    public static void setbit(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Long offset = Args.parseLong(c, 2);
        if (offset == null) return;
        if (offset < 0) { c.replyError("ERR bit offset is not an integer or out of range"); return; }
        String v = c.arg(3);
        int bit;
        if (v.equals("0")) bit = 0;
        else if (v.equals("1")) bit = 1;
        else { c.replyError("ERR bit is not an integer or out of range"); return; }

        byte[] bytes = getBytes(c, c.arg(1));
        if (bytes == null) bytes = new byte[0];
        int byteIdx = (int) (offset >>> 3);
        int bitIdx = 7 - (int) (offset & 7);
        if (byteIdx >= bytes.length) {
            byte[] grown = new byte[byteIdx + 1];
            System.arraycopy(bytes, 0, grown, 0, bytes.length);
            bytes = grown;
        }
        int old = (bytes[byteIdx] >>> bitIdx) & 1;
        if (bit == 1) bytes[byteIdx] |= (1 << bitIdx);
        else          bytes[byteIdx] &= ~(1 << bitIdx);

        // Preserve TTL
        long ttl = c.storage().ttl(c.arg(1));
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(c.arg(1), new String(bytes, StandardCharsets.ISO_8859_1), ValueType.STRING, ttlArg);
        c.replyInteger(old);
    }

    // ===== BITCOUNT key [start end [BYTE|BIT]] =====

    public static void bitcount(RedisClient c) {
        if (c.argc() < 2 || c.argc() > 5) { c.replyWrongArgc(); return; }
        byte[] bytes = getBytes(c, c.arg(1));
        if (bytes == null || bytes.length == 0) { c.replyInteger(0); return; }

        if (c.argc() == 2) {
            c.replyInteger(popcount(bytes, 0, bytes.length));
            return;
        }
        // start end [BYTE|BIT]
        Integer start = Args.parseInt(c, 2);
        Integer end   = Args.parseInt(c, 3);
        if (start == null || end == null) return;
        boolean bitMode = false;
        if (c.argc() == 5) {
            String mode = c.arg(4).toUpperCase();
            if (!mode.equals("BIT") && !mode.equals("BYTE")) { c.replySyntaxError(); return; }
            bitMode = mode.equals("BIT");
        }
        int unit = bitMode ? 1 : 8;
        int total = bytes.length * unit;
        int s = normalizeIndex(start, total);
        int e = normalizeIndex(end, total);
        if (s < 0) s = 0;
        if (e >= total) e = total - 1;
        if (s > e) { c.replyInteger(0); return; }

        if (bitMode) {
            int fromByte = s >>> 3, fromBit = 7 - (s & 7);
            int toByte   = e >>> 3, toBit   = 7 - (e & 7);
            long count = 0;
            for (int b = fromByte; b <= toByte; b++) {
                if (b >= bytes.length) break;
                int lo = (b == fromByte) ? (fromBit) : 7;
                int hi = (b == toByte)   ? (toBit)   : 0;
                for (int i = hi; i <= lo; i++) {
                    if ((bytes[b] >>> i & 1) == 1) count++;
                }
            }
            c.replyInteger(count);
        } else {
            c.replyInteger(popcount(bytes, s, e + 1));
        }
    }

    // ===== BITPOS key bit [start [end [BYTE|BIT]]] =====

    public static void bitpos(RedisClient c) {
        if (c.argc() < 3 || c.argc() > 6) { c.replyWrongArgc(); return; }
        int target;
        String t = c.arg(2);
        if (t.equals("0")) target = 0;
        else if (t.equals("1")) target = 1;
        else { c.replyError("ERR The bit argument must be 1 or 0."); return; }

        byte[] bytes = getBytes(c, c.arg(1));
        if (bytes == null) {
            // Empty key: bit 0 → 0, bit 1 → -1
            c.replyInteger(target == 0 ? 0 : -1);
            return;
        }
        if (bytes.length == 0) {
            c.replyInteger(target == 0 ? 0 : -1);
            return;
        }

        int start = 0, end = bytes.length - 1;
        boolean bitMode = false;
        if (c.argc() >= 4) {
            Integer s = Args.parseInt(c, 3);
            if (s == null) return;
            start = s;
        }
        if (c.argc() >= 5) {
            Integer e = Args.parseInt(c, 4);
            if (e == null) return;
            end = e;
        }
        if (c.argc() == 6) {
            String mode = c.arg(5).toUpperCase();
            if (!mode.equals("BIT") && !mode.equals("BYTE")) { c.replySyntaxError(); return; }
            bitMode = mode.equals("BIT");
        }
        int total = bytes.length * (bitMode ? 1 : 8);
        if (!bitMode) { start *= 8; end = end * 8 + 7; }
        start = normalizeIndex(start / (bitMode ? 1 : 1), total) / (bitMode ? 1 : 1);
        if (start < 0) start = 0;
        // Simpler & correct: walk bits linearly.
        for (int i = (start < 0 ? 0 : start); i <= end && i < total; i++) {
            int byteIdx = i >>> 3;
            int bitIdx  = 7 - (i & 7);
            int bit = byteIdx < bytes.length ? ((bytes[byteIdx] >>> bitIdx) & 1) : 0;
            if (bit == target) { c.replyInteger(i); return; }
        }
        c.replyInteger(-1);
    }

    // ===== BITOP operation destkey key [key ...] =====

    public static void bitop(RedisClient c) {
        if (c.argc() < 4) { c.replyWrongArgc(); return; }
        String op = c.arg(1).toUpperCase();
        String dest = c.arg(2);
        boolean isNot = op.equals("NOT");
        if (isNot && c.argc() != 4) { c.replyError("ERR BITOP NOT must be called with a single source key."); return; }
        if (!op.equals("AND") && !op.equals("OR") && !op.equals("XOR") && !isNot) {
            c.replySyntaxError(); return;
        }

        byte[][] sources = new byte[c.argc() - 3][];
        int maxLen = 0;
        for (int i = 0; i < sources.length; i++) {
            byte[] b = getBytes(c, c.arg(3 + i));
            sources[i] = b == null ? new byte[0] : b;
            if (sources[i].length > maxLen) maxLen = sources[i].length;
        }
        if (isNot) {
            byte[] result = new byte[sources[0].length];
            for (int i = 0; i < sources[0].length; i++) result[i] = (byte) ~sources[0][i];
            saveBytes(c, dest, result);
            c.replyInteger(result.length);
            return;
        }
        byte[] result = new byte[maxLen];
        for (int i = 0; i < maxLen; i++) {
            byte acc = (op.equals("AND")) ? (byte) 0xFF : 0;
            for (byte[] src : sources) {
                byte b = (i < src.length) ? src[i] : 0;
                if (op.equals("AND")) acc &= b;
                else if (op.equals("OR")) acc |= b;
                else acc ^= b;
            }
            result[i] = acc;
        }
        saveBytes(c, dest, result);
        c.replyInteger(result.length);
    }

    // ===== BITFIELD key [GET ...] [SET ...] [INCRBY ...] [OVERFLOW WRAP|SAT|FAIL] =====

    public static void bitfield(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String key = c.arg(1);
        byte[] bytes = getBytes(c, key);
        if (bytes == null) bytes = new byte[0];

        List<Object> results = new ArrayList<>();
        OverflowMode overflow = OverflowMode.WRAP;
        byte[] cur = bytes;

        for (int j = 2; j < c.argc(); j++) {
            String op = c.arg(j).toUpperCase();
            if (op.equals("OVERFLOW")) {
                if (++j >= c.argc()) { c.replySyntaxError(); return; }
                String mode = c.arg(j).toUpperCase();
                if (mode.equals("WRAP")) overflow = OverflowMode.WRAP;
                else if (mode.equals("SAT")) overflow = OverflowMode.SAT;
                else if (mode.equals("FAIL")) overflow = OverflowMode.FAIL;
                else { c.replySyntaxError(); return; }
                continue;
            }
            if (++j >= c.argc()) { c.replySyntaxError(); return; }
            String typeStr = c.arg(j);
            BitFieldType type = BitFieldType.parse(typeStr);
            if (type == null) { c.replyError("ERR Invalid bitfield type. Use something like i16 or u8"); return; }
            if (++j >= c.argc()) { c.replySyntaxError(); return; }
            Long offset = parseOffset(c.arg(j), type);
            if (offset == null) { c.replyError("ERR bit offset is not an integer or out of range"); return; }

            switch (op) {
                case "GET": {
                    Long v = readBits(cur, type, offset);
                    results.add(v);
                    break;
                }
                case "SET": {
                    if (++j >= c.argc()) { c.replySyntaxError(); return; }
                    Long value = Args.parseLong(c, j);
                    if (value == null) return;
                    Long prev = readBits(cur, type, offset);
                    cur = writeBits(cur, type, offset, value);
                    results.add(prev);
                    break;
                }
                case "INCRBY": {
                    if (++j >= c.argc()) { c.replySyntaxError(); return; }
                    Long delta = Args.parseLong(c, j);
                    if (delta == null) return;
                    Long prev = readBits(cur, type, offset);
                    long newVal;
                    try {
                        newVal = applyOverflow(prev, delta, type, overflow);
                    } catch (ArithmeticException ex) {
                        results.add(null); // OVERFLOW FAIL
                        break;
                    }
                    cur = writeBits(cur, type, offset, newVal);
                    results.add(newVal);
                    break;
                }
                default:
                    c.replyError("ERR syntax error"); return;
            }
        }
        saveBytes(c, key, cur);
        c.replyArray(results);
    }

    // ===== helpers =====

    private static byte[] getBytes(RedisClient c, String key) {
        try {
            String v = Values.getString(c.storage(), key);
            if (v == null) return null;
            return v.getBytes(StandardCharsets.ISO_8859_1);
        } catch (Values.WrongTypeException wte) {
            throw new RuntimeException(wte.getMessage());
        }
    }

    private static void saveBytes(RedisClient c, String key, byte[] bytes) {
        long ttl = c.storage().ttl(key);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(key, new String(bytes, StandardCharsets.ISO_8859_1), ValueType.STRING, ttlArg);
    }

    private static long popcount(byte[] bytes, int from, int to) {
        long count = 0;
        for (int i = from; i < to && i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            // Standard byte popcount: SWAR within one byte.
            b = b - ((b >>> 1) & 0x55);
            b = (b & 0x33) + ((b >>> 2) & 0x33);
            b = (b + (b >>> 4)) & 0x0F;
            count += b;  // 0..8 per byte
        }
        return count;
    }

    private static int normalizeIndex(int idx, int total) {
        return idx < 0 ? total + idx : idx;
    }

    private enum OverflowMode { WRAP, SAT, FAIL }

    private static final class BitFieldType {
        final boolean signed;
        final int width;
        BitFieldType(boolean s, int w) { signed = s; width = w; }
        static BitFieldType parse(String s) {
            if (s == null || s.length() < 2) return null;
            char c = Character.toLowerCase(s.charAt(0));
            boolean signed;
            if (c == 'i') signed = true;
            else if (c == 'u') signed = false;
            else return null;
            try {
                int w = Integer.parseInt(s.substring(1));
                if (w < 1 || w > 64) return null;
                if (!signed && w > 63) return null;
                return new BitFieldType(signed, w);
            } catch (NumberFormatException e) { return null; }
        }
    }

    private static Long parseOffset(String s, BitFieldType type) {
        // Supports "#N" (type-width based) and numeric.
        if (s.startsWith("#")) {
            try { return (long) Integer.parseInt(s.substring(1)) * type.width; }
            catch (NumberFormatException e) { return null; }
        }
        try { return Long.parseLong(s); }
        catch (NumberFormatException e) { return null; }
    }

    private static long bitMask(int width) {
        return width == 64 ? -1L : (1L << width) - 1;
    }

    private static Long readBits(byte[] bytes, BitFieldType type, long bitOffset) {
        int totalBits = bytes.length * 8;
        if (bitOffset + type.width > totalBits) {
            // Treat missing high bits as zero.
            long val = 0;
            for (int i = 0; i < type.width; i++) {
                long pos = bitOffset + i;
                if (pos < 0 || pos >= totalBits) continue;
                int byteIdx = (int) (pos >>> 3);
                int bitIdx  = 7 - (int) (pos & 7);
                if (((bytes[byteIdx] >>> bitIdx) & 1) == 1) val |= (1L << (type.width - 1 - i));
            }
            return type.signed ? signExtend(val, type.width) : val;
        }
        long val = 0;
        for (int i = 0; i < type.width; i++) {
            long pos = bitOffset + i;
            int byteIdx = (int) (pos >>> 3);
            int bitIdx  = 7 - (int) (pos & 7);
            if (((bytes[byteIdx] >>> bitIdx) & 1) == 1) val |= (1L << (type.width - 1 - i));
        }
        return type.signed ? signExtend(val, type.width) : val;
    }

    private static long signExtend(long val, int width) {
        if (width >= 64) return val;
        long signBit = 1L << (width - 1);
        if ((val & signBit) != 0) {
            return val | ~bitMask(width);
        }
        return val;
    }

    private static byte[] writeBits(byte[] bytes, BitFieldType type, long bitOffset, long value) {
        long needBits = bitOffset + type.width;
        int needBytes = (int) ((needBits + 7) >>> 3);
        byte[] out;
        if (needBytes > bytes.length) {
            out = new byte[needBytes];
            System.arraycopy(bytes, 0, out, 0, bytes.length);
        } else {
            out = bytes.clone();
        }
        // Mask the value first.
        long masked = value & bitMask(type.width);
        // Write MSB-first.
        for (int i = 0; i < type.width; i++) {
            long pos = bitOffset + i;
            int byteIdx = (int) (pos >>> 3);
            int bitIdx  = 7 - (int) (pos & 7);
            boolean set = ((masked >>> (type.width - 1 - i)) & 1) != 0;
            if (set) out[byteIdx] |= (1 << bitIdx);
            else     out[byteIdx] &= ~(1 << bitIdx);
        }
        return out;
    }

    private static long applyOverflow(long current, long delta, BitFieldType type, OverflowMode mode) {
        long result = current + delta;
        long max, min;
        if (type.signed) {
            if (type.width >= 64) return result; // no overflow possible
            max = (1L << (type.width - 1)) - 1;
            min = -(1L << (type.width - 1));
        } else {
            if (type.width >= 64) return result;
            max = (1L << type.width) - 1;
            min = 0;
        }
        if (result >= min && result <= max) return result;
        switch (mode) {
            case WRAP:
                long range = max - min + 1;
                long mod = (result - min) % range;
                if (mod < 0) mod += range;
                return min + mod;
            case SAT:
                return result > max ? max : min;
            case FAIL:
                throw new ArithmeticException("overflow");
            default:
                return result;
        }
    }
}
