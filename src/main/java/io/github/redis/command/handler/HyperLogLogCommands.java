package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.storage.ValueType;

import java.util.Arrays;

/**
 * HyperLogLog commands. Stores the HLL register as a 12 KB byte[] in STRING
 * storage (12,288 bytes = 16384 6-bit registers packed into a byte array).
 *
 * <p>Algorithm: classic HLL with 16384 6-bit registers, biased error ~0.81%.
 * The encoding is a thin wrapper around Redis's sparse/dense layout — Phase 2
 * uses dense-only (simpler, less compact, but correct).
 *
 * <p>Implemented: PFADD, PFCOUNT, PFMERGE.
 */
public final class HyperLogLogCommands {

    private HyperLogLogCommands() {}

    /** Number of HLL registers (Redis's HLL_REGISTERS = 16384). */
    private static final int REGISTERS = 16384;
    /** Bits per register. */
    private static final int REG_BITS = 6;
    /** Magic header Redis uses ("HLL"). */
    private static final byte[] HEADER = new byte[] { 'H', 'Y', 'L', 'L' };

    public static void pfadd(RedisClient c) {
        // PFADD key [element [element ...]]
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String key = c.arg(1);
        byte[] hll = getOrCreateHll(c, key);

        boolean changed = false;
        for (int j = 2; j < c.argc(); j++) {
            if (mergeElement(hll, c.arg(j))) changed = true;
        }
        saveHll(c, key, hll);
        // No elements → return 0 (just initialize). Otherwise 1 if changed.
        if (c.argc() == 2) c.replyInteger(0);
        else c.replyInteger(changed ? 1 : 0);
    }

    public static void pfcount(RedisClient c) {
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        // Single key: read directly.
        if (c.argc() == 2) {
            byte[] hll = getHll(c, c.arg(1));
            if (hll == null) { c.replyInteger(0); return; }
            c.replyInteger(estimate(hll));
            return;
        }
        // Multiple keys: merge into a temp HLL, then count.
        byte[] merged = emptyHll();
        for (int j = 1; j < c.argc(); j++) {
            byte[] src = getHll(c, c.arg(j));
            if (src == null) continue;
            mergeInto(merged, src);
        }
        c.replyInteger(estimate(merged));
    }

    public static void pfmerge(RedisClient c) {
        // PFMERGE dest [src ...]
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String dest = c.arg(1);
        byte[] merged = getOrCreateHll(c, dest);
        for (int j = 2; j < c.argc(); j++) {
            byte[] src = getHll(c, c.arg(j));
            if (src == null) continue;
            mergeInto(merged, src);
        }
        saveHll(c, dest, merged);
        c.replyOK();
    }

    // ===== storage glue =====

    private static byte[] getOrCreateHll(RedisClient c, String key) {
        byte[] existing = getHll(c, key);
        return existing == null ? emptyHll() : existing;
    }

    private static byte[] getHll(RedisClient c, String key) {
        try {
            String v = Values.getString(c.storage(), key);
            if (v == null) return null;
            byte[] b = v.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
            return isValidHll(b) ? b : null;
        } catch (Values.WrongTypeException wte) {
            throw new RuntimeException(wte.getMessage());
        }
    }

    private static void saveHll(RedisClient c, String key, byte[] hll) {
        long ttl = c.storage().ttl(key);
        Long ttlArg = ttl > 0 ? ttl : null;
        c.storage().set(key, new String(hll, java.nio.charset.StandardCharsets.ISO_8859_1),
                ValueType.STRING, ttlArg);
    }

    private static boolean isValidHll(byte[] b) {
        return b != null && b.length >= HEADER.length + 1
                && b[0] == HEADER[0] && b[1] == HEADER[1] && b[2] == HEADER[2] && b[3] == HEADER[3];
    }

    // ===== HLL core =====

    private static byte[] emptyHll() {
        // Layout: 4-byte header "HYLL", 1-byte version, then packed registers.
        // 16384 6-bit registers = 12288 bytes (the last 4 bits are padding).
        byte[] out = new byte[HEADER.length + 1 + (REGISTERS * REG_BITS + 7) / 8];
        System.arraycopy(HEADER, 0, out, 0, HEADER.length);
        out[4] = 1; // version
        return out;
    }

    private static int getRegister(byte[] hll, int idx) {
        long bitOffset = (HEADER.length + 1) * 8L + idx * REG_BITS;
        int byteIdx = (int) (bitOffset >>> 3);
        int bitInByte = (int) (bitOffset & 7);
        int b1 = hll[byteIdx] & 0xFF;
        int b2 = (byteIdx + 1 < hll.length) ? (hll[byteIdx + 1] & 0xFF) : 0;
        int b3 = (byteIdx + 2 < hll.length) ? (hll[byteIdx + 2] & 0xFF) : 0;
        int packed = (b1 << 16) | (b2 << 8) | b3;
        return (packed >>> (24 - REG_BITS - bitInByte)) & ((1 << REG_BITS) - 1);
    }

    private static void setRegister(byte[] hll, int idx, int value) {
        long bitOffset = (HEADER.length + 1) * 8L + idx * REG_BITS;
        int byteIdx = (int) (bitOffset >>> 3);
        int bitInByte = (int) (bitOffset & 7);
        int mask = ((1 << REG_BITS) - 1);
        int shift = 24 - REG_BITS - bitInByte;
        // Clear the bits then OR the new value (3-byte window).
        int clearMask = ~(mask << shift);
        int b1 = hll[byteIdx] & 0xFF;
        int b2 = (byteIdx + 1 < hll.length) ? (hll[byteIdx + 1] & 0xFF) : 0;
        int b3 = (byteIdx + 2 < hll.length) ? (hll[byteIdx + 2] & 0xFF) : 0;
        int packed = (b1 << 16) | (b2 << 8) | b3;
        packed = (packed & clearMask) | ((value & mask) << shift);
        hll[byteIdx]     = (byte) ((packed >>> 16) & 0xFF);
        if (byteIdx + 1 < hll.length) hll[byteIdx + 1] = (byte) ((packed >>> 8) & 0xFF);
        if (byteIdx + 2 < hll.length) hll[byteIdx + 2] = (byte) (packed & 0xFF);
    }

    /** Returns true if any register was updated. */
    private static boolean mergeElement(byte[] hll, String element) {
        long hash = murmurHash64A(element.getBytes(java.nio.charset.StandardCharsets.UTF_8), 0xadc83b19);
        int idx = (int) (hash & (REGISTERS - 1));           // low 14 bits = index
        long rest = hash >>> 14;                            // remaining bits for count
        // Always set the implicit 1 in the (count+1)th position.
        int count = 1;
        while ((rest & 1) == 0 && count < 50) {
            rest >>>= 1;
            count++;
        }
        return setIfGreater(hll, idx, count);
    }

    private static boolean setIfGreater(byte[] hll, int idx, int value) {
        int cur = getRegister(hll, idx);
        if (value > cur) { setRegister(hll, idx, value); return true; }
        return false;
    }

    private static void mergeInto(byte[] dest, byte[] src) {
        for (int i = 0; i < REGISTERS; i++) {
            int v = getRegister(src, i);
            if (v > 0) setIfGreater(dest, i, v);
        }
    }

    private static long estimate(byte[] hll) {
        // Classic HLL estimate (Heer-Chen algorithm). p=14, m=16384.
        double m = REGISTERS;
        double alpha = 0.7213 / (1 + 1.079 / m);
        double sum = 0;
        int zeros = 0;
        for (int i = 0; i < REGISTERS; i++) {
            int r = getRegister(hll, i);
            sum += 1.0 / (1L << r);
            if (r == 0) zeros++;
        }
        double raw = alpha * m * m / sum;
        // Small-range correction (linear counting when raw < 2.5m and zeros > 0).
        if (raw <= 2.5 * m && zeros > 0) {
            return Math.round(m * Math.log(m / zeros));
        }
        return Math.round(raw);
    }

    /**
     * MurmurHash64A — same algorithm Redis uses (varint seed 0xadc83b19).
     * Lifted from the canonical Wikipedia pseudocode.
     */
    private static long murmurHash64A(byte[] data, long seed) {
        long m = 0xc6a4a7935bd1e995L;
        int r = 47;
        long h = seed ^ ((long) data.length * m);

        int i = 0;
        int end = data.length - (data.length % 8);
        while (i < end) {
            long k = 0;
            for (int j = 7; j >= 0; j--) {
                k = (k << 8) | (data[i + j] & 0xFF);
            }
            i += 8;
            k *= m;
            k ^= (k >>> r);
            k *= m;
            h ^= k;
            h *= m;
        }
        int tail = data.length & 7;
        if (tail > 0) {
            long k = 0;
            for (int j = tail - 1; j >= 0; j--) {
                k = (k << 8) | (data[i + j] & 0xFF);
            }
            h ^= k;
            h *= m;
        }
        h ^= (h >>> r);
        h *= m;
        h ^= (h >>> r);
        return h;
    }
}
