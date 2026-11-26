package io.github.redis.encoding;

import java.util.Arrays;

/**
 * Sorted-array "intset" — a compact representation for sets whose every member
 * is an integer. Memory per element is exactly 1/2/4/8 bytes (depending on the
 * largest value), versus 32+ bytes per entry in a {@code HashSet}.
 *
 * <p>Search/insert is O(log N) via binary search. Conversion to {@code HashSet}
 * happens when a non-integer member is added or the set grows past
 * {@link Encoding#SET_MAX_INTSET_ENTRIES}.
 *
 * <p>This class is intentionally not thread-safe — the dispatcher's
 * single-threaded execution model guarantees safe access.
 */
public final class IntSet {

    /** Encoding values match Redis's INTSET_ENC_* constants. */
    private static final int ENC_16 = 2;
    private static final int ENC_32 = 4;
    private static final int ENC_64 = 8;

    /** Backing storage, treated as a packed array of {@code enc}-byte integers. */
    private byte[] data;
    /** Bytes per integer. Grows lazily when an out-of-range value is inserted. */
    private int enc = ENC_16;
    /** Number of integers currently stored. */
    private int size = 0;

    public IntSet() {
        this(8);
    }

    private IntSet(int initialCapacity) {
        this.data = new byte[initialCapacity * enc];
    }

    public int size() { return size; }
    public boolean isEmpty() { return size == 0; }

    /** Returns the value at logical index {@code i}. */
    private long get(int i) {
        int off = i * enc;
        long v = 0;
        for (int b = 0; b < enc; b++) {
            v = (v << 8) | (data[off + b] & 0xFFL);
        }
        return v;
    }

    /** Sets the value at logical index {@code i} (assumes enc is already big enough). */
    private void set(int i, long v) {
        int off = i * enc;
        for (int b = enc - 1; b >= 0; b--) {
            data[off + b] = (byte) (v & 0xFFL);
            v >>>= 8;
        }
    }

    private int encodingFor(long v) {
        if (v < 0) {
            if (v >= Short.MIN_VALUE) return ENC_16;
            if (v >= Integer.MIN_VALUE) return ENC_32;
            return ENC_64;
        } else {
            if (v <= Short.MAX_VALUE) return ENC_16;
            if (v <= Integer.MAX_VALUE) return ENC_32;
            return ENC_64;
        }
    }

    private void upgradeTo(int newEnc) {
        if (newEnc <= enc) return;
        byte[] oldData = data;
        int oldEnc = enc;
        enc = newEnc;
        data = new byte[Math.max(size * enc, oldData.length / oldEnc * enc) + enc * 4];
        // Re-insert each value (preserving sorted order — values are all positive
        // after expansion OR all negative; either way monotonic).
        for (int i = size - 1; i >= 0; i--) {
            long v = 0;
            int oldOff = i * oldEnc;
            for (int b = 0; b < oldEnc; b++) {
                v = (v << 8) | (oldData[oldOff + b] & 0xFFL);
            }
            set(i, v);
        }
    }

    /** Binary search for {@code v}; returns the index where v is or should be inserted. */
    private int search(long v) {
        int lo = 0, hi = size - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long mv = get(mid);
            if (mv == v) return -mid - 1; // found — negative-encoded index
            if (mv < v) lo = mid + 1;
            else        hi = mid - 1;
        }
        return lo; // insertion point
    }

    /**
     * Insert {@code v}. Returns true if added (was absent), false if it was a duplicate.
     */
    public boolean add(long v) {
        int idx = search(v);
        if (idx < 0) return false; // already present
        int needed = encodingFor(v);
        if (needed > enc) {
            upgradeTo(needed);
            // After upgrade the array is still sorted; re-search.
            idx = search(v);
            if (idx < 0) return false;
        }
        ensureCapacity(size + 1);
        // Shift right by 1 to make room.
        System.arraycopy(data, idx * enc, data, (idx + 1) * enc, (size - idx) * enc);
        set(idx, v);
        size++;
        return true;
    }

    /** Removes {@code v}; returns true if it was present. */
    public boolean remove(long v) {
        int idx = search(v);
        if (idx >= 0) return false;
        int real = -idx - 1;
        System.arraycopy(data, (real + 1) * enc, data, real * enc, (size - real - 1) * enc);
        size--;
        return true;
    }

    public boolean contains(long v) {
        return search(v) < 0;
    }

    /** Return all values in ascending order. */
    public long[] toArray() {
        long[] out = new long[size];
        for (int i = 0; i < size; i++) out[i] = get(i);
        return out;
    }

    /** Returns the original string form for value at index i, as Redis would store it. */
    public String stringValue(int i) {
        return Long.toString(get(i));
    }

    private void ensureCapacity(int required) {
        int requiredBytes = required * enc;
        if (requiredBytes <= data.length) return;
        int newSize = data.length;
        while (newSize < requiredBytes) newSize <<= 1;
        data = Arrays.copyOf(data, newSize);
    }
}
