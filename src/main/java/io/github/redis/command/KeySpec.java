package io.github.redis.command;

import java.util.List;

/**
 * Declarative description of which argv positions are keys for a given command.
 *
 * <p>Mirrors Redis's KeySpec struct ({@code commands.def}). Two orthogonal pieces:
 * <ul>
 *   <li><b>begin_search</b>: where to start looking for keys
 *       — {@link #INDEX(int)} (fixed position) or
 *         {@link #KEYNUM(int, int, int)} (a previous argv tells the count)</li>
 *   <li><b>find_keys</b>: how keys are laid out from there
 *       — range with lastkey offset and keystep (handles MSET, EXPIRE, ZUNIONSTORE, EVAL...)</li>
 * </ul>
 *
 * <p>Currently covers INDEX+RANGE (≈85% of all commands) and KEYNUM (EVAL, MSET, SORT).
 * KEYWORD-based begin_search (SORT BY/GET, SET, SETRANGE) can be added later without
 * breaking callers.
 */
public final class KeySpec {

    private final int beginIndex;      // first argv position that holds a key
    private final int lastKeyOffset;   // 0 = single key; positive N = N keys fixed; -1 = until argc-1
    private final int keyStep;         // 1 normally; 2 for (key,value) interleaved; 3 for (k,v,?)
    private final int keynumIndex;     // -1 = fixed range mode; otherwise argv position holding count

    private KeySpec(int beginIndex, int lastKeyOffset, int keyStep, int keynumIndex) {
        this.beginIndex = beginIndex;
        this.lastKeyOffset = lastKeyOffset;
        this.keyStep = keyStep;
        this.keynumIndex = keynumIndex;
    }

    // ========== Static factories ==========

    /** argv[idx] is the only key. e.g. GET key, INCR key, HLEN key. */
    public static KeySpec single(int idx) {
        return new KeySpec(idx, 0, 1, -1);
    }

    /**
     * All tokens from argv[begin] to argv[argc-1] are keys.
     * e.g. DEL k1 k2 k3, EXISTS k1 k2, SADD k m1 m2 (the keys part is just begin).
     */
    public static KeySpec variadic(int begin) {
        return new KeySpec(begin, -1, 1, -1);
    }

    /**
     * Fixed number of keys starting at begin.
     * e.g. RENAME src dst (begin=1, count=2), COPY src dst (begin=1, count=2).
     */
    public static KeySpec fixed(int begin, int count) {
        return new KeySpec(begin, count - 1, 1, -1);
    }

    /**
     * Keys interleaved with non-key args, starting at begin, every {@code step} tokens.
     * e.g. MSET k v k v (begin=1, step=2), ZUNIONSTORE dest n k1 w1 k2 w2 (after numkeys).
     */
    public static KeySpec interleaved(int begin, int step) {
        return new KeySpec(begin, -1, step, -1);
    }

    /**
     * Number of keys comes from argv[countIdx]; keys start at beginIndex with the given step.
     * e.g. EVAL script numkeys k1 k2 ... (countIdx=2, begin=3, step=1)
     *      XADD key [N number-of-fields-or-args] -- not used here, separate path.
     */
    public static KeySpec keynum(int countIdx, int begin, int step) {
        return new KeySpec(begin, -1, step, countIdx);
    }

    // ========== Key extraction ==========

    /**
     * Append every key referenced by this spec, given current argv/argc, into {@code out}.
     * Silently no-ops if the spec is out of range (defensive — handlers also check arity).
     */
    public void appendKeys(String[] argv, int argc, List<String> out) {
        if (keynumIndex >= 0) {
            appendKeynumKeys(argv, argc, out);
            return;
        }
        if (beginIndex < 0 || beginIndex >= argc) {
            return;
        }
        if (lastKeyOffset == 0) {
            // Single key
            String k = argv[beginIndex];
            if (k != null) {
                out.add(k);
            }
            return;
        }
        if (lastKeyOffset > 0) {
            // Fixed count: keys are at begin, begin+step, ... up to lastKeyOffset
            int end = beginIndex + lastKeyOffset * keyStep;
            for (int i = beginIndex; i <= end && i < argc; i += keyStep) {
                String k = argv[i];
                if (k != null) {
                    out.add(k);
                }
            }
            return;
        }
        // Variadic / interleaved until end
        for (int i = beginIndex; i < argc; i += keyStep) {
            String k = argv[i];
            if (k != null) {
                out.add(k);
            }
        }
    }

    private void appendKeynumKeys(String[] argv, int argc, List<String> out) {
        if (keynumIndex < 0 || keynumIndex >= argc) {
            return;
        }
        int count;
        try {
            count = Integer.parseInt(argv[keynumIndex]);
        } catch (NumberFormatException e) {
            return;
        }
        if (count < 0) {
            return;
        }
        for (int i = 0; i < count; i++) {
            int pos = beginIndex + i * keyStep;
            if (pos >= argc) {
                break;
            }
            String k = argv[pos];
            if (k != null) {
                out.add(k);
            }
        }
    }

    // Accessors (debug only — not on hot path)

    public int getBeginIndex() {
        return beginIndex;
    }

    public int getLastKeyOffset() {
        return lastKeyOffset;
    }

    public int getKeyStep() {
        return keyStep;
    }

    public int getKeynumIndex() {
        return keynumIndex;
    }

    public boolean isKeynumBased() {
        return keynumIndex >= 0;
    }

    @Override
    public String toString() {
        return "KeySpec{begin=" + beginIndex
                + ", lastKeyOffset=" + lastKeyOffset
                + ", step=" + keyStep
                + ", keynumIdx=" + keynumIndex + '}';
    }
}
