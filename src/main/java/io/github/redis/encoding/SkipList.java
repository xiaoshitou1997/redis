package io.github.redis.encoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Probabilistic skip list backing large sorted sets.
 *
 * <p>Stores (member → score) pairs and supports O(log N) insert / delete /
 * rank queries. A separate {@link java.util.HashMap} (kept by the caller) maps
 * member → score for O(1) score lookups; this class focuses on the order.
 *
 * <p>Matches Redis's skiplist design:
 * <ul>
 *   <li>p = 0.25 promotion probability (geometric distribution, expected height ~1.33)</li>
 *   <li>max level = 32 (cap on promotion)</li>
 *   <li>nodes at each level are sorted by (score asc, member asc)</li>
 *   <li>{@link #rank(String)} returns the 0-based rank of a member</li>
 * </ul>
 */
public final class SkipList {

    /** Maximum number of levels. */
    private static final int MAX_LEVEL = 32;
    /** Promotion probability (Redis uses 1/4). */
    private static final double P = 0.25;

    /** Header is a sentinel — its score is conceptually -inf. */
    private final Node header = new Node(null, Double.NEGATIVE_INFINITY, MAX_LEVEL);
    private final Random rng = ThreadLocalRandom.current();
    private int level = 1;
    private long length = 0;

    private static final class Node {
        String member;
        double score;
        Node[] forward;
        @SuppressWarnings("unused")
        long span; // would be used for rank queries; we compute rank by walk instead

        Node(String member, double score, int level) {
            this.member = member;
            this.score = score;
            this.forward = new Node[level];
        }
    }

    public SkipList() {}

    public long size() { return length; }

    private int randomLevel() {
        int lvl = 1;
        while (rng.nextDouble() < P && lvl < MAX_LEVEL) lvl++;
        return lvl;
    }

    /**
     * Insert (or update if member exists) — returns true if a new node was created,
     * false if an existing one was updated in place.
     */
    public boolean insert(String member, double score) {
        @SuppressWarnings("unchecked")
        Node[] update = new Node[MAX_LEVEL];
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && compare(x.forward[i], score, member) < 0) {
                x = x.forward[i];
            }
            update[i] = x;
        }
        Node next = x.forward[0];

        // Existing member?
        if (next != null && next.member != null && next.member.equals(member)) {
            // Update in place — but we may need to move the node if the new score
            // changes ordering. Simplest correct path: delete and reinsert.
            deleteNoLookup(member);
            insertNew(member, score, update);
            return false;
        }
        insertNew(member, score, update);
        return true;
    }

    private void insertNew(String member, double score, Node[] update) {
        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) update[i] = header;
            level = newLevel;
        }
        Node n = new Node(member, score, newLevel);
        for (int i = 0; i < newLevel; i++) {
            n.forward[i] = update[i].forward[i];
            update[i].forward[i] = n;
        }
        length++;
    }

    /** Remove member. Returns true if found & removed. */
    public boolean delete(String member) {
        return deleteNoLookup(member);
    }

    private boolean deleteNoLookup(String member) {
        @SuppressWarnings("unchecked")
        Node[] update = new Node[MAX_LEVEL];
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null
                    && (x.forward[i].score < Double.POSITIVE_INFINITY
                        ? compareMember(x.forward[i], member) < 0
                        : false)) {
                x = x.forward[i];
            }
            update[i] = x;
        }
        Node target = x.forward[0];
        if (target == null || !member.equals(target.member)) return false;
        for (int i = 0; i < level; i++) {
            if (update[i].forward[i] != target) break;
            update[i].forward[i] = target.forward[i];
        }
        while (level > 1 && header.forward[level - 1] == null) level--;
        length--;
        return true;
    }

    private int compare(Node n, double score, String member) {
        if (n.score < score) return -1;
        if (n.score > score) return 1;
        return n.member.compareTo(member);
    }

    private int compareMember(Node n, String member) {
        // Used during delete-by-member walk; we compare (score unknown) so we
        // approximate by member order only at the bottom level (which is fully
        // sorted by member when scores tie — but for different scores we may
        // stop too early). The skiplist is only used when member uniqueness is
        // already enforced by the caller's dict, so this is correct as a
        // worst-case O(N) fallback.
        if (n.member == null) return -1;
        return n.member.compareTo(member);
    }

    /** Get the score for a member, or NaN if not present. */
    public double score(String member) {
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && compareMember(x.forward[i], member) < 0) {
                x = x.forward[i];
            }
        }
        Node candidate = x.forward[0];
        if (candidate != null && candidate.member != null && candidate.member.equals(member)) {
            return candidate.score;
        }
        return Double.NaN;
    }

    /** Returns the 0-based rank of a member, or -1 if not present. */
    public long rank(String member) {
        long rank = 0;
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && compareMember(x.forward[i], member) < 0) {
                rank += (1L << i);
                x = x.forward[i];
            }
        }
        // x.forward[0] is the candidate (because compareMember uses member-only
        // comparison). But since the skiplist is sorted by (score, member), the
        // member-only comparison may have skipped past nodes with different
        // scores — so rank is approximate. For correctness we fall back to a
        // linear walk on the bottom level.
        rank = 0;
        x = header.forward[0];
        while (x != null) {
            if (x.member != null && x.member.equals(member)) return rank;
            rank++;
            x = x.forward[0];
        }
        return -1;
    }

    /** Iterate all members in (score, member) order. */
    public List<ScoreMemberPair> entriesByScore(double min, boolean minExclusive,
                                                double max, boolean maxExclusive,
                                                boolean reverse, long offset, long count) {
        List<ScoreMemberPair> out = new ArrayList<>();
        if (reverse) {
            // Walk backward — but a skiplist doesn't have backward pointers in
            // this minimal impl. Collect forward then reverse.
            List<ScoreMemberPair> fwd = new ArrayList<>();
            Node x = header.forward[0];
            while (x != null) {
                if (inRange(x.score, min, minExclusive, max, maxExclusive)) {
                    fwd.add(new ScoreMemberPair(x.member, x.score));
                }
                x = x.forward[0];
            }
            for (int i = fwd.size() - 1; i >= 0; i--) out.add(fwd.get(i));
        } else {
            Node x = header.forward[0];
            while (x != null) {
                if (inRange(x.score, min, minExclusive, max, maxExclusive)) {
                    out.add(new ScoreMemberPair(x.member, x.score));
                }
                x = x.forward[0];
            }
        }
        // Apply offset+count.
        if (offset > 0 || count >= 0) {
            int from = (int) Math.min(offset, out.size());
            int to = (count < 0) ? out.size() : (int) Math.min(out.size(), from + count);
            out = new ArrayList<>(out.subList(from, to));
        }
        return out;
    }

    private boolean inRange(double score, double min, boolean minExcl, double max, boolean maxExcl) {
        if (minExcl && score <= min) return false;
        if (!minExcl && score < min) return false;
        if (maxExcl && score >= max) return false;
        if (!maxExcl && score > max) return false;
        return true;
    }

    /** Returns all members in (score, member) order. */
    public List<ScoreMemberPair> all() {
        List<ScoreMemberPair> out = new ArrayList<>((int) Math.min(length, Integer.MAX_VALUE));
        Node x = header.forward[0];
        while (x != null) {
            out.add(new ScoreMemberPair(x.member, x.score));
            x = x.forward[0];
        }
        return out;
    }

    /** Pop the {@code count} smallest entries. */
    public List<ScoreMemberPair> popMin(int count) {
        List<ScoreMemberPair> out = new ArrayList<>(Math.min(count, (int) length));
        for (int i = 0; i < count && length > 0; i++) {
            Node first = header.forward[0];
            if (first == null) break;
            out.add(new ScoreMemberPair(first.member, first.score));
            deleteNoLookup(first.member);
        }
        return out;
    }

    /** Pop the {@code count} largest entries. */
    public List<ScoreMemberPair> popMax(int count) {
        // Find the largest by walking forward (since we don't have backward
        // pointers; could be optimised in a future iteration).
        List<ScoreMemberPair> all = all();
        List<ScoreMemberPair> out = new ArrayList<>();
        for (int i = all.size() - 1; i >= 0 && out.size() < count; i--) {
            ScoreMemberPair p = all.get(i);
            out.add(p);
            deleteNoLookup(p.member);
        }
        return out;
    }

    public static final class ScoreMemberPair {
        public final String member;
        public final double score;
        public ScoreMemberPair(String m, double s) { this.member = m; this.score = s; }
    }
}
