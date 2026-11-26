package io.github.redis.encoding;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Skiplist ordering and lifecycle. The list is sorted by (score asc, member
 * asc); member lookup, score updates and deletes must stay correct even when
 * the member's dictionary order disagrees with its score order.
 */
class SkipListTest {

    @Test
    void entriesAreOrderedByScoreThenMember() {
        SkipList sl = new SkipList();
        sl.insert("x", 1.0);
        sl.insert("b", 1.0);
        sl.insert("a", 1.0);
        sl.insert("m", -1.0);
        List<SkipList.ScoreMemberPair> all = sl.all();
        assertEquals(4, all.size());
        assertEquals("m", all.get(0).member);
        assertEquals("a", all.get(1).member);
        assertEquals("b", all.get(2).member);
        assertEquals("x", all.get(3).member);
    }

    @Test
    void insertExistingMemberUpdatesScoreInPlace() {
        SkipList sl = new SkipList();
        sl.insert("a", 1.0);
        sl.insert("b", 2.0);
        sl.insert("c", 3.0);
        // Move "a" far past the others — must relocate, not duplicate.
        assertFalse(sl.insert("a", 10.0));
        assertEquals(3, sl.size());
        assertEquals(10.0, sl.score("a"), 0.0);
        List<SkipList.ScoreMemberPair> all = sl.all();
        assertEquals("b", all.get(0).member);
        assertEquals("c", all.get(1).member);
        assertEquals("a", all.get(2).member);
    }

    @Test
    void deleteAndScoreWorkRegardlessOfMemberOrder() {
        SkipList sl = new SkipList();
        sl.insert("a", 1.0);
        sl.insert("z", 9.0);
        sl.insert("b", 100.0); // sorts after "z" by score, before it by member
        assertEquals(100.0, sl.score("b"), 0.0);
        assertTrue(sl.delete("b"));
        assertFalse(sl.delete("b"));
        assertEquals(2, sl.size());
        assertTrue(Double.isNaN(sl.score("b")));
        assertEquals(1.0, sl.score("a"), 0.0);
        assertEquals(9.0, sl.score("z"), 0.0);
    }

    @Test
    void rankIsZeroBasedOverAllEntries() {
        SkipList sl = new SkipList();
        sl.insert("m1", 1.0);
        sl.insert("m2", 2.0);
        sl.insert("m3", 3.0);
        assertEquals(0, sl.rank("m1"));
        assertEquals(1, sl.rank("m2"));
        assertEquals(2, sl.rank("m3"));
        assertEquals(-1, sl.rank("missing"));
    }

    @Test
    void entriesByScoreHonorsInclusiveAndExclusiveBounds() {
        SkipList sl = new SkipList();
        for (int i = 1; i <= 5; i++) {
            sl.insert("m" + i, i);
        }
        List<SkipList.ScoreMemberPair> r = sl.entriesByScore(2, false, 4, true, false, 0, -1);
        assertEquals(2, r.size());
        assertEquals("m2", r.get(0).member);
        assertEquals("m3", r.get(1).member);

        List<SkipList.ScoreMemberPair> rev = sl.entriesByScore(2, false, 4, true, true, 0, -1);
        assertEquals("m3", rev.get(0).member);
        assertEquals("m2", rev.get(1).member);

        List<SkipList.ScoreMemberPair> page = sl.entriesByScore(
                Double.NEGATIVE_INFINITY, false, Double.POSITIVE_INFINITY, false, false, 1, 2);
        assertEquals(2, page.size());
        assertEquals("m2", page.get(0).member);
        assertEquals("m3", page.get(1).member);
    }

    @Test
    void popMinAndPopMaxRemoveExtremes() {
        SkipList sl = new SkipList();
        for (int i = 1; i <= 4; i++) {
            sl.insert("m" + i, i);
        }
        List<SkipList.ScoreMemberPair> mins = sl.popMin(2);
        assertEquals("m1", mins.get(0).member);
        assertEquals("m2", mins.get(1).member);
        List<SkipList.ScoreMemberPair> maxs = sl.popMax(1);
        assertEquals("m4", maxs.get(0).member);
        assertEquals(1, sl.size());
        assertEquals(3.0, sl.score("m3"), 0.0);
    }
}
