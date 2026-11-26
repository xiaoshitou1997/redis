package io.github.redis.encoding;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sorted-array intset: insert/lookup/remove semantics, encoding upgrades, and
 * correct decoding of negative values (16/32-bit encodings must sign-extend).
 */
class IntSetTest {

    @Test
    void addReturnsTrueForNewValueAndFalseForDuplicate() {
        IntSet set = new IntSet();
        assertTrue(set.add(10));
        assertTrue(set.add(20));
        assertFalse(set.add(10));
        assertEquals(2, set.size());
    }

    @Test
    void toArrayReturnsAscendingOrder() {
        IntSet set = new IntSet();
        set.add(30);
        set.add(10);
        set.add(20);
        assertArrayEquals(new long[] {10, 20, 30}, set.toArray());
    }

    @Test
    void containsFindsPresentAndMissesAbsent() {
        IntSet set = new IntSet();
        set.add(7);
        assertTrue(set.contains(7));
        assertFalse(set.contains(8));
    }

    @Test
    void removeDeletesPresentValueOnly() {
        IntSet set = new IntSet();
        set.add(1);
        set.add(2);
        set.add(3);
        assertTrue(set.remove(2));
        assertFalse(set.remove(2));
        assertFalse(set.remove(99));
        assertArrayEquals(new long[] {1, 3}, set.toArray());
    }

    @Test
    void negativeValuesSurviveInsertAndLookup() {
        IntSet set = new IntSet();
        assertTrue(set.add(-5));
        assertTrue(set.contains(-5));
        assertFalse(set.contains(-6));
        assertTrue(set.add(-40000)); // forces 16 -> 32-bit upgrade
        assertTrue(set.contains(-5)); // old values still found after upgrade
        assertTrue(set.contains(-40000));
        assertFalse(set.contains(-40001));
        assertArrayEquals(new long[] {-40000, -5}, set.toArray());
    }

    @Test
    void mixedSignValuesStaySortedAcrossUpgrades() {
        IntSet set = new IntSet();
        set.add(3);
        set.add(-5);
        set.add(2000000000);
        set.add(-2000000000);
        set.add(0);
        assertArrayEquals(new long[] {-2000000000L, -5L, 0L, 3L, 2000000000L}, set.toArray());
        for (long v : new long[] {-2000000000L, -5L, 0L, 3L, 2000000000L}) {
            assertTrue(set.contains(v), "value: " + v);
        }
        assertEquals("-2000000000", set.stringValue(0));
    }

    @Test
    void upgradeKeepsExistingMembersIntact() {
        IntSet set = new IntSet();
        for (int i = 0; i < 100; i++) {
            set.add(i);
        }
        set.add(Short.MAX_VALUE + 1); // upgrade 16 -> 32 bits
        assertEquals(101, set.size());
        assertTrue(set.contains(0));
        assertTrue(set.contains(99));
        assertTrue(set.contains(Short.MAX_VALUE + 1));
        assertFalse(set.contains(100));
    }
}
