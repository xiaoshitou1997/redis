package io.github.redis.encoding;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Listpack container: positional order, lookup, mutation, and the
 * promotion-relevant {@code maxElementBytes()} of each entry type.
 */
class ListpackTest {

    @Test
    void appendAndPrependMaintainPositionalOrder() {
        Listpack lp = new Listpack();
        lp.append(new Listpack.SingleEntry("b"));
        lp.append(new Listpack.SingleEntry("c"));
        lp.prepend(new Listpack.SingleEntry("a"));
        assertEquals(3, lp.size());
        assertEquals("a", ((Listpack.SingleEntry) lp.get(0)).value);
        assertEquals("b", ((Listpack.SingleEntry) lp.get(1)).value);
        assertEquals("c", ((Listpack.SingleEntry) lp.get(2)).value);
    }

    @Test
    void setAndRemoveMutateByIndex() {
        Listpack lp = new Listpack();
        lp.append(new Listpack.SingleEntry("1"));
        lp.append(new Listpack.SingleEntry("2"));
        lp.set(1, new Listpack.SingleEntry("2x"));
        assertEquals("2x", ((Listpack.SingleEntry) lp.get(1)).value);
        lp.remove(0);
        assertEquals(1, lp.size());
        assertEquals("2x", ((Listpack.SingleEntry) lp.get(0)).value);
    }

    @Test
    void indexOfKeyFindsFieldsOrReturnsMinusOne() {
        Listpack lp = new Listpack();
        lp.append(new Listpack.FieldValue("f1", "v1"));
        lp.append(new Listpack.FieldValue("f2", "v2"));
        assertEquals(0, lp.indexOfKey("f1"));
        assertEquals(1, lp.indexOfKey("f2"));
        assertEquals(-1, lp.indexOfKey("nope"));
    }

    @Test
    void clearEmptiesTheListpack() {
        Listpack lp = new Listpack();
        lp.append(new Listpack.SingleEntry("x"));
        lp.clear();
        assertTrue(lp.isEmpty());
        assertEquals(0, lp.size());
    }

    @Test
    void maxElementBytesCoversFieldAndValue() {
        Listpack.FieldValue fv = new Listpack.FieldValue("short", "a-much-longer-value");
        assertEquals("a-much-longer-value".length(), fv.maxElementBytes());
        assertEquals("short".length(), new Listpack.FieldValue("short", null).maxElementBytes());
        assertEquals(0, new Listpack.SingleEntry(null).maxElementBytes());
        Listpack.ScoreMember sm = new Listpack.ScoreMember("member", 1.0);
        assertEquals("member", sm.key());
        assertEquals("member".length(), sm.maxElementBytes());
    }
}
