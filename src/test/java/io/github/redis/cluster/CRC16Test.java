package io.github.redis.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CRC16 (CRC-CCITT/XMODEM) and cluster slot hashing.
 *
 * <p>Expected values follow the Redis Cluster spec: the classic XMODEM check
 * value over "123456789", and hash-tag semantics — the tag is the substring
 * between the first '{@code {}' and the first '{@code }}' AFTER it; an empty
 * tag or missing braces fall back to hashing the whole key.
 */
class CRC16Test {

    private static final int TOTAL_SLOTS = 16384;

    private static int slotOf(String s) {
        return CRC16.crc16(s) % TOTAL_SLOTS;
    }

    @Test
    void crc16MatchesXmodemCheckValue() {
        assertEquals(0x31C3, CRC16.crc16("123456789"));
    }

    @Test
    void crc16OfEmptyInputIsZero() {
        assertEquals(0, CRC16.crc16(""));
    }

    @Test
    void crc16StaysWithin16Bits() {
        assertEquals(0, CRC16.crc16("abc") & ~0xFFFF);
    }

    @Test
    void keyHashSlotMatchesCrc16Modulo() {
        String[] keys = {"foo", "bar", "user1000", "订单:1"};
        for (String key : keys) {
            int slot = CRC16.keyHashSlot(key);
            assertEquals(slotOf(key), slot, "key: " + key);
            assertTrue(slot >= 0 && slot < TOTAL_SLOTS, "slot out of range: " + slot);
        }
    }

    @Test
    void hashTagsRouteToSameSlot() {
        assertEquals(slotOf("user1000"), CRC16.keyHashSlot("{user1000}.following"));
        assertEquals(slotOf("user1000"), CRC16.keyHashSlot("{user1000}.followers"));
    }

    @Test
    void emptyHashTagFallsBackToWholeKey() {
        assertEquals(slotOf("{}"), CRC16.keyHashSlot("{}"));
        assertEquals(slotOf("{}.x"), CRC16.keyHashSlot("{}.x"));
    }

    @Test
    void braceAfterCloseStillUsesTagInsideBraces() {
        // The first '}' must be searched after the '{': "a}b{c}" hashes tag "c".
        assertEquals(slotOf("c"), CRC16.keyHashSlot("a}b{c}"));
    }

    @Test
    void keyWithoutBracesHashesWholeKey() {
        assertEquals(slotOf("plainkey"), CRC16.keyHashSlot("plainkey"));
    }
}
