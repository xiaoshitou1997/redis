package io.github.redis.acl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Redis-style ACL password hashing: known SHA-256 vectors, verify semantics,
 * hex validation and random password generation.
 */
class PasswordHasherTest {

    @Test
    void hashCleartextProducesKnownSha256Hex() {
        assertEquals("5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8",
                PasswordHasher.hashCleartext("password"));
    }

    @Test
    void hashIs64LowercaseHexChars() {
        String h = PasswordHasher.hashCleartext("s3cret");
        assertEquals(64, h.length());
        assertEquals(h.toLowerCase(), h);
        assertTrue(PasswordHasher.isHexHash(h));
    }

    @Test
    void verifyAcceptsCorrectAndRejectsWrongPassword() {
        String stored = PasswordHasher.hashCleartext("s3cret");
        assertTrue(PasswordHasher.verify("s3cret", stored));
        assertFalse(PasswordHasher.verify("wrong", stored));
        assertFalse(PasswordHasher.verify("s3cret", null));
        assertFalse(PasswordHasher.verify(null, stored));
    }

    @Test
    void verifyIsCaseInsensitiveOnStoredHash() {
        String stored = PasswordHasher.hashCleartext("s3cret").toUpperCase();
        assertTrue(PasswordHasher.verify("s3cret", stored));
    }

    @Test
    void isHexHashEnforcesLengthAndAlphabet() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 64; i++) {
            sb.append('a');
        }
        assertTrue(PasswordHasher.isHexHash(sb.toString()));

        sb.setLength(63);
        assertFalse(PasswordHasher.isHexHash(sb.toString()));

        StringBuilder bad = new StringBuilder();
        for (int i = 0; i < 63; i++) {
            bad.append('a');
        }
        bad.append('g');
        assertFalse(PasswordHasher.isHexHash(bad.toString()));
        assertFalse(PasswordHasher.isHexHash(null));
    }

    @Test
    void generateRandomProducesDistinct64HexPasswords() {
        String a = PasswordHasher.generateRandom();
        String b = PasswordHasher.generateRandom();
        assertEquals(64, a.length());
        assertTrue(PasswordHasher.isHexHash(a));
        assertNotEquals(a, b);
    }
}
