package io.github.redis.acl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * Redis-style password hashing.
 *
 * <p>Redis stores ACL passwords as hex SHA256 digests of the cleartext password.
 * This class implements that scheme plus the prefixed wire format used in ACL
 * files ({@code "<64 hex chars>"}).
 *
 * <p>For Phase-5 we keep the cleartext-path too, because the legacy {@code >pass}
 * ACL rule and the {@code requirepass} directive both accept cleartext on the
 * wire — Redis hashes them once at ingest time.
 */
public final class PasswordHasher {

    private static final SecureRandom RNG = new SecureRandom();
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private PasswordHasher() {}

    /** Hash a cleartext password to its 64-char lowercase-hex SHA256 form. */
    public static String hashCleartext(String cleartext) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(cleartext.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /** Verify a cleartext candidate against a stored hex SHA256 hash. */
    public static boolean verify(String cleartextCandidate, String storedHexHash) {
        if (cleartextCandidate == null || storedHexHash == null) return false;
        // Redis 6+ stores 64-char SHA256 hex. Be permissive about case.
        String computed = hashCleartext(cleartextCandidate);
        return constantTimeEquals(computed, storedHexHash.toLowerCase());
    }

    /** Generate a random 256-bit password (returned as 64 hex chars). */
    public static String generateRandom() {
        byte[] bytes = new byte[32];
        RNG.nextBytes(bytes);
        return bytesToHex(bytes);
    }

    /** Test whether the given string looks like a hex SHA256 hash. */
    public static boolean isHexHash(String s) {
        if (s == null || s.length() != 64) return false;
        for (int i = 0; i < 64; i++) {
            char c = s.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    private static String bytesToHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            out[i * 2]     = HEX[v >>> 4];
            out[i * 2 + 1] = HEX[v & 0x0F];
        }
        return new String(out);
    }

    /** Constant-time equality for stored-secret comparisons. */
    public static boolean constantTimeEquals(String a, String b) {
        if (a.length() != b.length()) return false;
        int diff = 0;
        for (int i = 0; i < a.length(); i++) diff |= a.charAt(i) ^ b.charAt(i);
        return diff == 0;
    }
}
