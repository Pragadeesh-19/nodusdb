package io.nodusdb.kernel.symbols;

import io.nodusdb.log.record.RecordFormat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

public final class Pseudonym {

    public static final int RECORD_BYTES = RecordFormat.ERASE_PSEUDONYM_BYTES;
    public static final String PREFIX = "erased:";

    private static final int HEX_DIGITS = 16;
    private static final int TEXT_BYTES = PREFIX.length() + HEX_DIGITS;

    private Pseudonym() {
    }

    public static byte[] of(byte[] salt, byte[] utf8) {
        Objects.requireNonNull(salt, "salt");
        Objects.requireNonNull(utf8, "utf8");
        MessageDigest sha256 = sha256();
        sha256.update(salt);
        sha256.update(utf8);
        String hex = HexFormat.of().formatHex(sha256.digest()).substring(0, HEX_DIGITS);
        return (PREFIX + hex).getBytes(StandardCharsets.US_ASCII);
    }

    public static byte[] padded(byte[] pseudonym) {
        if (pseudonym.length != TEXT_BYTES) {
            throw new IllegalArgumentException("a pseudonym is " + TEXT_BYTES + " bytes, got " + pseudonym.length);
        }
        return Arrays.copyOf(pseudonym, RECORD_BYTES);
    }

    public static byte[] unpadded(byte[] field) {
        if (field.length != RECORD_BYTES) {
            throw new IllegalArgumentException("a pseudonym field is " + RECORD_BYTES + " bytes, got " + field.length);
        }
        int length = field.length;
        while (length > 0 && field[length - 1] == 0) {
            length--;
        }
        return Arrays.copyOf(field, length);
    }

    public static boolean isPseudonym(byte[] utf8) {
        if (utf8.length != TEXT_BYTES) {
            return false;
        }
        for (int i = 0; i < PREFIX.length(); i++) {
            if (utf8[i] != PREFIX.charAt(i)) {
                return false;
            }
        }
        for (int i = PREFIX.length(); i < TEXT_BYTES; i++) {
            byte b = utf8[i];
            if (!(b >= '0' && b <= '9' || b >= 'a' && b <= 'f')) {
                return false;
            }
        }
        return true;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("the JDK has no SHA-256", e);
        }
    }
}
