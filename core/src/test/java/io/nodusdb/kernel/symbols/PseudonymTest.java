package io.nodusdb.kernel.symbols;

import io.nodusdb.log.record.RecordFormat;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PseudonymTest {

    private static byte[] salt(int fill) {
        byte[] salt = new byte[32];
        java.util.Arrays.fill(salt, (byte) fill);
        return salt;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void thePseudonymIsErasedFollowedByTheFirstSixteenHexDigitsOfTheSaltedHash() throws Exception {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        sha256.update(salt(7));
        sha256.update(bytes("user:alice"));
        String expected = "erased:" + HexFormat.of().formatHex(sha256.digest()).substring(0, 16);

        assertEquals(expected, new String(Pseudonym.of(salt(7), bytes("user:alice")), StandardCharsets.US_ASCII));
    }

    @Test
    void theSaltChangesThePseudonym() {
        assertFalse(java.util.Arrays.equals(Pseudonym.of(salt(1), bytes("user:alice")),
                Pseudonym.of(salt(2), bytes("user:alice"))));
        assertArrayEquals(Pseudonym.of(salt(1), bytes("user:alice")), Pseudonym.of(salt(1), bytes("user:alice")));
    }

    @Test
    void theRecordFieldIsTheTwentyThreeBytesPaddedWithOneZero() {
        byte[] pseudonym = Pseudonym.of(salt(3), bytes("user:alice"));

        byte[] field = Pseudonym.padded(pseudonym);

        assertEquals(RecordFormat.ERASE_PSEUDONYM_BYTES, field.length);
        assertEquals(0, field[23]);
        assertArrayEquals(pseudonym, Pseudonym.unpadded(field));
    }

    @Test
    void badLengthsAreRefused() {
        assertThrows(IllegalArgumentException.class, () -> Pseudonym.padded(bytes("erased:short")));
        assertThrows(IllegalArgumentException.class, () -> Pseudonym.unpadded(new byte[23]));
    }

    @Test
    void onlyTheExactShapeIsRecognisedAsAPseudonym() {
        assertTrue(Pseudonym.isPseudonym(Pseudonym.of(salt(5), bytes("user:alice"))));
        assertFalse(Pseudonym.isPseudonym(bytes("user:alice")));
        assertFalse(Pseudonym.isPseudonym(bytes("erased:0123456789ABCDEF")));
        assertFalse(Pseudonym.isPseudonym(bytes("erased:0123456789abcde")));
        assertFalse(Pseudonym.isPseudonym(bytes("Erased:0123456789abcdef")));
    }
}
