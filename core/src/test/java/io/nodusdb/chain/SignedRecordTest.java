package io.nodusdb.chain;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SignedRecordTest {

    private static final String DOMAIN = "nodus.commit.v1";
    private static final KeyPair PAIR = KeyFiles.generate();
    private static final SigningKey KEY = new SigningKey(7, PAIR.getPrivate());
    private static final Keyring RING = Keyring.single(7, PAIR.getPublic());

    private static byte[] payload(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aSealedRecordOpensToItsPayloadAndKeyId() {
        String sealed = SignedRecord.seal(KEY, DOMAIN, payload("{\"lsn\":42}"));

        SignedRecord.Opened opened = SignedRecord.open(RING, DOMAIN, sealed);

        assertEquals(7, opened.keyId());
        assertArrayEquals(payload("{\"lsn\":42}"), opened.payload());
    }

    @Test
    void anEmptyAndALargePayloadRoundTrip() {
        byte[] large = new byte[100_000];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) (i * 31);
        }

        assertArrayEquals(new byte[0], SignedRecord.open(RING, DOMAIN, SignedRecord.seal(KEY, DOMAIN, new byte[0]))
                .payload());
        assertArrayEquals(large, SignedRecord.open(RING, DOMAIN, SignedRecord.seal(KEY, DOMAIN, large)).payload());
    }

    @Test
    void theSealedFormIsThreeUrlSafePartsWithoutPadding() {
        String sealed = SignedRecord.seal(KEY, DOMAIN, payload("??>>"));

        String[] parts = sealed.split("\\.");

        assertEquals(3, parts.length);
        assertTrue(parts[0].matches("[A-Za-z0-9_-]*"));
        assertTrue(parts[1].matches("[A-Za-z0-9_-]+"));
        assertEquals("7", parts[2]);
    }

    @Test
    void anotherDomainDoesNotVerify() {
        String sealed = SignedRecord.seal(KEY, DOMAIN, payload("x"));

        assertThrows(ChainTrustException.class, () -> SignedRecord.open(RING, "nodus.chain.v1", sealed));
    }

    @Test
    void aTamperedPayloadDoesNotVerify() {
        String[] parts = SignedRecord.seal(KEY, DOMAIN, payload("original")).split("\\.");
        String forged = Base64.getUrlEncoder().withoutPadding().encodeToString(payload("forged"));

        assertThrows(ChainTrustException.class,
                () -> SignedRecord.open(RING, DOMAIN, forged + "." + parts[1] + "." + parts[2]));
    }

    @Test
    void aTamperedSignatureDoesNotVerify() {
        String sealed = SignedRecord.seal(KEY, DOMAIN, payload("original"));
        String[] parts = sealed.split("\\.");
        char first = parts[1].charAt(0);
        String flipped = (first == 'A' ? 'B' : 'A') + parts[1].substring(1);

        assertThrows(ChainTrustException.class,
                () -> SignedRecord.open(RING, DOMAIN, parts[0] + "." + flipped + "." + parts[2]));
    }

    @Test
    void aKeyThatIsNotTrustedIsRefused() {
        String sealed = SignedRecord.seal(KEY, DOMAIN, payload("x"));

        assertThrows(ChainTrustException.class, () -> SignedRecord.open(Keyring.empty(), DOMAIN, sealed));
        assertThrows(ChainTrustException.class, () -> SignedRecord.open(
                Keyring.single(7, KeyFiles.generate().getPublic()), DOMAIN, sealed));
        assertThrows(ChainTrustException.class, () -> SignedRecord.open(
                Keyring.single(8, PAIR.getPublic()), DOMAIN, sealed));
    }

    @Test
    void aClaimedKeyIdOtherThanTheSignersDoesNotVerify() {
        String[] parts = SignedRecord.seal(KEY, DOMAIN, payload("x")).split("\\.");
        Keyring both = Keyring.of(Map.of(7, PAIR.getPublic(), 9, KeyFiles.generate().getPublic()));

        assertThrows(ChainTrustException.class,
                () -> SignedRecord.open(both, DOMAIN, parts[0] + "." + parts[1] + ".9"));
    }

    @Test
    void malformedTextIsAFormatErrorNotATrustError() {
        String good = SignedRecord.seal(KEY, DOMAIN, payload("x"));
        String[] parts = good.split("\\.");

        for (String bad : new String[]{"", "a", "a.b", "a.b.c.d", parts[0] + "." + parts[1] + ".x",
                parts[0] + "." + parts[1] + ".-1", "!!!." + parts[1] + "." + parts[2],
                parts[0] + ".%%%." + parts[2], parts[0] + "." + parts[1] + "."}) {
            assertThrows(ChainFormatException.class, () -> SignedRecord.open(RING, DOMAIN, bad), "'" + bad + "'");
        }
    }
}
