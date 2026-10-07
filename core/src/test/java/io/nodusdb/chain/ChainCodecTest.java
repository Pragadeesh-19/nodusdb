package io.nodusdb.chain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.nodusdb.chain.ChainFixtures.KEY_ID;
import static io.nodusdb.chain.ChainFixtures.header;
import static io.nodusdb.chain.ChainFixtures.keyring;
import static io.nodusdb.chain.ChainFixtures.opaqueRecords;
import static io.nodusdb.chain.ChainFixtures.sampleRecordsObject;
import static io.nodusdb.chain.ChainFixtures.sampleSnapshotObject;
import static io.nodusdb.chain.ChainFixtures.signingKey;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainCodecTest {

    private static byte[] forge(int kindCode, byte[] body) {
        ByteBuffer out = ByteBuffer.allocate(ChainCodec.HEADER_BYTES + body.length + ChainCodec.TRAILER_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        out.putInt(ChainCodec.MAGIC).putShort((short) ChainCodec.VERSION).put((byte) kindCode).put((byte) 0);
        out.putLong(1).putLong(1).putLong(1);
        out.put(new byte[ChainHash.BYTES]);
        out.putInt(KEY_ID).putInt(0);
        out.put(body);
        ChainHash digest = ChainHash.sha256(out.array(), 0, ChainCodec.HEADER_BYTES + body.length);
        out.put(digest.toBytes());
        out.put(new byte[ChainSignature.BYTES]);
        return out.array();
    }

    private static byte[] recordsBody(long first, long last, int recordBytes) {
        ByteBuffer body = ByteBuffer.allocate(16 + recordBytes);
        body.putLong(first).putLong(last);
        return body.array();
    }

    private static byte[] snapshotBody(byte[] path, int declaredLength, long lsn) {
        ByteBuffer body = ByteBuffer.allocate(2 + path.length + ChainHash.BYTES + 8);
        body.putShort((short) declaredLength).put(path).put(new byte[ChainHash.BYTES]).putLong(lsn);
        return body.array();
    }

    private static byte[] patched(byte[] object, int offset, byte value) {
        byte[] copy = object.clone();
        copy[offset] = value;
        return copy;
    }

    private static void assertRefused(byte[] bytes, String reason) {
        ChainFormatException refused = assertThrows(ChainFormatException.class, () -> ChainCodec.decode(bytes));
        assertTrue(refused.getMessage().contains(reason), "expected '" + reason + "' in: " + refused.getMessage());
    }

    @Test
    void everyCorpusObjectIsEncodedByteForByteLikeTheIndependentImplementation() throws IOException {
        for (ChainCorpus.Entry entry : ChainCorpus.load()) {
            byte[] encoded = ChainCodec.encode(entry.header(), entry.body(), entry.key());

            assertArrayEquals(entry.object(), encoded, entry.name());
        }
    }

    @Test
    void everyCorpusObjectDecodesToItsFields() throws IOException {
        for (ChainCorpus.Entry entry : ChainCorpus.load()) {
            ChainObject decoded = ChainCodec.decode(entry.object());

            assertEquals(entry.header(), decoded.header(), entry.name());
            assertEquals(entry.digest(), decoded.digest(), entry.name());
            assertArrayEquals(entry.signature(), decoded.signature(), entry.name());
            assertArrayEquals(entry.object(), decoded.encoded(), entry.name());
            switch (entry.body()) {
                case ChainBody.Records expected -> {
                    ChainBody.Records actual = assertInstanceOf(ChainBody.Records.class, decoded.body());
                    assertEquals(expected.lsnFirst(), actual.lsnFirst(), entry.name());
                    assertEquals(expected.lsnLast(), actual.lsnLast(), entry.name());
                    assertArrayEquals(expected.records(), actual.records(), entry.name());
                }
                case ChainBody.SnapshotRef expected -> assertEquals(expected, decoded.body(), entry.name());
            }
        }
    }

    @Test
    void everyCorpusObjectVerifiesWithItsPublicKeyAndOnlyThat() throws IOException {
        KeyPair stranger = KeyFiles.generate();
        for (ChainCorpus.Entry entry : ChainCorpus.load()) {
            ChainObject decoded = ChainCodec.decode(entry.object());

            new ChainVerifier(Keyring.single(entry.header().keyId(), entry.publicKey())).verify(decoded);
            assertThrows(ChainTrustException.class, () -> new ChainVerifier(
                    Keyring.single(entry.header().keyId(), stranger.getPublic())).verifySignature(decoded),
                    entry.name());
        }
    }

    @Test
    void theIndependentImplementationsPublicKeyRoundTripsThroughOurRawFormat() throws IOException {
        for (ChainCorpus.Entry entry : ChainCorpus.load()) {
            byte[] raw = KeyFiles.rawPublic(entry.publicKey());

            assertEquals(entry.publicKey(), KeyFiles.publicKey(raw), entry.name());
        }
    }

    @Test
    void encodingTheSameInputTwiceGivesTheSameBytes() {
        assertArrayEquals(sampleRecordsObject(), sampleRecordsObject());
        assertArrayEquals(sampleSnapshotObject(), sampleSnapshotObject());
    }

    @Test
    void aRoundTripKeepsEveryField() {
        ChainObject records = ChainCodec.decode(sampleRecordsObject());
        ChainObject snapshot = ChainCodec.decode(sampleSnapshotObject());

        assertEquals(header(ChainKind.RECORDS, 7), records.header());
        assertEquals(header(ChainKind.SNAPSHOT_REF, 8), snapshot.header());
        assertEquals(10, ((ChainBody.Records) records.body()).lsnFirst());
        assertEquals(12, ((ChainBody.Records) records.body()).lsnLast());
        assertArrayEquals(opaqueRecords(), ((ChainBody.Records) records.body()).records());
        assertEquals(42, ((ChainBody.SnapshotRef) snapshot.body()).lsn());
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void randomObjectsRoundTrip(long seed) {
        Random random = new Random(seed);
        for (int i = 0; i < 300; i++) {
            ChainHeader header = new ChainHeader(random.nextBoolean() ? ChainKind.RECORDS : ChainKind.SNAPSHOT_REF,
                    1 + Math.floorMod(random.nextLong(), Long.MAX_VALUE), Math.floorMod(random.nextLong(), 1L << 62),
                    random.nextLong(), hash(random), KEY_ID);
            ChainBody body;
            if (header.kind() == ChainKind.RECORDS) {
                byte[] payload = new byte[1 + random.nextInt(300)];
                random.nextBytes(payload);
                long first = 1 + Math.floorMod(random.nextLong(), 1L << 40);
                body = new ChainBody.Records(first, first + random.nextInt(1000), payload);
            } else {
                body = new ChainBody.SnapshotRef(randomPath(random), hash(random),
                        Math.floorMod(random.nextLong(), Long.MAX_VALUE));
            }

            ChainObject decoded = ChainCodec.decode(ChainCodec.encode(header, body, signingKey()));

            String where = "seed=" + seed + " i=" + i;
            assertEquals(header, decoded.header(), where);
            assertEquals(body.kind(), decoded.body().kind(), where);
            if (body instanceof ChainBody.Records expected) {
                ChainBody.Records actual = (ChainBody.Records) decoded.body();
                assertArrayEquals(expected.records(), actual.records(), where);
                assertEquals(expected.lsnFirst(), actual.lsnFirst(), where);
                assertEquals(expected.lsnLast(), actual.lsnLast(), where);
            } else {
                assertEquals(body, decoded.body(), where);
            }
            new ChainVerifier(keyring()).verifySignature(decoded);
        }
    }

    private static ChainHash hash(Random random) {
        byte[] bytes = new byte[ChainHash.BYTES];
        random.nextBytes(bytes);
        return ChainHash.of(bytes);
    }

    private static String randomPath(Random random) {
        String alphabet = "abcdefghijklmnopqrstuvwxyz0123456789_-";
        StringBuilder path = new StringBuilder("_nodus/snapshots/s");
        int length = random.nextInt(60);
        for (int i = 0; i < length; i++) {
            path.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return path.append(".nsnap").toString();
    }

    @Test
    void anObjectShorterThanTheFixedPartsIsRefusedAtEveryLength() {
        byte[] object = sampleRecordsObject();

        for (int length = 0; length < ChainCodec.HEADER_BYTES + ChainCodec.TRAILER_BYTES; length++) {
            byte[] cut = Arrays.copyOf(object, length);
            assertThrows(ChainFormatException.class, () -> ChainCodec.decode(cut), "length " + length);
        }
    }

    @Test
    void everyTruncationAndExtensionOfAnObjectIsRefused() {
        for (byte[] object : List.of(sampleRecordsObject(), sampleSnapshotObject())) {
            for (int length = 0; length < object.length; length++) {
                byte[] cut = Arrays.copyOf(object, length);
                assertThrows(ChainFormatException.class, () -> ChainCodec.decode(cut), "length " + length);
            }
            for (int extra = 1; extra <= 3; extra++) {
                byte[] longer = Arrays.copyOf(object, object.length + extra);
                assertThrows(ChainFormatException.class, () -> ChainCodec.decode(longer), "extra " + extra);
            }
        }
    }

    @Test
    void everySingleBitFlipIsEitherRefusedOrFailsTheSignature() {
        ChainVerifier verifier = new ChainVerifier(keyring());
        for (byte[] original : List.of(sampleRecordsObject(), sampleSnapshotObject())) {
            for (int index = 0; index < original.length; index++) {
                for (int bit = 0; bit < 8; bit++) {
                    byte[] flipped = original.clone();
                    flipped[index] ^= (byte) (1 << bit);
                    ChainObject decoded;
                    try {
                        decoded = ChainCodec.decode(flipped);
                    } catch (ChainFormatException refused) {
                        continue;
                    }
                    assertThrows(ChainTrustException.class, () -> verifier.verifySignature(decoded),
                            "byte " + index + " bit " + bit + " was accepted");
                }
            }
        }
    }

    @Test
    void arbitraryBytesAfterAValidPrefixOnlyEverRaiseTheFormatException() {
        Random random = new Random(99);
        byte[] prefix = Arrays.copyOf(sampleRecordsObject(), ChainCodec.HEADER_BYTES);
        int accepted = 0;
        for (int input = 0; input < 20_000; input++) {
            byte[] bytes = new byte[ChainCodec.HEADER_BYTES + random.nextInt(260)];
            random.nextBytes(bytes);
            if (random.nextInt(4) != 0) {
                System.arraycopy(prefix, 0, bytes, 0, Math.min(prefix.length, bytes.length));
            }
            try {
                ChainCodec.decode(bytes);
                accepted++;
            } catch (ChainFormatException expected) {
                continue;
            }
        }
        assertEquals(0, accepted, "random bytes cannot carry a matching digest");
    }

    @Test
    void theHeaderIsCheckedBeforeAnythingElse() {
        byte[] object = sampleRecordsObject();

        assertRefused(patched(object, 0, (byte) 'X'), "not a chain object");
        assertRefused(patched(object, 5, (byte) 2), "version 2");
        assertRefused(patched(object, 4, (byte) 1), "version 257");
        assertRefused(patched(object, 7, (byte) 1), "reserved header byte");
        assertRefused(patched(object, 6, (byte) 0), "unknown chain object kind 0");
        assertRefused(patched(object, 6, (byte) 4), "unknown chain object kind 4");
        assertRefused(patched(object, 6, (byte) 255), "unknown chain object kind 255");
        for (int offset = 68; offset < 72; offset++) {
            assertRefused(patched(object, offset, (byte) 1), "reserved header field");
        }
    }

    @Test
    void aZeroSequenceAndNegativeEpochOrKeyIdAreRefused() {
        byte[] object = sampleRecordsObject();
        byte[] zeroSeq = object.clone();
        Arrays.fill(zeroSeq, 8, 16, (byte) 0);

        assertRefused(zeroSeq, "sequence");
        assertRefused(patched(object, 16, (byte) 0x80), "negative");
        assertRefused(patched(object, 64, (byte) 0x80), "negative");
    }

    @Test
    void aRedactionObjectIsRecognizedButNotSupportedYet() {
        assertRefused(forge(3, new byte[]{1, 2, 3}), "redaction objects are not supported");
    }

    @Test
    void aTamperedDigestIsReportedAsAMismatch() {
        byte[] object = sampleRecordsObject();

        assertRefused(patched(object, ChainCodec.HEADER_BYTES + 3, (byte) 0x77), "stored digest does not match");
        assertRefused(patched(object, object.length - ChainSignature.BYTES - 1, (byte) 0x77),
                "stored digest does not match");
    }

    @Test
    void aRecordsBodyNeedsAValidRangeAndAtLeastOneRecord() {
        assertRefused(forge(1, recordsBody(0, 5, 8)), "not valid");
        assertRefused(forge(1, recordsBody(5, 4, 8)), "not valid");
        assertRefused(forge(1, recordsBody(1, 1, 0)), "at least one record");
        assertRefused(forge(1, new byte[3]), "at least one record");
        ChainCodec.decode(forge(1, recordsBody(1, 1, 8)));
    }

    @Test
    void aSnapshotReferenceNeedsAConsistentWellFormedPath() {
        byte[] path = "_nodus/snapshots/00000000000000000001.nsnap".getBytes(StandardCharsets.UTF_8);

        ChainCodec.decode(forge(2, snapshotBody(path, path.length, 7)));
        assertRefused(forge(2, snapshotBody(path, 0, 7)), "invalid path length");
        assertRefused(forge(2, snapshotBody(path, path.length - 1, 7)), "invalid path length");
        assertRefused(forge(2, snapshotBody(path, path.length + 1, 7)), "invalid path length");
        assertRefused(forge(2, snapshotBody(path, 513, 7)), "invalid path length");
        assertRefused(forge(2, snapshotBody(path, path.length, -1)), "LSN is negative");
        assertRefused(forge(2, new byte[5]), "cut short");
    }

    @Test
    void aSnapshotPathMustBeAValidObjectKey() {
        for (String bad : new String[]{"Upper/Case", "a//b", "../escape", "has space", "nul", "trailing/"}) {
            byte[] path = bad.getBytes(StandardCharsets.UTF_8);
            assertRefused(forge(2, snapshotBody(path, path.length, 1)), "not a valid object key");
        }
        byte[] invalidUtf8 = {(byte) 0xFF, (byte) 0xFE, 'a'};
        assertRefused(forge(2, snapshotBody(invalidUtf8, invalidUtf8.length, 1)), "not a valid object key");
    }

    @Test
    void anObjectOverTheSizeLimitIsRefusedBeforeAnyParsing() {
        assertRefused(new byte[ChainCodec.MAX_OBJECT_BYTES + 1], "limited to");
    }

    @Test
    void encodingRefusesAHeaderThatDisagreesWithItsBodyOrKey() {
        SigningKey key = signingKey();
        ChainBody.Records records = new ChainBody.Records(1, 1, opaqueRecords());

        assertThrows(IllegalArgumentException.class,
                () -> ChainCodec.encode(header(ChainKind.SNAPSHOT_REF, 1), records, key));
        assertThrows(IllegalArgumentException.class, () -> ChainCodec.encode(
                new ChainHeader(ChainKind.RECORDS, 1, 1, 1, ChainHash.ZERO, KEY_ID + 1), records, key));
    }

    @Test
    void encodingRefusesAnOversizedBodyAndAnInvalidSnapshotPath() {
        SigningKey key = signingKey();

        assertThrows(IllegalArgumentException.class, () -> ChainCodec.encode(header(ChainKind.RECORDS, 1),
                new ChainBody.Records(1, 1, new byte[ChainCodec.MAX_OBJECT_BYTES]), key));
        assertThrows(IllegalArgumentException.class, () -> ChainCodec.encode(header(ChainKind.SNAPSHOT_REF, 1),
                new ChainBody.SnapshotRef("Bad Path", ChainHash.ZERO, 1), key));
    }

    @Test
    void headersBodiesAndKeysRefuseNonsense() {
        KeyPair pair = KeyFiles.generate();

        assertThrows(IllegalArgumentException.class, () -> new ChainHeader(ChainKind.RECORDS, 0, 1, 1, ChainHash.ZERO, 1));
        assertThrows(IllegalArgumentException.class, () -> new ChainHeader(ChainKind.RECORDS, 1, -1, 1, ChainHash.ZERO, 1));
        assertThrows(IllegalArgumentException.class, () -> new ChainBody.Records(0, 1, opaqueRecords()));
        assertThrows(IllegalArgumentException.class, () -> new ChainBody.Records(5, 4, opaqueRecords()));
        assertThrows(IllegalArgumentException.class, () -> new ChainBody.SnapshotRef("a/b", ChainHash.ZERO, -1));
        assertThrows(IllegalArgumentException.class, () -> ChainHash.of(new byte[31]));
        assertThrows(IllegalArgumentException.class, () -> new SigningKey(-1, pair.getPrivate()));
        assertThrows(IllegalArgumentException.class, () -> Keyring.single(-1, pair.getPublic()));
    }

    @Test
    void theHashIsAValueThatPrintsAsHex() {
        ChainHash hash = ChainHash.sha256("abc".getBytes(StandardCharsets.UTF_8));

        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", hash.hex());
        assertEquals(hash.hex(), hash.toString());
        assertEquals(hash, ChainHash.parse(hash.hex()));
        assertEquals(hash.hashCode(), ChainHash.parse(hash.hex()).hashCode());
        assertEquals(ChainHash.ZERO, ChainHash.of(new byte[32]));
    }

    @Test
    void aSigningKeyNeverPrintsKeyMaterial() {
        assertEquals("SigningKey[keyId=5]", signingKey().toString());
    }
}
