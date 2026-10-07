package io.nodusdb.chain;

import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Map;

import static io.nodusdb.chain.ChainFixtures.KEY_ID;
import static io.nodusdb.chain.ChainFixtures.header;
import static io.nodusdb.chain.ChainFixtures.keyring;
import static io.nodusdb.chain.ChainFixtures.otherSigningKey;
import static io.nodusdb.chain.ChainFixtures.sampleSnapshotObject;
import static io.nodusdb.chain.ChainFixtures.signingKey;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainVerifierTest {

    private static final long MICROS = RecordFixtures.COMMIT_MICROS;

    private final ChainVerifier verifier = new ChainVerifier(keyring());

    private static RecordBatch transaction(long firstLsn) {
        RecordBatch batch = new RecordBatch();
        batch.tuple(RecordType.TUPLE_ADD, 1, 2, 0, 3);
        batch.tuple(RecordType.TUPLE_REMOVE, 4, 5, 0, 6);
        batch.commit();
        batch.seal(firstLsn, MICROS);
        return batch;
    }

    private static RecordBatch autocommit(long firstLsn) {
        RecordBatch batch = new RecordBatch();
        batch.autocommitTuple(RecordType.TUPLE_ADD, 7, 8, 0, 9);
        batch.seal(firstLsn, MICROS);
        return batch;
    }

    private static byte[] bytesOf(RecordBatch... batches) {
        int total = 0;
        for (RecordBatch batch : batches) {
            total += batch.size();
        }
        ByteBuffer joined = ByteBuffer.allocate(total);
        for (RecordBatch batch : batches) {
            joined.put(RecordFixtures.copyOf(batch));
        }
        return joined.array();
    }

    private static ChainObject recordsObject(long first, long last, byte[] records, SigningKey key) {
        return ChainCodec.decode(ChainCodec.encode(header(ChainKind.RECORDS, 3), new ChainBody.Records(first, last,
                records), key));
    }

    @Test
    void aSignedSnapshotReferenceVerifies() {
        assertDoesNotThrow(() -> verifier.verify(ChainCodec.decode(sampleSnapshotObject())));
    }

    @Test
    void wholeTransactionsWithRightLsnsVerify() {
        byte[] records = bytesOf(transaction(100), autocommit(103), transaction(104));

        assertDoesNotThrow(() -> verifier.verify(recordsObject(100, 106, records, signingKey())));
    }

    @Test
    void anAutocommitRecordAloneVerifies() {
        assertDoesNotThrow(() -> verifier.verify(recordsObject(50, 50, bytesOf(autocommit(50)), signingKey())));
    }

    @Test
    void aKeyThatIsNotInTheKeyringIsRefusedByItsId() {
        ChainObject object = recordsObject(100, 102, bytesOf(transaction(100)), signingKey());
        ChainVerifier other = new ChainVerifier(Keyring.single(KEY_ID + 1, KeyFiles.generate().getPublic()));

        ChainTrustException refused = assertThrows(ChainTrustException.class, () -> other.verify(object));

        assertTrue(refused.getMessage().contains("no trusted key has the id " + KEY_ID), refused.getMessage());
        assertThrows(ChainTrustException.class, () -> new ChainVerifier(Keyring.empty()).verify(object));
    }

    @Test
    void anObjectSignedByAnotherKeyUnderTheSameIdIsRefused() {
        ChainObject forged = recordsObject(100, 102, bytesOf(transaction(100)), otherSigningKey());

        ChainTrustException refused = assertThrows(ChainTrustException.class, () -> verifier.verify(forged));

        assertTrue(refused.getMessage().contains("does not verify"), refused.getMessage());
    }

    @Test
    void theKeyIsChosenByTheIdInTheHeader() {
        KeyPair second = KeyFiles.generate();
        Keyring both = Keyring.of(Map.of(KEY_ID, keyring().find(KEY_ID).orElseThrow(), 9, second.getPublic()));
        ChainObject signedBySecond = ChainCodec.decode(ChainCodec.encode(
                new ChainHeader(ChainKind.RECORDS, 1, 1, 1, ChainHash.ZERO, 9),
                new ChainBody.Records(100, 102, bytesOf(transaction(100))), new SigningKey(9, second.getPrivate())));

        assertDoesNotThrow(() -> new ChainVerifier(both).verify(signedBySecond));
        assertThrows(ChainTrustException.class, () -> verifier.verify(signedBySecond));
    }

    @Test
    void aFlippedSignatureBitIsRefused() {
        byte[] encoded = ChainCodec.encode(header(ChainKind.RECORDS, 3),
                new ChainBody.Records(100, 102, bytesOf(transaction(100))), signingKey());
        encoded[encoded.length - 5] ^= 0x01;

        assertThrows(ChainTrustException.class, () -> verifier.verify(ChainCodec.decode(encoded)));
    }

    @Test
    void aCorruptRecordIsRefusedEvenWithAValidSignature() {
        byte[] records = bytesOf(transaction(100));
        records[10] ^= 0x01;
        ChainObject object = recordsObject(100, 102, records, signingKey());

        assertThrows(ChainFormatException.class, () -> verifier.verify(object));
    }

    @Test
    void theRangeInTheHeaderMustMatchTheRecords() {
        byte[] records = bytesOf(transaction(100));

        assertThrows(ChainFormatException.class, () -> verifier.verify(recordsObject(101, 103, records, signingKey())));
        ChainFormatException tooShort = assertThrows(ChainFormatException.class,
                () -> verifier.verify(recordsObject(100, 101, records, signingKey())));
        assertTrue(tooShort.getMessage().contains("header says 101"), tooShort.getMessage());
        assertThrows(ChainFormatException.class, () -> verifier.verify(recordsObject(100, 103, records, signingKey())));
    }

    @Test
    void anObjectThatEndsInsideATransactionIsRefused() {
        byte[] whole = bytesOf(transaction(100));
        byte[] withoutCommit = Arrays.copyOf(whole, whole.length - 48);

        ChainFormatException refused = assertThrows(ChainFormatException.class,
                () -> verifier.verify(recordsObject(100, 101, withoutCommit, signingKey())));

        assertTrue(refused.getMessage().contains("inside a transaction"), refused.getMessage());
    }

    @Test
    void anObjectThatStartsMidTransactionIsRefused() {
        byte[] whole = bytesOf(transaction(100));
        int firstLength = ByteBuffer.wrap(whole).getInt(0);
        byte[] dropped = Arrays.copyOfRange(whole, firstLength, whole.length);

        assertThrows(ChainFormatException.class, () -> verifier.verify(recordsObject(101, 102, dropped, signingKey())));
    }

    @Test
    void aGapInsideTheRecordsIsRefused() {
        byte[] records = bytesOf(transaction(100), autocommit(104));

        ChainFormatException refused = assertThrows(ChainFormatException.class,
                () -> verifier.verify(recordsObject(100, 104, records, signingKey())));

        assertTrue(refused.getMessage().contains("expected LSN 103 but found 104"), refused.getMessage());
    }

    @Test
    void aCorruptedCommitRecordIsRefused() {
        byte[] records = bytesOf(transaction(100));
        ByteBuffer buffer = ByteBuffer.wrap(records);
        int commitOffset = records.length - 48;
        buffer.putInt(commitOffset + 24, 5);
        ChainObject object = recordsObject(100, 102, records, signingKey());

        assertThrows(ChainFormatException.class, () -> verifier.verify(object));
    }

    @Test
    void theRecordsOfEveryRecordTypeVerify() {
        RecordBatch every = RecordFixtures.everyRecordType();
        byte[] records = RecordFixtures.copyOf(every);

        assertDoesNotThrow(() -> verifier.verify(recordsObject(RecordFixtures.FIRST_LSN,
                RecordFixtures.FIRST_LSN + every.count() - 1, records, signingKey())));
    }
}
