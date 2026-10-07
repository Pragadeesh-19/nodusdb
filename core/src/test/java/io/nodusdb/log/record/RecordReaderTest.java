package io.nodusdb.log.record;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordReaderTest {

    @Test
    void everyProperPrefixOfAValidRecordIsIncomplete() {
        RecordBatch batch = RecordFixtures.everyRecordType();
        byte[] bytes = RecordFixtures.copyOf(batch);
        RecordReader reader = batch.reader();
        int offset = 0;
        while (reader.hasRecord()) {
            int length = reader.length();
            for (int prefix = 0; prefix < length; prefix++) {
                ByteBuffer truncated = ByteBuffer.wrap(bytes, 0, offset + prefix);
                RecordReader view = new RecordReader().wrap(truncated, offset, offset + prefix);
                assertEquals(Verdict.INCOMPLETE, view.inspect(), "prefix " + prefix + " of record at " + offset);
            }
            offset += length;
            reader.advance();
        }
    }

    @Test
    void anySingleBitFlipIsNeverAcceptedAsAValidRecord() {
        for (RecordBatch batch : new RecordBatch[] {
                RecordFixtures.everyRecordType(),
                RecordFixtures.transaction(),
                RecordFixtures.autocommit(RecordType.TUPLE_REMOVE, 3, 4, 5, 6)}) {
            byte[] original = RecordFixtures.copyOf(batch);
            RecordReader reader = batch.reader();
            int offset = 0;
            while (reader.hasRecord()) {
                int length = reader.length();
                for (int bit = 0; bit < length * 8; bit++) {
                    byte[] damaged = original.clone();
                    damaged[offset + bit / 8] ^= (byte) (1 << (bit % 8));
                    RecordReader view = new RecordReader().wrap(ByteBuffer.wrap(damaged), offset, damaged.length);
                    assertNotEquals(Verdict.VALID, view.inspect(),
                            "bit " + bit + " of the " + reader.type() + " record at " + offset);
                }
                offset += length;
                reader.advance();
            }
        }
    }

    @Test
    void randomBytesNeverThrowAndAreNeverAcceptedAsRecords() {
        Random random = new Random(7L);
        byte[] noise = new byte[4096];
        RecordReader reader = new RecordReader();
        for (int round = 0; round < 20_000; round++) {
            random.nextBytes(noise);
            int from = random.nextInt(64);
            reader.wrap(ByteBuffer.wrap(noise), from, noise.length);
            Verdict verdict = reader.inspect();
            assertNotEquals(Verdict.VALID, verdict);
        }
    }

    @Test
    void aZeroFilledRegionIsInvalidBecauseNoRecordHasLengthZero() {
        RecordReader reader = new RecordReader().wrap(ByteBuffer.allocate(128), 0, 128);

        assertEquals(Verdict.INVALID, reader.inspect());
        assertTrue(reader.invalidReason().contains("length"));
    }

    @Test
    void fewerThanFourBytesCannotEvenNameALength() {
        RecordReader reader = new RecordReader().wrap(ByteBuffer.allocate(3), 0, 3);

        assertEquals(Verdict.INCOMPLETE, reader.inspect());
    }

    @Test
    void aHugeLengthIsInvalidRatherThanWaitedFor() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.putInt(0, Integer.MAX_VALUE & -8);

        assertEquals(Verdict.INVALID, new RecordReader().wrap(buffer, 0, 64).inspect());
    }

    @Test
    void unknownRecordTypesAreInvalidAndNeverSkipped() {
        RecordBatch batch = RecordFixtures.autocommit(RecordType.TUPLE_ADD, 1, 2, 0, 3);
        byte[] bytes = RecordFixtures.copyOf(batch);
        bytes[RecordFormat.TYPE_OFFSET] = 0x7F;
        resealWithCorrectChecksum(bytes);

        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length);

        assertEquals(Verdict.INVALID, reader.inspect());
        assertTrue(reader.invalidReason().contains("unknown record type"));
    }

    @Test
    void nonZeroReservedBytesAreInvalidEvenWithAMatchingChecksum() {
        RecordBatch batch = RecordFixtures.autocommit(RecordType.TUPLE_ADD, 1, 2, 0, 3);
        byte[] bytes = RecordFixtures.copyOf(batch);
        bytes[RecordFormat.RESERVED_OFFSET] = 1;
        resealWithCorrectChecksum(bytes);

        assertEquals(Verdict.INVALID, new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length).inspect());
    }

    @Test
    void autocommitFlagOnANonTupleRecordIsInvalid() {
        RecordBatch batch = RecordFixtures.transaction();
        byte[] bytes = RecordFixtures.copyOf(batch);
        bytes[RecordFormat.FLAGS_OFFSET] = RecordFormat.FLAG_AUTOCOMMIT;
        resealWithCorrectChecksum(bytes);

        assertEquals(Verdict.INVALID, new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length).inspect());
    }

    @Test
    void aNegativeNodeIdIsInvalid() {
        RecordBatch batch = RecordFixtures.autocommit(RecordType.TUPLE_ADD, 1, 2, 0, 3);
        byte[] bytes = RecordFixtures.copyOf(batch);
        ByteBuffer view = ByteBuffer.wrap(bytes);
        view.putInt(RecordFormat.TUPLE_OBJECT_OFFSET, -5);
        resealWithCorrectChecksum(bytes);

        assertEquals(Verdict.INVALID, new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length).inspect());
    }

    @Test
    void aSymbolWhoseLengthDisagreesWithTheRecordLengthIsInvalid() {
        RecordBatch batch = new RecordBatch();
        RecordFixtures.symbol(batch, 1, "user:alice");
        batch.commit();
        batch.seal(1, 1);
        byte[] bytes = RecordFixtures.copyOf(batch);
        ByteBuffer.wrap(bytes).putInt(RecordFormat.SYMBOL_LENGTH_OFFSET, 3);
        resealWithCorrectChecksum(bytes);

        assertEquals(Verdict.INVALID, new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length).inspect());
    }

    @Test
    void readerWalksRecordsBackToBackAndStopsAtTheEnd() {
        RecordBatch batch = RecordFixtures.transaction();
        RecordReader reader = batch.reader();
        int visited = 0;

        while (reader.hasRecord()) {
            assertEquals(Verdict.VALID, reader.inspect());
            reader.advance();
            visited++;
        }

        assertEquals(batch.count(), visited);
        assertEquals(batch.size(), reader.position());
    }

    private static void resealWithCorrectChecksum(byte[] bytes) {
        ByteBuffer view = ByteBuffer.wrap(bytes);
        int length = view.getInt(0);
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(bytes, 0, length - RecordFormat.CHECKSUM_BYTES);
        view.putInt(length - RecordFormat.CHECKSUM_BYTES, (int) crc.getValue());
    }
}
