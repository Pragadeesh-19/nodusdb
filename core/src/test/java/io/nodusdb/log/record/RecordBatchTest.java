package io.nodusdb.log.record;

import org.junit.jupiter.api.Test;

import static io.nodusdb.log.record.RecordFixtures.COMMIT_MICROS;
import static io.nodusdb.log.record.RecordFixtures.FIRST_LSN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordBatchTest {

    @Test
    void autocommitTupleIsOneFortyByteRecordCarryingItsCommitTime() {
        RecordBatch batch = RecordFixtures.autocommit(RecordType.TUPLE_ADD, 12, 3, 0, 99);

        RecordReader reader = batch.reader();
        assertEquals(1, batch.count());
        assertEquals(RecordFormat.TUPLE_AUTOCOMMIT_BYTES, batch.size());
        assertEquals(Verdict.VALID, reader.inspect());
        assertEquals(RecordType.TUPLE_ADD, reader.type());
        assertTrue(reader.autocommit());
        assertTrue(reader.isCommitPoint());
        assertEquals(FIRST_LSN, reader.lsn());
        assertEquals(COMMIT_MICROS, reader.commitMicros());
        assertEquals(12, reader.object());
        assertEquals(3, reader.relation());
        assertEquals(0, reader.subjectRelation());
        assertEquals(99, reader.subject());
    }

    @Test
    void transactionRecordsAreNumberedFromTheFirstLsnAndEndWithACommit() {
        RecordBatch batch = RecordFixtures.transaction();

        RecordReader reader = batch.reader();
        long expectedLsn = FIRST_LSN;
        int seen = 0;
        while (reader.hasRecord()) {
            assertEquals(Verdict.VALID, reader.inspect(), "record " + seen);
            assertEquals(expectedLsn++, reader.lsn());
            seen++;
            if (seen < batch.count()) {
                assertFalse(reader.isCommitPoint());
            } else {
                assertEquals(RecordType.TXN_COMMIT, reader.type());
                assertEquals(FIRST_LSN, reader.commitFirstLsn());
                assertEquals(batch.count() - 1, reader.commitRecordCount());
                assertEquals(COMMIT_MICROS, reader.commitMicros());
            }
            reader.advance();
        }
        assertEquals(batch.count(), seen);
    }

    @Test
    void everyRecordTypeRoundTripsItsFields() {
        RecordBatch batch = RecordFixtures.everyRecordType();
        RecordReader reader = batch.reader();

        assertEquals(Verdict.VALID, reader.inspect());
        assertEquals(RecordType.EPOCH, reader.type());
        assertEquals(3, reader.epochNumber());
        assertEquals(11, reader.epochWriterKey());
        assertEquals(995, reader.epochHandoffLsn());
        reader.advance();

        assertEquals(RecordType.GRAPH_CONFIG, reader.type());
        assertEquals(1, reader.keyKindCode());
        reader.advance();

        assertEquals(RecordType.SYMBOL, reader.type());
        assertEquals(0, reader.symbolId());
        assertEquals(0, reader.symbolLength());
        reader.advance();

        assertEquals(RecordType.SYMBOL, reader.type());
        assertEquals("user:alice", new String(reader.symbolBytes(), java.nio.charset.StandardCharsets.UTF_8));
        reader.advance();
        reader.advance();

        assertEquals(RecordType.SCHEMA, reader.type());
        assertEquals(Verdict.VALID, reader.inspect());
        assertEquals(4, reader.schemaVersion());
        assertEquals(3, reader.schemaRelationCount());
        assertEquals(2, reader.schemaRelationId(1));
        assertEquals(10, reader.schemaRelationTypeSymbol(1));
        assertEquals(21, reader.schemaRelationNameSymbol(1));
        assertEquals(6, reader.schemaRelationFlags(2));
        assertEquals("type user\ntype group { relation member: user }\n",
                new String(reader.schemaDocument(), java.nio.charset.StandardCharsets.UTF_8));
        reader.advance();

        assertEquals(RecordType.TUPLE_ADD, reader.type());
        assertFalse(reader.autocommit());
        reader.advance();

        assertEquals(RecordType.TUPLE_REMOVE, reader.type());
        assertEquals(65_535, reader.subjectRelation());
        assertEquals(2_147_483_637, reader.subject());
        reader.advance();

        assertEquals(RecordType.ERASE, reader.type());
        assertEquals(1, reader.eraseSymbolId());
        assertEquals(24, reader.erasePseudonym().length);
    }

    @Test
    void tuplesRefuseRelationsThatDoNotFitSixteenBits() {
        RecordBatch batch = new RecordBatch();

        assertThrows(IllegalArgumentException.class, () -> batch.tuple(RecordType.TUPLE_ADD, 1, 65_536, 0, 2));
        assertThrows(IllegalArgumentException.class, () -> batch.tuple(RecordType.TUPLE_ADD, 1, 0, -1, 2));
    }

    @Test
    void anAutocommitRecordMustBeTheOnlyRecord() {
        RecordBatch batch = new RecordBatch();
        batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);

        assertThrows(IllegalStateException.class,
                () -> batch.autocommitTuple(RecordType.TUPLE_ADD, 1, 0, 0, 3));
        batch.clear();
        batch.autocommitTuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);
        assertThrows(IllegalStateException.class, () -> batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 3));
        assertThrows(IllegalStateException.class, batch::commit);
    }

    @Test
    void aBatchCannotBeSealedBeforeItIsCommittedOrSealedTwice() {
        RecordBatch batch = new RecordBatch();
        batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);

        assertThrows(IllegalStateException.class, () -> batch.seal(1, 1));
        batch.commit();
        batch.seal(1, 1);
        assertThrows(IllegalStateException.class, () -> batch.seal(1, 1));
        assertThrows(IllegalStateException.class, () -> batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2));
    }

    @Test
    void anEmptyBatchCannotBeCommitted() {
        RecordBatch batch = new RecordBatch();

        assertThrows(IllegalStateException.class, batch::commit);
        assertTrue(batch.isEmpty());
    }

    @Test
    void clearingMakesTheBatchReusableWithoutLeakingEarlierBytes() {
        RecordBatch batch = RecordFixtures.everyRecordType();
        batch.clear();

        batch.autocommitTuple(RecordType.TUPLE_ADD, 5, 6, 0, 7);
        batch.seal(2, 3);

        assertEquals(1, batch.count());
        assertEquals(Verdict.VALID, batch.reader().inspect());
    }

    @Test
    void symbolsLargerThanTheRecordLimitAreRefused() {
        RecordBatch batch = new RecordBatch();
        byte[] big = new byte[RecordFormat.WRITE_LIMIT_BYTES];

        assertThrows(IllegalArgumentException.class, () -> batch.symbol(1, big, 0, big.length));
    }
}
