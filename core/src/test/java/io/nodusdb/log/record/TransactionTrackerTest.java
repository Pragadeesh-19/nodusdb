package io.nodusdb.log.record;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionTrackerTest {

    private static List<RecordReader> records(RecordBatch batch) {
        List<RecordReader> records = new ArrayList<>();
        RecordReader walker = batch.reader();
        while (walker.hasRecord()) {
            records.add(new RecordReader().wrap(batch.bytes(), walker.position(),
                    walker.position() + walker.length()));
            walker.advance();
        }
        return records;
    }

    private static RecordBatch transaction(long firstLsn, int tuples) {
        RecordBatch batch = new RecordBatch();
        for (int i = 0; i < tuples; i++) {
            batch.tuple(RecordType.TUPLE_ADD, i, 1, 0, i + 1);
        }
        batch.commit();
        batch.seal(firstLsn, RecordFixtures.COMMIT_MICROS);
        return batch;
    }

    @Test
    void aSingleRecordCommitCompletesAtOnce() {
        TransactionTracker tracker = new TransactionTracker();
        RecordBatch batch = RecordFixtures.autocommit(RecordType.TUPLE_ADD, 1, 2, 0, 3);

        assertTrue(tracker.accept(records(batch).get(0)));
        assertFalse(tracker.open());
        assertEquals(0, tracker.records());
    }

    @Test
    void aTransactionCompletesOnlyAtItsCommitRecord() {
        TransactionTracker tracker = new TransactionTracker();
        List<RecordReader> records = records(transaction(500, 3));

        assertEquals(4, records.size());
        for (int i = 0; i < 3; i++) {
            assertFalse(tracker.accept(records.get(i)), "record " + i);
            assertTrue(tracker.open());
            assertEquals(i + 1, tracker.records());
            assertEquals(500, tracker.firstLsn());
        }
        assertTrue(tracker.accept(records.get(3)));
        assertFalse(tracker.open());
        assertEquals(0, tracker.records());
    }

    @Test
    void aTrackerIsReadyForTheNextTransactionAfterACommit() {
        TransactionTracker tracker = new TransactionTracker();

        for (long first = 10; first < 40; first += 10) {
            List<RecordReader> records = records(transaction(first, 2));
            assertFalse(tracker.accept(records.get(0)));
            assertFalse(tracker.accept(records.get(1)));
            assertTrue(tracker.accept(records.get(2)));
        }
    }

    @Test
    void aLoneCommitRecordIsRefused() {
        TransactionTracker tracker = new TransactionTracker();
        List<RecordReader> records = records(transaction(500, 2));

        assertThrows(MalformedTransactionException.class, () -> tracker.accept(records.get(2)));
    }

    @Test
    void aCommitThatCountsTheWrongNumberOfRecordsIsRefused() {
        TransactionTracker tracker = new TransactionTracker();
        List<RecordReader> records = records(transaction(500, 3));
        tracker.accept(records.get(0));
        tracker.accept(records.get(1));

        MalformedTransactionException refused = assertThrows(MalformedTransactionException.class,
                () -> tracker.accept(records.get(3)));

        assertEquals("the commit record does not match the records before it", refused.getMessage());
    }

    @Test
    void aCommitFromAnotherTransactionIsRefused() {
        TransactionTracker tracker = new TransactionTracker();
        List<RecordReader> first = records(transaction(500, 2));
        List<RecordReader> second = records(transaction(900, 2));
        tracker.accept(first.get(0));
        tracker.accept(first.get(1));

        assertThrows(MalformedTransactionException.class, () -> tracker.accept(second.get(2)));
    }

    @Test
    void aCommitWhoseOwnFirstLsnIsWrongIsRefusedEvenWhenPositionAndCountAreRight() {
        TransactionTracker tracker = new TransactionTracker();
        RecordBatch batch = transaction(500, 2);
        List<RecordReader> records = records(batch);
        int commitStart = records.get(2).position();
        batch.bytes().putLong(commitStart + RecordFormat.COMMIT_FIRST_LSN_OFFSET, 499);
        tracker.accept(records.get(0));
        tracker.accept(records.get(1));

        assertThrows(MalformedTransactionException.class, () -> tracker.accept(records.get(2)));
    }

    @Test
    void aCommitWhoseCountIsWrongEvenWithTheRightFirstLsnIsRefused() {
        TransactionTracker tracker = new TransactionTracker();
        RecordBatch batch = transaction(500, 2);
        List<RecordReader> records = records(batch);
        int commitStart = records.get(2).position();
        batch.bytes().putInt(commitStart + RecordFormat.COMMIT_COUNT_OFFSET, 3);
        tracker.accept(records.get(0));
        tracker.accept(records.get(1));

        assertThrows(MalformedTransactionException.class, () -> tracker.accept(records.get(2)));
    }

    @Test
    void aSingleRecordCommitInsideAnOpenTransactionIsRefused() {
        TransactionTracker tracker = new TransactionTracker();
        tracker.accept(records(transaction(500, 2)).get(0));

        MalformedTransactionException refused = assertThrows(MalformedTransactionException.class,
                () -> tracker.accept(records(RecordFixtures.autocommit(RecordType.TUPLE_ADD, 1, 2, 0, 3)).get(0)));

        assertEquals("a single-record commit interrupts an open transaction", refused.getMessage());
    }

    @Test
    void resettingForgetsAnOpenTransaction() {
        TransactionTracker tracker = new TransactionTracker();
        tracker.accept(records(transaction(500, 2)).get(0));

        tracker.reset();

        assertFalse(tracker.open());
        assertTrue(tracker.accept(records(RecordFixtures.autocommit(RecordType.TUPLE_ADD, 1, 2, 0, 3)).get(0)));
    }

    @Test
    void everyRecordTypeInsideOneTransactionIsCountedAndTheCommitMatches() {
        TransactionTracker tracker = new TransactionTracker();
        List<RecordReader> records = records(RecordFixtures.everyRecordType());

        for (int i = 0; i < records.size() - 1; i++) {
            assertFalse(tracker.accept(records.get(i)), "record " + i);
        }
        assertTrue(tracker.accept(records.get(records.size() - 1)));
    }
}
