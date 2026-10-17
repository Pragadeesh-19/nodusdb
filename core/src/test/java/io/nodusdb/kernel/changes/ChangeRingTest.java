package io.nodusdb.kernel.changes;

import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChangeRingTest {

    private static final long MICROS = 1_700_000_000_000_000L;
    private static final byte[] PSEUDONYM = "erased:0123456789abcdef\0".getBytes(StandardCharsets.US_ASCII);
    private static final long STRESS_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final long STRESS_READS = 20_000;

    private static final class Writer {

        private final ChangeRing ring;
        private final ConcurrentSkipListMap<Long, Long> starts;
        private long nextLsn;

        Writer(ChangeRing ring, long firstLsn) {
            this(ring, firstLsn, null);
        }

        Writer(ChangeRing ring, long firstLsn, ConcurrentSkipListMap<Long, Long> starts) {
            this.ring = ring;
            this.nextLsn = firstLsn;
            this.starts = starts;
        }

        long single(RecordType type, int object, int relation, int subject) {
            RecordBatch batch = new RecordBatch();
            batch.autocommitTuple(type, object, relation, 0, subject);
            return seal(batch);
        }

        long numbered() {
            return single(RecordType.TUPLE_ADD, objectOf(nextLsn), 1, subjectOf(nextLsn));
        }

        long transaction(int tuples) {
            RecordBatch batch = new RecordBatch();
            for (int i = 0; i < tuples; i++) {
                batch.tuple(RecordType.TUPLE_ADD, objectOf(nextLsn + i), 1, 0, subjectOf(nextLsn + i));
            }
            batch.commit();
            return seal(batch);
        }

        long mixed() {
            RecordBatch batch = new RecordBatch();
            byte[] name = "user:alice".getBytes(StandardCharsets.UTF_8);
            batch.symbol(7, name, 0, name.length);
            batch.tuple(RecordType.TUPLE_REMOVE, 3, 2, 5, 7);
            batch.epoch(4, 9, 11);
            batch.erase(7, PSEUDONYM);
            batch.schema(6, new byte[32], new int[]{1}, new int[]{1}, new int[]{2}, new int[]{0},
                    "schema 1".getBytes(StandardCharsets.UTF_8));
            batch.commit();
            return seal(batch);
        }

        long seal(RecordBatch batch) {
            long last = nextLsn + batch.count() - 1;
            if (starts != null) {
                starts.put(nextLsn, last);
            }
            ring.record(batch.bytes(), 0, batch.size(), last, MICROS + last);
            nextLsn = last + 1;
            return last;
        }

        void skipTo(long lsn) {
            nextLsn = lsn;
        }
    }

    private static int objectOf(long lsn) {
        return (int) (lsn % 1_000);
    }

    private static int subjectOf(long lsn) {
        return (int) (lsn % 997);
    }

    private static ChangeRing ring() {
        return new ChangeRing(ChangeRing.MIN_CAPACITY, 10);
    }

    @Test
    void anEmptyRingHoldsItsStartAndNothingBefore() {
        ChangeRing ring = ring();
        ChangeBuffer out = new ChangeBuffer();

        assertTrue(ring.read(10, Long.MAX_VALUE, 100, out));
        assertEquals(0, out.size());
        assertTrue(ring.read(25, Long.MAX_VALUE, 100, out));
        assertFalse(ring.read(9, Long.MAX_VALUE, 100, out));
        assertEquals(10, ring.publishedLsn());
        assertEquals(10, ring.servesFromLsn());
        assertEquals(ChangeRing.MIN_CAPACITY, ring.capacity());
    }

    @Test
    void aSingleWriteBecomesATupleAndItsCommit() {
        ChangeRing ring = ring();
        long lsn = new Writer(ring, 11).single(RecordType.TUPLE_ADD, 4, 3, 8);
        ChangeBuffer out = new ChangeBuffer();

        assertTrue(ring.read(10, Long.MAX_VALUE, 100, out));

        assertEquals(2, out.size());
        assertEquals(1, out.changes());
        assertEquals(lsn, out.lsn(0));
        assertEquals(ChangeEntry.ADD, out.kind(0));
        assertEquals(3, ChangeEntry.relation(out.meta(0)));
        assertEquals(0, ChangeEntry.subjectRelation(out.meta(0)));
        assertEquals(4, ChangeEntry.object(out.payload(0)));
        assertEquals(8, ChangeEntry.subject(out.payload(0)));
        assertEquals(ChangeEntry.COMMIT, out.kind(1));
        assertEquals(lsn, out.lsn(1));
        assertEquals(MICROS + lsn, out.payload(1));
        assertEquals(lsn, ring.publishedLsn());
    }

    @Test
    void everyRecordTypeOfATransactionIsKeptButSymbols() {
        ChangeRing ring = ring();
        long commit = new Writer(ring, 11).mixed();
        ChangeBuffer out = new ChangeBuffer();

        assertTrue(ring.read(10, Long.MAX_VALUE, 100, out));

        assertEquals(5, out.size());
        assertEquals(4, out.changes());
        assertEquals(ChangeEntry.REMOVE, out.kind(0));
        assertEquals(12, out.lsn(0));
        assertEquals(2, ChangeEntry.relation(out.meta(0)));
        assertEquals(5, ChangeEntry.subjectRelation(out.meta(0)));
        assertEquals(ChangeEntry.EPOCH, out.kind(1));
        assertEquals(4, out.payload(1));
        assertEquals(ChangeEntry.ERASE, out.kind(2));
        assertEquals(7, out.payload(2));
        assertEquals(ChangeEntry.SCHEMA, out.kind(3));
        assertEquals(6, out.payload(3));
        assertEquals(ChangeEntry.COMMIT, out.kind(4));
        assertEquals(commit, out.lsn(4));
    }

    @Test
    void aCursorInsideATransactionGetsTheWholeTransaction() {
        ChangeRing ring = ring();
        new Writer(ring, 11).transaction(3);
        ChangeBuffer out = new ChangeBuffer();

        assertTrue(ring.read(12, Long.MAX_VALUE, 100, out));

        assertEquals(4, out.size());
        assertEquals(11, out.lsn(0));
        assertEquals(14, out.lastCommitLsn());
    }

    @Test
    void aCursorAtACommitStartsAtTheNextTransaction() {
        ChangeRing ring = ring();
        Writer writer = new Writer(ring, 11);
        long first = writer.transaction(2);
        long second = writer.single(RecordType.TUPLE_ADD, 1, 1, 2);
        ChangeBuffer out = new ChangeBuffer();

        assertTrue(ring.read(first, Long.MAX_VALUE, 100, out));

        assertEquals(2, out.size());
        assertEquals(second, out.lsn(0));
        out.clear();
        assertTrue(ring.read(second, Long.MAX_VALUE, 100, out));
        assertEquals(0, out.size());
    }

    @Test
    void theBoundStopsBeforeATransactionThatCommitsAfterIt() {
        ChangeRing ring = ring();
        Writer writer = new Writer(ring, 11);
        long first = writer.single(RecordType.TUPLE_ADD, 1, 1, 2);
        writer.transaction(3);
        ChangeBuffer out = new ChangeBuffer();

        assertTrue(ring.read(10, first + 2, 100, out));

        assertEquals(2, out.size());
        assertEquals(first, out.lastCommitLsn());
    }

    @Test
    void theLimitEndsOnATransactionBoundaryAndNeverSplitsOne() {
        ChangeRing ring = ring();
        Writer writer = new Writer(ring, 11);
        long big = writer.transaction(5);
        writer.single(RecordType.TUPLE_ADD, 1, 1, 2);
        ChangeBuffer out = new ChangeBuffer();

        assertTrue(ring.read(10, Long.MAX_VALUE, 2, out));

        assertEquals(5, out.changes());
        assertEquals(big, out.lastCommitLsn());
        out.clear();
        assertTrue(ring.read(10, Long.MAX_VALUE, 6, out));
        assertEquals(6, out.changes());
    }

    @Test
    void anEvictedCursorIsNotHeldAndARecentOneIs() {
        ChangeRing ring = ring();
        Writer writer = new Writer(ring, 11);
        long early = writer.single(RecordType.TUPLE_ADD, 1, 1, 1);
        long last = 0;
        for (int i = 0; i < ChangeRing.MIN_CAPACITY; i++) {
            last = writer.numbered();
        }
        ChangeBuffer out = new ChangeBuffer();

        assertFalse(ring.read(early, Long.MAX_VALUE, 10, out));
        assertEquals(0, out.size());
        assertTrue(ring.read(last - 3, Long.MAX_VALUE, 10, out));
        assertEquals(3, out.changes());
        assertEquals(last, out.lastCommitLsn());
        assertTrue(ring.servesFromLsn() > early);
    }

    @Test
    void aGapInTheLsnsRestartsTheSpanAtTheGap() {
        ChangeRing ring = ring();
        Writer writer = new Writer(ring, 11);
        long before = writer.single(RecordType.TUPLE_ADD, 1, 1, 1);
        writer.skipTo(40);
        long after = writer.single(RecordType.TUPLE_ADD, 2, 1, 2);
        ChangeBuffer out = new ChangeBuffer();

        assertFalse(ring.read(before, Long.MAX_VALUE, 10, out));
        assertFalse(ring.read(10, Long.MAX_VALUE, 10, out));
        assertTrue(ring.read(39, Long.MAX_VALUE, 10, out));
        assertEquals(after, out.lastCommitLsn());
        assertEquals(39, ring.servesFromLsn());
    }

    @Test
    void aTransactionLargerThanTheRingIsNotHeldButLaterOnesAre() {
        ChangeRing ring = ring();
        Writer writer = new Writer(ring, 11);
        long huge = writer.transaction(ChangeRing.MIN_CAPACITY);
        long next = writer.single(RecordType.TUPLE_ADD, 1, 1, 1);
        ChangeBuffer out = new ChangeBuffer();

        assertFalse(ring.read(10, Long.MAX_VALUE, 10, out));
        assertTrue(ring.read(huge, Long.MAX_VALUE, 10, out));
        assertEquals(next, out.lastCommitLsn());
        assertEquals(huge, ring.servesFromLsn());
    }

    @Test
    void aDecodedTupleIsRecordedExactlyAsItsRecordWouldBe() {
        ChangeRing parsed = ring();
        ChangeRing decoded = ring();
        int[][] tuples = {{ChangeEntry.ADD, 4, 3, 0, 8}, {ChangeEntry.REMOVE, Integer.MAX_VALUE, 65_535, 65_535, 0},
                {ChangeEntry.ADD, 0, 1, 2, Integer.MAX_VALUE}};
        long[] lsns = {11, 12, 40};
        for (int i = 0; i < tuples.length; i++) {
            int[] tuple = tuples[i];
            RecordBatch batch = new RecordBatch();
            batch.autocommitTuple(tuple[0] == ChangeEntry.ADD ? RecordType.TUPLE_ADD : RecordType.TUPLE_REMOVE,
                    tuple[1], tuple[2], tuple[3], tuple[4]);
            parsed.record(batch.bytes(), 0, batch.size(), lsns[i], MICROS + lsns[i]);
            decoded.recordTuple(tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], lsns[i], MICROS + lsns[i]);
        }

        for (long after : new long[]{10, 11, 39}) {
            ChangeBuffer expected = new ChangeBuffer();
            ChangeBuffer actual = new ChangeBuffer();
            assertEquals(parsed.read(after, Long.MAX_VALUE, 10, expected),
                    decoded.read(after, Long.MAX_VALUE, 10, actual), "after " + after);
            assertEquals(expected.size(), actual.size(), "after " + after);
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.lsn(i), actual.lsn(i), "after " + after + " entry " + i);
                assertEquals(expected.meta(i), actual.meta(i), "after " + after + " entry " + i);
                assertEquals(expected.payload(i), actual.payload(i), "after " + after + " entry " + i);
            }
        }
        assertEquals(parsed.publishedLsn(), decoded.publishedLsn());
        assertEquals(parsed.servesFromLsn(), decoded.servesFromLsn());
        assertEquals(39, decoded.servesFromLsn());
    }

    @Test
    void recordTupleRefusesAChangeThatIsNotATupleWithAPositiveLsn() {
        ChangeRing ring = ring();

        assertThrows(IllegalArgumentException.class, () -> ring.recordTuple(ChangeEntry.COMMIT, 1, 1, 0, 2, 11, 0));
        assertThrows(IllegalArgumentException.class, () -> ring.recordTuple(ChangeEntry.SCHEMA, 1, 1, 0, 2, 11, 0));
        assertThrows(IllegalArgumentException.class, () -> ring.recordTuple(ChangeEntry.ADD, 1, 1, 0, 2, 0, 0));
        assertEquals(10, ring.publishedLsn());
    }

    @Test
    void recordRefusesInputThatIsNotOneWholeTransaction() {
        ChangeRing ring = ring();
        RecordBatch batch = new RecordBatch();
        batch.tuple(RecordType.TUPLE_ADD, 1, 1, 0, 2);
        batch.commit();
        int withoutCommit = batch.size() - 48;

        assertThrows(IllegalArgumentException.class, () -> ring.record(batch.bytes(), 0, withoutCommit, 11, MICROS));
        assertThrows(IllegalArgumentException.class, () -> ring.record(batch.bytes(), 0, 0, 11, MICROS));
        assertThrows(IllegalArgumentException.class, () -> ring.record(batch.bytes(), 0, batch.size(), 1, MICROS));
        assertEquals(10, ring.publishedLsn());
    }

    @Test
    void recordsAreNumberedFromTheLastLsnTheCallerGives() {
        ChangeRing ring = ring();
        RecordBatch unsealed = new RecordBatch();
        unsealed.tuple(RecordType.TUPLE_ADD, 1, 1, 0, 2);
        unsealed.tuple(RecordType.TUPLE_ADD, 1, 1, 0, 3);
        unsealed.commit();

        ring.record(unsealed.bytes(), 0, unsealed.size(), 13, 0);

        ChangeBuffer out = new ChangeBuffer();
        assertTrue(ring.read(10, Long.MAX_VALUE, 10, out));
        assertEquals(11, out.lsn(0));
        assertEquals(12, out.lsn(1));
        assertEquals(13, out.lsn(2));
        assertEquals(0, out.payload(2));
        assertEquals(13, ring.publishedLsn());
    }

    @Test
    void theCapacityMustBeAPowerOfTwoInRange() {
        assertThrows(IllegalArgumentException.class, () -> new ChangeRing(ChangeRing.MIN_CAPACITY + 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new ChangeRing(ChangeRing.MIN_CAPACITY / 2, 0));
        assertThrows(IllegalArgumentException.class, () -> new ChangeRing(ChangeRing.MAX_CAPACITY * 2, 0));
        assertThrows(IllegalArgumentException.class, () -> new ChangeRing(ChangeRing.MIN_CAPACITY, -1));
        assertThrows(IllegalArgumentException.class, () -> ring().read(10, 20, 0, new ChangeBuffer()));
    }

    @Test
    void aWaitEndsAtTheTimeoutWhenNothingIsPublished() {
        ChangeRing ring = ring();

        long started = System.nanoTime();
        assertFalse(ring.awaitBeyond(10, TimeUnit.MILLISECONDS.toNanos(30)));

        assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(25));
        assertFalse(ring.awaitBeyond(10, 0));
    }

    @Test
    void aWaitEndsAtTheNextPublish() throws InterruptedException {
        ChangeRing ring = ring();
        Writer writer = new Writer(ring, 11);
        AtomicReference<Boolean> woke = new AtomicReference<>();
        Thread waiter = new Thread(() -> woke.set(ring.awaitBeyond(10, TimeUnit.SECONDS.toNanos(60))));
        waiter.start();
        awaitWaiting(waiter);

        writer.single(RecordType.TUPLE_ADD, 1, 1, 1);
        waiter.join(TimeUnit.SECONDS.toMillis(60));

        assertEquals(Boolean.TRUE, woke.get());
        assertTrue(ring.awaitBeyond(10, 0));
    }

    @Test
    void closeWakesAWaiter() throws InterruptedException {
        ChangeRing ring = ring();
        AtomicReference<Boolean> woke = new AtomicReference<>();
        Thread waiter = new Thread(() -> woke.set(ring.awaitBeyond(10, TimeUnit.SECONDS.toNanos(60))));
        waiter.start();
        awaitWaiting(waiter);

        ring.close();
        waiter.join(TimeUnit.SECONDS.toMillis(60));

        assertFalse(waiter.isAlive());
        assertEquals(Boolean.FALSE, woke.get());
        assertTrue(ring.isClosed());
    }

    @Test
    void readersRacingAWrappingWriterSeeOnlyTheTransactionsAfterTheirCursor() throws Exception {
        ChangeRing ring = new ChangeRing(ChangeRing.MIN_CAPACITY, 0);
        ConcurrentSkipListMap<Long, Long> starts = new ConcurrentSkipListMap<>();
        AtomicBoolean done = new AtomicBoolean();
        AtomicLong verifiedReads = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            Writer writer = new Writer(ring, 1, starts);
            int round = 0;
            while (!done.get()) {
                if (round % 3 == 0) {
                    writer.transaction(1 + round % 7);
                } else {
                    writer.numbered();
                }
                round++;
            }
        });
        List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < 3; r++) {
            readers.add(new Thread(() -> {
                ChangeBuffer out = new ChangeBuffer();
                try {
                    long reads = 0;
                    while (!done.get()) {
                        long newest = ring.publishedLsn();
                        long jitter = System.nanoTime() & 0x3F;
                        long after = (reads++ & 1) == 0
                                ? Math.min(newest - 1, ring.servesFromLsn() + jitter)
                                : Math.max(0, newest - 1 - (System.nanoTime() & 0x1FFFF));
                        after = Math.max(0, after);
                        out.clear();
                        if (ring.read(after, Long.MAX_VALUE, 64, out)) {
                            verify(out, after, newest, starts);
                            verifiedReads.incrementAndGet();
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                    done.set(true);
                }
            }));
        }
        producer.start();
        readers.forEach(Thread::start);
        long deadline = System.nanoTime() + STRESS_DEADLINE_NANOS;
        while (!done.get() && System.nanoTime() < deadline
                && (ring.publishedLsn() < 4L * ChangeRing.MIN_CAPACITY || verifiedReads.get() < STRESS_READS)) {
            Thread.onSpinWait();
        }
        done.set(true);
        producer.join();
        for (Thread reader : readers) {
            reader.join();
        }

        assertNull(failure.get());
        assertTrue(ring.publishedLsn() >= 4L * ChangeRing.MIN_CAPACITY, "the producer wrapped the ring");
        assertTrue(verifiedReads.get() >= STRESS_READS, "readers verified " + verifiedReads.get() + " reads");
    }

    private static void verify(ChangeBuffer out, long after, long published, ConcurrentSkipListMap<Long, Long> starts) {
        if (out.size() == 0) {
            assertTrue(published <= after, "LSN " + published + " was published, so a read after " + after
                    + " cannot be empty");
            return;
        }
        long expectedStart = starts.floorKey(after + 1);
        for (int i = 0; i < out.size(); i++) {
            long lsn = out.lsn(i);
            if (i == 0 || out.kind(i - 1) == ChangeEntry.COMMIT) {
                assertEquals(expectedStart, lsn, "transaction starts in order after " + after);
            }
            if (out.kind(i) == ChangeEntry.COMMIT) {
                assertEquals(starts.get(expectedStart), lsn, "the commit closes its transaction");
                assertTrue(lsn > after, "every delivered transaction commits after the cursor");
                assertEquals(MICROS + lsn, out.payload(i), "the commit time belongs to its LSN");
                expectedStart = lsn + 1;
                continue;
            }
            assertEquals(ChangeEntry.ADD, out.kind(i));
            assertEquals(objectOf(lsn), ChangeEntry.object(out.payload(i)));
            assertEquals(subjectOf(lsn), ChangeEntry.subject(out.payload(i)));
        }
        assertEquals(ChangeEntry.COMMIT, out.kind(out.size() - 1));
    }

    private static void awaitWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (thread.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(Thread.State.TIMED_WAITING, thread.getState());
    }
}
