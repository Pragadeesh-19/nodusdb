package io.nodusdb.watch;

import io.nodusdb.chain.ChainLayout;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.RecordStream;
import io.nodusdb.log.RecordsGoneException;
import io.nodusdb.objectstore.ForwardingObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ChainFetch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainRecordSourceTest {

    private static final int TUPLES = 3;
    private static final int LSNS_PER_OBJECT = TUPLES + 1;

    private final MemoryObjectStore memory = new MemoryObjectStore();
    private final AtomicInteger ranges = new AtomicInteger();
    private final ForwardingObjectStore store = new ForwardingObjectStore(memory) {
        @Override
        public Optional<byte[]> getRange(String key, long offset, int length) {
            ranges.incrementAndGet();
            return super.getRange(key, offset, length);
        }
    };
    private final ChainBuilder chain = new ChainBuilder(memory, 9);
    private final ChainRecordSource source = new ChainRecordSource(store,
            new ChainFetch(store, ChainBuilder.keyring(), ChainFetch.Trust.REQUIRED));

    private void build(int objects) {
        chain.snapshotRef(0, 1);
        for (int i = 0; i < objects; i++) {
            chain.records(TUPLES);
            if (i % 7 == 3) {
                chain.snapshotRef(chain.lastLsn(), 1);
            }
        }
    }

    private static long readThrough(RecordStream stream, long afterLsn) throws IOException {
        long expectedFirst = -1;
        long last = afterLsn;
        while (true) {
            LogTailReader.Batch batch = stream.next(1 << 20);
            if (batch == null) {
                return last;
            }
            if (expectedFirst < 0) {
                assertTrue(batch.lsnFirst() <= afterLsn + 1 && batch.lsnLast() >= afterLsn + 1,
                        "the first batch " + batch.lsnFirst() + ".." + batch.lsnLast() + " holds LSN " + (afterLsn + 1));
            } else {
                assertEquals(expectedFirst, batch.lsnFirst());
            }
            expectedFirst = batch.lsnLast() + 1;
            last = batch.lsnLast();
        }
    }

    @Test
    void everyCursorIsLocatedAndStreamedContiguouslyToTheEnd() throws IOException {
        build(25);
        long end = chain.lastLsn();

        for (long after = 0; after < end; after++) {
            try (RecordStream stream = source.open(after).orElseThrow()) {
                assertEquals(end, readThrough(stream, after), "after " + after);
            }
        }
    }

    @Test
    void locatingTakesLogarithmicallyManyRangeReads() {
        build(500);

        ranges.set(0);
        source.open(chain.lastLsn() / 2).orElseThrow();

        assertTrue(ranges.get() <= 40, "locating used " + ranges.get() + " range reads");
    }

    @Test
    void aCursorAtTheHeadWaitsForTheNextObject() throws IOException {
        build(3);
        RecordStream stream = source.open(chain.lastLsn()).orElseThrow();

        assertNull(stream.next(1 << 20));
        chain.records(TUPLES);

        LogTailReader.Batch next = stream.next(1 << 20);
        assertEquals(chain.lastLsn() - TUPLES, next.lsnFirst());
        assertEquals(chain.lastLsn(), next.lsnLast());
    }

    @Test
    void aCursorBeforeTheOldestObjectIsNotHeld() {
        build(10);
        for (long seq = 1; seq <= 4; seq++) {
            memory.delete(ChainLayout.chainKey(seq));
        }

        assertTrue(source.open(0).isEmpty());
        assertTrue(source.open(3L * LSNS_PER_OBJECT - 1).isEmpty());
        assertTrue(source.open(3L * LSNS_PER_OBJECT).isPresent());
        assertTrue(source.open(chain.lastLsn() - 1).isPresent());
    }

    @Test
    void anEmptyBucketHoldsNothing() {
        assertTrue(source.open(0).isEmpty());
        assertTrue(source.open(-1).isEmpty());
    }

    @Test
    void retentionDeletingTheNextObjectEndsTheStream() throws IOException {
        build(6);
        RecordStream stream = source.open(0).orElseThrow();
        stream.next(1 << 20);
        for (long seq = 1; seq <= 5; seq++) {
            memory.delete(ChainLayout.chainKey(seq));
        }

        assertThrows(RecordsGoneException.class, () -> readThrough(stream, LSNS_PER_OBJECT));
    }

    @Test
    void aHoleInTheChainEndsTheStreamInsteadOfWaitingForever() throws IOException {
        build(6);
        RecordStream stream = source.open(0).orElseThrow();
        stream.next(1 << 20);
        memory.delete(ChainLayout.chainKey(3));

        assertThrows(RecordsGoneException.class, () -> readThrough(stream, LSNS_PER_OBJECT));
    }
}
