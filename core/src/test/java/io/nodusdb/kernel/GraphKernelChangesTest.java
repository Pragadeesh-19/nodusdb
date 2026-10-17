package io.nodusdb.kernel;

import io.nodusdb.kernel.changes.ChangeBuffer;
import io.nodusdb.kernel.changes.ChangeEntry;
import io.nodusdb.kernel.changes.ChangeRing;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelChangesTest {

    private static ChangeBuffer read(ChangeRing ring, long after) {
        ChangeBuffer out = new ChangeBuffer();
        assertTrue(ring.read(after, Long.MAX_VALUE, Integer.MAX_VALUE, out));
        return out;
    }

    @Test
    void theRingStartsAtTheAppliedLsnAndIsCreatedOnce() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(1, 2);

        assertNull(kernel.changes());
        ChangeRing ring = kernel.enableChanges(ChangeRing.MIN_CAPACITY);

        assertSame(ring, kernel.enableChanges(ChangeRing.DEFAULT_CAPACITY));
        assertSame(ring, kernel.changes());
        assertEquals(kernel.appliedLsn(), ring.publishedLsn());
        assertEquals(0, read(ring, kernel.appliedLsn()).size());
    }

    @Test
    void anInvalidCapacityLeavesTheKernelWithoutARing() {
        GraphKernel kernel = new GraphKernel();

        assertThrows(IllegalArgumentException.class, () -> kernel.enableChanges(3));
        assertNull(kernel.changes());
    }

    @Test
    void singleWritesOnBothPathsAreRecorded() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(1, 2);
        long start = kernel.appliedLsn();
        ChangeRing ring = kernel.enableChanges(ChangeRing.MIN_CAPACITY);

        kernel.addEdge(1, 3);
        kernel.addEdge(1_000, 5_000);
        kernel.removeEdge(1, 2);

        ChangeBuffer out = read(ring, start);
        assertEquals(6, out.size());
        assertEquals(ChangeEntry.ADD, out.kind(0));
        assertEquals(3, ChangeEntry.subject(out.payload(0)));
        assertEquals(ChangeEntry.ADD, out.kind(2));
        assertEquals(1_000, ChangeEntry.object(out.payload(2)));
        assertEquals(ChangeEntry.REMOVE, out.kind(4));
        assertEquals(kernel.appliedLsn(), out.lastCommitLsn());
    }

    @Test
    void aWriteThatChangesNothingRecordsNothing() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(1, 2);
        ChangeRing ring = kernel.enableChanges(ChangeRing.MIN_CAPACITY);
        long before = ring.publishedLsn();

        kernel.addEdge(1, 2);
        kernel.removeEdge(7, 8);

        assertEquals(before, ring.publishedLsn());
    }

    @Test
    void batchesRecordOneCommitPerEdge() {
        GraphKernel kernel = new GraphKernel();
        ChangeRing ring = kernel.enableChanges(ChangeRing.MIN_CAPACITY);
        long start = ring.publishedLsn();

        kernel.addEdges(new long[]{1, 2, 3, 4, 5, 6}, 3);

        ChangeBuffer out = read(ring, start);
        assertEquals(3, out.changes());
        assertEquals(6, out.size());
        assertEquals(kernel.appliedLsn(), out.lastCommitLsn());
    }

    @Test
    void aTransactionIsRecordedWholeWithItsCommit() {
        GraphKernel kernel = new GraphKernel();
        ChangeRing ring = kernel.enableChanges(ChangeRing.MIN_CAPACITY);
        long start = ring.publishedLsn();
        RecordBatch batch = new RecordBatch();
        batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);
        batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 3);
        batch.commit();

        long commit = kernel.commit(batch);

        ChangeBuffer out = read(ring, start);
        assertEquals(3, out.size());
        assertEquals(2, out.changes());
        assertEquals(commit, out.lsn(2));
        assertEquals(ChangeEntry.COMMIT, out.kind(2));
        assertEquals(commit, ring.publishedLsn());
    }

    @Test
    void replicatedTransactionsAreRecorded() {
        GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
        ChangeRing ring = replica.enableChanges(ChangeRing.MIN_CAPACITY);
        ReplicatedStream stream = new ReplicatedStream();
        stream.add(1, 2);
        stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 2, 0, 0, 3);
            batch.tuple(RecordType.TUPLE_REMOVE, 1, 0, 0, 2);
        });

        stream.drain().applyTo(replica);

        ChangeBuffer out = read(ring, 0);
        assertEquals(5, out.size());
        assertEquals(3, out.changes());
        assertEquals(stream.lastLsn(), out.lastCommitLsn());
        assertEquals(stream.lastCommitMicros(), out.payload(4));
    }

    @Test
    void closingTheKernelClosesItsRingAndRefusesANewOne() {
        GraphKernel kernel = new GraphKernel();
        ChangeRing ring = kernel.enableChanges(ChangeRing.MIN_CAPACITY);

        kernel.close();

        assertTrue(ring.isClosed());
        assertThrows(IllegalStateException.class, () -> kernel.enableChanges(ChangeRing.MIN_CAPACITY));
    }
}
