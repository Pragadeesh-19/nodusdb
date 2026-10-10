package io.nodusdb.kernel;

import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordFormat;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaKernelValidationTest {

    private final GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
    private final ReplicatedStream stream = new ReplicatedStream();

    private ReplicatedStream.Chunk seed() {
        ReplicatedStream.Chunk chunk = stream.add(1, 2).add(3, 4).drain();
        chunk.applyTo(replica);
        return chunk;
    }

    private void assertRejectedWithNothingChanged(ReplicatedStream.Chunk chunk) {
        GraphDigest before = GraphDigest.of(replica);
        long applied = replica.appliedLsn();

        assertThrows(IllegalArgumentException.class, () -> chunk.applyTo(replica));

        assertEquals(before, GraphDigest.of(replica));
        assertEquals(applied, replica.appliedLsn());
    }

    @Test
    void aGapInTheLsnsIsRejected() {
        seed();
        ReplicatedStream skipped = new ReplicatedStream(4);

        assertRejectedWithNothingChanged(skipped.add(5, 6).drain());
        assertFalse(replica.hasEdge(5, 6));
    }

    @Test
    void aRepeatOfAnAppliedRangeIsRejected() {
        ReplicatedStream.Chunk applied = seed();

        assertRejectedWithNothingChanged(applied);
    }

    @Test
    void aRangeOverlappingTheAppliedPositionIsRejected() {
        seed();
        ReplicatedStream overlapping = new ReplicatedStream(2);

        assertRejectedWithNothingChanged(overlapping.add(7, 8).add(9, 10).drain());
    }

    @Test
    void theFirstLsnArgumentMustMatchTheRecords() {
        seed();
        ReplicatedStream elsewhere = new ReplicatedStream(5);
        ReplicatedStream.Chunk chunk = elsewhere.add(5, 6).drain();

        assertRejectedWithNothingChanged(new ReplicatedStream.Chunk(chunk.records(), 3, 3));
    }

    @Test
    void theLastLsnArgumentMustMatchTheRecords() {
        seed();
        ReplicatedStream.Chunk chunk = stream.add(5, 6).add(7, 8).drain();

        assertRejectedWithNothingChanged(new ReplicatedStream.Chunk(chunk.records(), chunk.first(), chunk.last() + 1));
        assertRejectedWithNothingChanged(new ReplicatedStream.Chunk(chunk.records(), chunk.first(), chunk.last() - 1));
    }

    @Test
    void anEmptyRangeIsRejected() {
        seed();

        assertThrows(IllegalArgumentException.class, () -> replica.applyReplicated(new byte[0], 3, 2));
        assertEquals(2, replica.appliedLsn());
    }

    @Test
    void nullRecordsAreRejected() {
        assertThrows(NullPointerException.class, () -> replica.applyReplicated(null, 1, 1));
    }

    @Test
    void aCutShortRecordAppliesNothingOfTheChunk() {
        seed();
        ReplicatedStream.Chunk chunk = stream.add(5, 6).add(7, 8).drain();
        byte[] cut = Arrays.copyOf(chunk.records(), chunk.records().length - 3);

        assertRejectedWithNothingChanged(chunk.withRecords(cut));
        assertFalse(replica.hasEdge(5, 6));
    }

    @Test
    void aFlippedByteInALaterTransactionAppliesNothingOfTheChunk() {
        seed();
        ReplicatedStream.Chunk chunk = stream.add(5, 6).add(7, 8).drain();
        byte[] damaged = chunk.records().clone();
        damaged[damaged.length - 20] ^= 0x01;

        assertRejectedWithNothingChanged(chunk.withRecords(damaged));
        assertFalse(replica.hasEdge(5, 6));
        assertFalse(replica.hasEdge(7, 8));
    }

    @Test
    void everySingleByteFlipInAChunkIsRejectedWithNothingApplied() {
        seed();
        ReplicatedStream.Chunk chunk = stream.add(5, 6).transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 7, 0, 0, 8);
            batch.tuple(RecordType.TUPLE_ADD, 9, 0, 0, 10);
        }).drain();
        GraphDigest before = GraphDigest.of(replica);

        for (int index = 0; index < chunk.records().length; index++) {
            byte[] damaged = chunk.records().clone();
            damaged[index] ^= 0x40;
            ReplicatedStream.Chunk bad = chunk.withRecords(damaged);
            assertThrows(IllegalArgumentException.class, () -> bad.applyTo(replica), "byte " + index);
            assertEquals(before, GraphDigest.of(replica), "byte " + index);
            assertEquals(2, replica.appliedLsn(), "byte " + index);
        }
    }

    @Test
    void everyTruncationOfAChunkIsRejectedWithNothingApplied() {
        seed();
        ReplicatedStream.Chunk chunk = stream.add(5, 6).transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 7, 0, 0, 8);
            batch.tuple(RecordType.TUPLE_ADD, 9, 0, 0, 10);
        }).drain();
        GraphDigest before = GraphDigest.of(replica);

        for (int length = 1; length < chunk.records().length; length++) {
            ReplicatedStream.Chunk bad = chunk.withRecords(Arrays.copyOf(chunk.records(), length));
            assertThrows(IllegalArgumentException.class, () -> bad.applyTo(replica), "length " + length);
            assertEquals(before, GraphDigest.of(replica), "length " + length);
        }
    }

    @Test
    void aChunkThatEndsInsideATransactionIsRejected() {
        seed();
        ReplicatedStream.Chunk chunk = stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 5, 0, 0, 6);
            batch.tuple(RecordType.TUPLE_ADD, 7, 0, 0, 8);
        }).drain();
        byte[] withoutCommit = Arrays.copyOf(chunk.records(), chunk.records().length - RecordFormat.COMMIT_BYTES);

        assertRejectedWithNothingChanged(new ReplicatedStream.Chunk(withoutCommit, chunk.first(), chunk.last() - 1));
    }

    @Test
    void aSingleRecordCommitInsideAnOpenTransactionIsRejected() {
        seed();
        ReplicatedStream.Chunk open = stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 5, 0, 0, 6);
            batch.tuple(RecordType.TUPLE_ADD, 7, 0, 0, 8);
        }).drain();
        byte[] withoutCommit = Arrays.copyOf(open.records(), open.records().length - RecordFormat.COMMIT_BYTES);
        ReplicatedStream.Chunk interrupting = new ReplicatedStream(open.last()).add(9, 10).drain();
        byte[] joined = new byte[withoutCommit.length + interrupting.records().length];
        System.arraycopy(withoutCommit, 0, joined, 0, withoutCommit.length);
        System.arraycopy(interrupting.records(), 0, joined, withoutCommit.length, interrupting.records().length);

        assertRejectedWithNothingChanged(new ReplicatedStream.Chunk(joined, open.first(), interrupting.last()));
    }

    @Test
    void aPrimaryRefusesReplicatedRecords() {
        stream.add(1, 2);
        ReplicatedStream.Chunk chunk = stream.drain();

        assertThrows(IllegalStateException.class, () -> chunk.applyTo(new GraphKernel()));
    }

    @Test
    void anEraseRecordStopsTheChunkAtTheLastWholeTransactionAndLeavesTheReplicaUsable() {
        stream.add(1, 2);
        stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 3, 0, 0, 4);
            batch.erase(1, RecordFixtures.pseudonym());
        });
        ReplicatedStream.Chunk chunk = stream.drain();

        assertThrows(UnsupportedFeatureException.class, () -> chunk.applyTo(replica));

        assertEquals(1, replica.appliedLsn());
        assertTrue(replica.hasEdge(1, 2));
        assertFalse(replica.hasEdge(3, 4));
        new ReplicatedStream(2).add(5, 6).drain().applyTo(replica);
        assertTrue(replica.hasEdge(5, 6));
    }

    @Test
    void aNonIncreasingEpochIsRefusedBeforeAnyRecordOfItsTransactionIsApplied() {
        stream.transaction(batch -> batch.epoch(4, 7, 0)).drain().applyTo(replica);
        ReplicatedStream.Chunk chunk = stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);
            batch.epoch(4, 7, 1);
        }).drain();

        assertThrows(IllegalStateException.class, () -> chunk.applyTo(replica));

        assertFalse(replica.hasEdge(1, 2));
        assertEquals(2, replica.appliedLsn());
        assertEquals(1, replica.epochHistory().size());
    }

    @Test
    void aSymbolThatIsNotTheNextIdIsRefusedWithNothingApplied() {
        ReplicatedStream.Chunk chunk = stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);
            batch.symbol(5, RecordFixtures.utf8("x"), 0, 1);
        }).drain();

        assertThrows(IllegalArgumentException.class, () -> chunk.applyTo(replica));

        assertFalse(replica.hasEdge(1, 2));
        assertEquals(0, replica.symbols().size());
        assertEquals(0, replica.appliedLsn());
    }

    @Test
    void aSchemaThatSkipsAVersionIsRefusedWithNothingApplied() {
        ReplicatedStream.Chunk chunk = stream.transaction(batch -> {
            batch.symbol(0, RecordFixtures.utf8("t"), 0, 1);
            batch.symbol(1, RecordFixtures.utf8("r"), 0, 1);
            batch.schema(3, RecordFixtures.digest(), new int[] {5}, new int[] {0}, new int[] {1}, new int[] {0},
                    RecordFixtures.utf8("schema 3\n"));
        }).drain();

        assertThrows(SchemaViolationException.class, () -> chunk.applyTo(replica));

        assertEquals(0, replica.symbols().size());
        assertEquals(0, replica.catalog().version());
        assertEquals(0, replica.appliedLsn());
    }

    @Test
    void aFailureAfterRecordsWereAppliedFaultsTheReplicaAndKeepsReadsServing() {
        stream.transaction(batch -> {
            batch.symbol(0, RecordFixtures.utf8("folder"), 0, 6);
            batch.symbol(1, RecordFixtures.utf8("parent"), 0, 6);
            batch.schema(1, RecordFixtures.digest(), new int[] {5}, new int[] {0}, new int[] {1}, new int[] {0},
                    RecordFixtures.utf8("schema 1\n"));
        }).drain().applyTo(replica);
        long applied = replica.appliedLsn();
        ReplicatedStream.Chunk poisoned = stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 0, 5, 0, 1);
            batch.schema(2, RecordFixtures.digest(), new int[] {5}, new int[] {0}, new int[] {1},
                    new int[] {RelationCatalog.TUPLESET}, RecordFixtures.utf8("schema 2\n"));
        }).drain();

        assertThrows(SchemaViolationException.class, () -> poisoned.applyTo(replica));

        assertEquals(applied, replica.appliedLsn());
        assertEquals(1, replica.catalog().version());
        ReplicatedStream.Chunk next = new ReplicatedStream(applied + 1).add(9, 10).drain();
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> next.applyTo(replica));
        assertTrue(refused.getMessage().contains("inconsistent"), refused.getMessage());
        assertTrue(replica.probeTuple(0, 5, 0, 1));
        assertFalse(replica.hasEdge(9, 10));
    }
}
