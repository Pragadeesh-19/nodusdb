package io.nodusdb.kernel;

import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaKernelApplyTest {

    private final GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
    private final ReplicatedStream stream = new ReplicatedStream();

    private static byte[] utf8(String text) {
        return RecordFixtures.utf8(text);
    }

    @Test
    void aSingleAutocommitTupleIsApplied() {
        stream.add(1, 2).drain().applyTo(replica);

        assertTrue(replica.hasEdge(1, 2));
        assertEquals(1, replica.getDegree(1));
        assertEquals(1, replica.getInDegree(2));
        assertEquals(1, replica.appliedLsn());
    }

    @Test
    void aMultiRecordTransactionAdvancesTheLsnPastItsCommitRecord() {
        stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);
            batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 3);
            batch.tuple(RecordType.TUPLE_ADD, 2, 0, 0, 3);
        }).drain().applyTo(replica);

        assertEquals(4, replica.appliedLsn());
        assertEquals(2, replica.getDegree(1));
        assertEquals(2, replica.getInDegree(3));
    }

    @Test
    void severalTransactionsInOneChunkAreAllApplied() {
        stream.add(1, 2);
        stream.transaction(batch -> batch.tuple(RecordType.TUPLE_ADD, 2, 0, 0, 3));
        stream.remove(1, 2);
        stream.add(4, 5);

        stream.drain().applyTo(replica);

        assertFalse(replica.hasEdge(1, 2));
        assertTrue(replica.hasEdge(2, 3));
        assertTrue(replica.hasEdge(4, 5));
        assertEquals(5, replica.appliedLsn());
    }

    @Test
    void consecutiveChunksContinueFromTheLastAppliedLsn() {
        stream.add(1, 2);
        stream.drain().applyTo(replica);
        stream.add(3, 4);
        stream.add(5, 6);
        stream.drain().applyTo(replica);

        assertEquals(3, replica.appliedLsn());
        assertTrue(replica.hasEdge(5, 6));
    }

    @Test
    void applyingStartsAfterARestoredPosition() {
        replica.restorePosition(100, 7);
        ReplicatedStream later = new ReplicatedStream(101);

        later.add(1, 2).drain().applyTo(replica);

        assertEquals(101, replica.appliedLsn());
        assertTrue(replica.hasEdge(1, 2));
    }

    @Test
    void theCommitTimestampOfTheLastTransactionIsRecorded() {
        stream.add(1, 2);
        long first = stream.lastCommitMicros();
        stream.transaction(batch -> batch.tuple(RecordType.TUPLE_ADD, 3, 0, 0, 4));
        long second = stream.lastCommitMicros();
        assertTrue(second > first);

        stream.drain().applyTo(replica);

        assertEquals(second, replica.lastCommitMicros());
    }

    @Test
    void theTokenCarriesTheLatestEpochAndTheAppliedLsn() {
        replica.recordEpoch(3, 1, 0);

        stream.add(1, 2).drain().applyTo(replica);

        assertEquals(new Token(3, 1), replica.token());
        assertEquals(3, replica.epoch());
    }

    @Test
    void anEpochRecordExtendsTheEpochHistory() {
        stream.transaction(batch -> batch.epoch(2, 7, 0));
        stream.add(1, 2);
        stream.transaction(batch -> batch.epoch(3, 7, 3));

        stream.drain().applyTo(replica);

        assertEquals(3, replica.epoch());
        assertEquals(2, replica.epochHistory().size());
        assertEquals(new EpochHistory.Tenure(2, 1, 0), replica.epochHistory().tenureAt(0));
        assertEquals(new EpochHistory.Tenure(3, 4, 3), replica.epochHistory().tenureAt(1));
        assertEquals(new Token(3, 5), replica.token());
    }

    @Test
    void anEpochRecordMayShareATransactionWithTuples() {
        stream.transaction(batch -> {
            batch.epoch(2, 7, 0);
            batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);
        });

        stream.drain().applyTo(replica);

        assertEquals(2, replica.epoch());
        assertTrue(replica.hasEdge(1, 2));
    }

    @Test
    void symbolsTheKeyKindAndASchemaAreApplied() {
        stream.transaction(batch -> {
            batch.graphConfig(KeyKind.STRING.code());
            batch.symbol(0, utf8("document"), 0, 8);
            batch.symbol(1, utf8("viewer"), 0, 6);
            batch.schema(1, RecordFixtures.digest(), new int[] {5}, new int[] {0}, new int[] {1}, new int[] {0},
                    utf8("schema 1\n"));
            batch.tuple(RecordType.TUPLE_ADD, 0, 5, 0, 1);
        }).drain().applyTo(replica);

        assertEquals(KeyKind.STRING, replica.keyKind());
        assertEquals(2, replica.symbols().size());
        assertArrayEquals(utf8("viewer"), replica.symbols().resolve(1));
        assertEquals(1, replica.catalog().version());
        assertTrue(replica.probeTuple(0, 5, 0, 1));
    }

    @Test
    void aLaterTransactionMayUseSymbolsDefinedByAnEarlierOneInTheSameChunk() {
        stream.transaction(batch -> batch.symbol(0, utf8("a"), 0, 1));
        stream.transaction(batch -> batch.symbol(1, utf8("b"), 0, 1));

        stream.drain().applyTo(replica);

        assertEquals(2, replica.symbols().size());
    }

    @Test
    void aTupleOnATuplesetRelationLandsInTheIndirectPartition() {
        stream.transaction(batch -> {
            batch.symbol(0, utf8("folder"), 0, 6);
            batch.symbol(1, utf8("parent"), 0, 6);
            batch.schema(1, RecordFixtures.digest(), new int[] {5}, new int[] {0}, new int[] {1},
                    new int[] {RelationCatalog.TUPLESET}, utf8("schema 1\n"));
            batch.tuple(RecordType.TUPLE_ADD, 2, 5, 0, 3);
        }).drain().applyTo(replica);

        assertTrue(replica.probeTuple(2, 5, 0, 3));
        assertTrue(replica.hasPartition(Partition.INDIRECT));
        assertFalse(replica.hasEdge(2, 3));
    }

    @Test
    void removingAnAbsentTupleIsHarmless() {
        stream.remove(8, 9).drain().applyTo(replica);

        assertEquals(1, replica.appliedLsn());
        assertFalse(replica.hasEdges());
    }

    @Test
    void addingATupleTwiceKeepsOneEdge() {
        stream.add(1, 2).add(1, 2).drain().applyTo(replica);

        assertEquals(1, replica.getDegree(1));
        assertEquals(2, replica.appliedLsn());
    }

    @Test
    void aTransactionThatCrossesThePromotionBoundaryIsApplied() {
        stream.transaction(batch -> {
            for (int target = 100; target < 140; target++) {
                batch.tuple(RecordType.TUPLE_ADD, 1, 0, 0, target);
            }
        }).drain().applyTo(replica);

        assertEquals(40, replica.getDegree(1));
        assertTrue(replica.isHighDegree(1));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void aReplicaFedWholeTransactionsEqualsAKernelReplayedRecordByRecord(long seed) {
        Random random = new Random(seed);
        GraphKernel reference = new GraphKernel();
        for (int round = 0; round < 40; round++) {
            int transactions = 1 + random.nextInt(6);
            for (int t = 0; t < transactions; t++) {
                if (random.nextInt(3) == 0) {
                    RecordType type = random.nextBoolean() ? RecordType.TUPLE_ADD : RecordType.TUPLE_REMOVE;
                    stream.autocommit(type, random.nextInt(60), 0, 0, random.nextInt(60));
                } else {
                    int records = 1 + random.nextInt(5);
                    stream.transaction(batch -> {
                        for (int r = 0; r < records; r++) {
                            RecordType type = random.nextInt(4) == 0 ? RecordType.TUPLE_REMOVE : RecordType.TUPLE_ADD;
                            batch.tuple(type, random.nextInt(60), 0, 0, random.nextInt(60));
                        }
                    });
                }
            }
            ReplicatedStream.Chunk chunk = stream.drain();
            chunk.applyTo(replica);
            replayRecordByRecord(reference, chunk.records());
            assertEquals(GraphDigest.of(reference), GraphDigest.of(replica), "seed=" + seed + " round=" + round);
            assertEquals(chunk.last(), replica.appliedLsn(), "seed=" + seed + " round=" + round);
        }
    }

    @Test
    void aMemoryLimitHitMidChunkLeavesTheLastWholeTransactionApplied() {
        long footprint = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT).memoryUsedBytes();
        GraphKernel limited = GraphKernel.openReplica(footprint + 20_000);
        stream.add(0, 1);
        stream.add(2, 3_000_000);
        stream.add(4, 5);
        ReplicatedStream.Chunk chunk = stream.drain();

        assertThrows(MemoryLimitExceededException.class, () -> chunk.applyTo(limited));

        assertEquals(1, limited.appliedLsn());
        assertTrue(limited.hasEdge(0, 1));
        assertTrue(limited.hasIncoming(1, 0));
        assertFalse(limited.hasEdge(2, 3_000_000));
        assertEquals(0, limited.getDegree(4));
    }

    @Test
    void aReplicaThatHitTheLimitKeepsServingAndCanApplyTransactionsThatFit() {
        long footprint = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT).memoryUsedBytes();
        GraphKernel limited = GraphKernel.openReplica(footprint + 20_000);
        stream.add(0, 1);
        stream.add(2, 3_000_000);
        ReplicatedStream.Chunk chunk = stream.drain();
        assertThrows(MemoryLimitExceededException.class, () -> chunk.applyTo(limited));
        long used = limited.memoryUsedBytes();

        new ReplicatedStream(2).add(6, 7).drain().applyTo(limited);

        assertEquals(2, limited.appliedLsn());
        assertTrue(limited.hasEdge(6, 7));
        assertTrue(limited.hasEdge(0, 1));
        assertTrue(limited.memoryUsedBytes() >= used);
    }

    @Test
    void aRejectedTransactionChangesNeitherTheGraphNorTheBudget() {
        long footprint = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT).memoryUsedBytes();
        GraphKernel limited = GraphKernel.openReplica(footprint + 20_000);
        new ReplicatedStream().add(0, 1).drain().applyTo(limited);
        long used = limited.memoryUsedBytes();
        int capacity = limited.nodeCapacity();

        assertThrows(MemoryLimitExceededException.class,
                () -> new ReplicatedStream(2).add(2, 3_000_000).drain().applyTo(limited));

        assertEquals(used, limited.memoryUsedBytes());
        assertEquals(capacity, limited.nodeCapacity());
        assertEquals(1, limited.appliedLsn());
    }

    private static void replayRecordByRecord(GraphKernel kernel, byte[] records) {
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(records), 0, records.length);
        while (reader.hasRecord()) {
            kernel.replay(reader);
            reader.advance();
        }
    }
}
