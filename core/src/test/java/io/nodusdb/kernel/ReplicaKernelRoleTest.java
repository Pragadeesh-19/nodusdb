package io.nodusdb.kernel;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.ShipWatermark;
import io.nodusdb.log.VolatileLog;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaKernelRoleTest {

    private record Write(String name, Consumer<GraphKernel> call) {

        @Override
        public String toString() {
            return name;
        }
    }

    private static final DurableStorage NO_STORAGE = new DurableStorage() {
        @Override
        public void checkpoint(GraphKernel kernel, LogStore log) {
        }

        @Override
        public void close() {
        }
    };

    static List<Write> everyWriteEntryPoint() {
        return List.of(
                new Write("commit", kernel -> kernel.commit(committedTuple())),
                new Write("claimKeyKind", kernel -> kernel.claimKeyKind(KeyKind.STRING)),
                new Write("addEdge", kernel -> kernel.addEdge(1, 2)),
                new Write("removeEdge", kernel -> kernel.removeEdge(1, 2)),
                new Write("addTuple", kernel -> kernel.addTuple(1, 2, 0, 3)),
                new Write("removeTuple", kernel -> kernel.removeTuple(1, 2, 0, 3)),
                new Write("addEdges", kernel -> kernel.addEdges(new long[] {1, 2}, 1)),
                new Write("removeEdges", kernel -> kernel.removeEdges(new long[] {1, 2}, 1)),
                new Write("checkpoint", GraphKernel::checkpoint),
                new Write("attachLog", kernel -> kernel.attachLog(new VolatileLog(), NO_STORAGE)),
                new Write("attachShipping", kernel -> kernel.attachShipping(ShipWatermark.NONE)));
    }

    private static RecordBatch committedTuple() {
        RecordBatch batch = new RecordBatch();
        batch.autocommitTuple(RecordType.TUPLE_ADD, 1, 0, 0, 2);
        return batch;
    }

    private final GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);

    @Test
    void aReplicaKnowsItsRoleAndAPrimaryDoesNot() {
        assertTrue(replica.isReplica());
        assertFalse(new GraphKernel().isReplica());
        assertFalse(GraphKernel.openInMemory().isReplica());
    }

    @ParameterizedTest
    @MethodSource("everyWriteEntryPoint")
    void everyWriteEntryPointIsRefusedAndChangesNothing(Write write) {
        UnsupportedFeatureException refused = assertThrows(UnsupportedFeatureException.class,
                () -> write.call().accept(replica));

        assertTrue(refused.getMessage().contains("follower"), refused.getMessage());
        assertFalse(replica.hasEdges());
        assertEquals(0, replica.appliedLsn());
        assertEquals(KeyKind.UNSET, replica.keyKind());
        assertFalse(replica.isDurable());
    }

    @Test
    void aRefusalComesBeforeArgumentValidation() {
        assertThrows(UnsupportedFeatureException.class, () -> replica.addEdge(-1, 2));
        assertThrows(UnsupportedFeatureException.class, () -> replica.addEdges(new long[0], 5));
        assertThrows(UnsupportedFeatureException.class, () -> replica.commit(new RecordBatch()));
        assertThrows(UnsupportedFeatureException.class, () -> replica.claimKeyKind(KeyKind.UNSET));
    }

    @Test
    void aPrimaryIsStillWritable() {
        GraphKernel primary = new GraphKernel();

        assertTrue(primary.addEdge(1, 2));
        assertTrue(primary.hasEdge(1, 2));
        assertEquals(1, primary.appliedLsn());
    }

    @Test
    void aFreshReplicaSitsAtPositionZeroWithNoEpoch() {
        assertEquals(0, replica.appliedLsn());
        assertEquals(0, replica.epoch());
        assertEquals(0, replica.lastCommitMicros());
        assertEquals(new Token(0, 0), replica.token());
    }

    @Test
    void theEpochAndTheTokenComeFromTheEpochHistory() {
        replica.recordEpoch(4, 1, 0);
        replica.recordEpoch(6, 50, 49);

        assertEquals(6, replica.epoch());
        assertEquals(new Token(6, 0), replica.token());
    }

    @Test
    void theLoaderHooksStillFillAReplica() {
        byte[] name = {'a', ':', '1'};
        replica.restoreKeyKind(KeyKind.STRING);
        replica.restoreSymbol(0, name);
        replica.recordEpoch(2, 1, 0);

        assertEquals(KeyKind.STRING, replica.keyKind());
        assertEquals(1, replica.symbols().size());
        assertEquals(1, replica.epochHistory().size());
    }

    @Test
    void theBulkLoaderStillFillsAReplicaBeforeItIsShared() {
        replica.prepareBulkLoad(new int[] {1, 0}, new int[] {0, 1});
        replica.loadBulkNode(true, 0, new long[] {1}, 1);
        replica.loadBulkNode(false, 1, new long[] {0}, 1);

        assertTrue(replica.hasEdge(0, 1));
        assertEquals(1, replica.getInDegree(1));
    }

    @Test
    void restoringAPositionSetsWhereReplicationContinues() {
        replica.restorePosition(40, 1_700_000_000_000_000L);

        assertEquals(40, replica.appliedLsn());
        assertEquals(1_700_000_000_000_000L, replica.lastCommitMicros());
        assertEquals(40, replica.token().lsn());
    }

    @Test
    void aPositionOfZeroIsAValidEmptySnapshot() {
        replica.restorePosition(0, 0);

        assertEquals(0, replica.appliedLsn());
    }

    @Test
    void aPositionCannotBeRestoredOnAPrimary() {
        assertThrows(IllegalStateException.class, () -> new GraphKernel().restorePosition(1, 1));
    }

    @Test
    void aNegativePositionIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> replica.restorePosition(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> replica.restorePosition(0, -1));
    }

    @Test
    void aPositionCannotBeRestoredTwice() {
        replica.restorePosition(10, 5);

        assertThrows(IllegalStateException.class, () -> replica.restorePosition(20, 6));
        assertEquals(10, replica.appliedLsn());
    }

    @Test
    void aPositionCannotBeRestoredAfterRecordsWereApplied() {
        ReplicatedStream stream = new ReplicatedStream();
        stream.add(1, 2).drain().applyTo(replica);

        assertThrows(IllegalStateException.class, () -> replica.restorePosition(20, 6));
        assertEquals(1, replica.appliedLsn());
    }

    @Test
    void theMemoryLimitAppliesToAReplicaAndIsReported() {
        long footprint = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT).memoryUsedBytes();

        GraphKernel limited = GraphKernel.openReplica(footprint + 4_096);

        assertEquals(footprint + 4_096, limited.memoryLimitBytes());
        assertThrows(MemoryLimitExceededException.class, () -> GraphKernel.openReplica(1));
        assertThrows(IllegalArgumentException.class, () -> GraphKernel.openReplica(-1));
    }

    @Test
    void closingAReplicaIsIdempotentAndRefusesFurtherApplies() {
        replica.close();
        replica.close();

        ReplicatedStream stream = new ReplicatedStream();
        stream.add(1, 2);
        ReplicatedStream.Chunk chunk = stream.drain();

        assertThrows(IllegalStateException.class, () -> chunk.applyTo(replica));
        assertEquals(0, replica.appliedLsn());
    }
}
