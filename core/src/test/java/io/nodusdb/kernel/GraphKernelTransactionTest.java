package io.nodusdb.kernel;

import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelTransactionTest {

    private static final byte[] DIGEST = new byte[32];
    private static final byte[] DOCUMENT = "schema 1".getBytes(StandardCharsets.UTF_8);
    private static final int MEMBER = 1;
    private static final int PARENT = 2;
    private static final int VIEWER = 3;

    private final GraphKernel kernel = new GraphKernel();

    private static void symbol(RecordBatch batch, int id, String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        batch.symbol(id, bytes, 0, bytes.length);
    }

    private static RecordBatch schema(int version, int[] ids, int[] flags) {
        int[] zeros = new int[ids.length];
        RecordBatch batch = new RecordBatch();
        batch.schema(version, DIGEST, ids, zeros, zeros, flags, DOCUMENT);
        batch.commit();
        return batch;
    }

    private static RecordBatch tuple(RecordType type, int object, int relation, int subjectRelation, int subject) {
        RecordBatch batch = new RecordBatch();
        batch.tuple(type, object, relation, subjectRelation, subject);
        batch.commit();
        return batch;
    }

    @Test
    void aTransactionAppliesEverySymbolAndTupleTogether() {
        RecordBatch batch = new RecordBatch();
        symbol(batch, 0, "user:alice");
        symbol(batch, 1, "doc:readme");
        batch.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 0);
        batch.commit();

        long lsn = kernel.commit(batch);

        assertEquals(4, lsn);
        assertEquals(4, kernel.appliedLsn());
        assertEquals(2, kernel.symbols().size());
        assertTrue(kernel.probeTuple(1, VIEWER, 0, 0));
        assertFalse(kernel.probeTuple(1, PARENT, 0, 0));
    }

    @Test
    void plainTuplesStayInTheDirectPartition() {
        kernel.commit(tuple(RecordType.TUPLE_ADD, 5, VIEWER, 0, 6));

        assertTrue(kernel.hasPartition(Partition.DIRECT));
        assertFalse(kernel.hasPartition(Partition.INDIRECT));
        assertEquals(1, kernel.probeDegree(Partition.DIRECT, 5));
    }

    @Test
    void usersetSubjectsLiveInTheIndirectPartition() {
        kernel.commit(tuple(RecordType.TUPLE_ADD, 5, VIEWER, MEMBER, 6));

        assertTrue(kernel.hasPartition(Partition.INDIRECT));
        assertTrue(kernel.probeTuple(5, VIEWER, MEMBER, 6));
        assertEquals(0, kernel.probeDegree(Partition.DIRECT, 5));
        assertEquals(1, kernel.probeDegree(Partition.INDIRECT, 5));
        assertEquals(EdgeKey.pack(VIEWER, MEMBER, 6), kernel.probeKey(Partition.INDIRECT, 5, 0));
    }

    @Test
    void tuplesetRelationsLiveInTheIndirectPartition() {
        kernel.commit(schema(1, new int[] {PARENT}, new int[] {RelationCatalog.TUPLESET}));

        kernel.commit(tuple(RecordType.TUPLE_ADD, 5, PARENT, 0, 6));

        assertTrue(kernel.probeTuple(5, PARENT, 0, 6));
        assertEquals(1, kernel.probeDegree(Partition.INDIRECT, 5));
        assertEquals(0, kernel.probeDegree(Partition.DIRECT, 5));
    }

    @Test
    void theSameObjectAndSubjectUnderTwoRelationsAreTwoTuples() {
        kernel.commit(tuple(RecordType.TUPLE_ADD, 5, VIEWER, 0, 6));
        kernel.commit(tuple(RecordType.TUPLE_ADD, 5, MEMBER, 0, 6));

        assertEquals(2, kernel.probeDegree(Partition.DIRECT, 5));
        kernel.commit(tuple(RecordType.TUPLE_REMOVE, 5, VIEWER, 0, 6));
        assertTrue(kernel.probeTuple(5, MEMBER, 0, 6));
        assertFalse(kernel.probeTuple(5, VIEWER, 0, 6));
    }

    @Test
    void removingAnAbsentTupleChangesNothing() {
        kernel.commit(tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 2));

        kernel.commit(tuple(RecordType.TUPLE_REMOVE, 9, VIEWER, 0, 2));

        assertTrue(kernel.probeTuple(1, VIEWER, 0, 2));
    }

    @Test
    void aRejectedTransactionAppliesNoneOfItsRecords() {
        RecordBatch batch = new RecordBatch();
        batch.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 2);
        symbol(batch, 7, "out-of-sequence");
        batch.commit();

        assertThrows(IllegalArgumentException.class, () -> kernel.commit(batch));

        assertFalse(kernel.probeTuple(1, VIEWER, 0, 2));
        assertEquals(0, kernel.symbols().size());
        assertEquals(0, kernel.appliedLsn());
    }

    @Test
    void symbolsMustBeDefinedInSequenceWithinAndAcrossTransactions() {
        RecordBatch first = new RecordBatch();
        symbol(first, 0, "a");
        symbol(first, 1, "b");
        first.commit();
        kernel.commit(first);
        RecordBatch repeated = new RecordBatch();
        symbol(repeated, 1, "c");
        repeated.commit();

        assertThrows(IllegalArgumentException.class, () -> kernel.commit(repeated));
        assertEquals(2, kernel.symbols().size());
    }

    @Test
    void epochAndErasureRecordsCannotBeSubmittedByCallers() {
        RecordBatch epoch = new RecordBatch();
        epoch.epoch(2, 0, 0);
        epoch.commit();
        RecordBatch erase = new RecordBatch();
        erase.erase(0, new byte[24]);
        erase.commit();

        assertThrows(IllegalArgumentException.class, () -> kernel.commit(epoch));
        assertThrows(UnsupportedFeatureException.class, () -> kernel.commit(erase));
    }

    @Test
    void theKeyKindIsClaimedOnceAndAConflictIsRefusedBeforeAnythingIsApplied() {
        kernel.claimKeyKind(KeyKind.STRING);
        long applied = kernel.appliedLsn();
        RecordBatch conflicting = new RecordBatch();
        conflicting.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 2);
        conflicting.graphConfig(KeyKind.INTEGER.code());
        conflicting.commit();

        assertEquals(KeyKind.STRING, kernel.keyKind());
        assertEquals(applied, kernel.claimKeyKind(KeyKind.STRING));
        assertThrows(IllegalStateException.class, () -> kernel.commit(conflicting));
        assertFalse(kernel.probeTuple(1, VIEWER, 0, 2));
        assertEquals(applied, kernel.appliedLsn());
    }

    @Test
    void aSchemaMustFollowTheCurrentVersionByOne() {
        assertThrows(SchemaViolationException.class,
                () -> kernel.commit(schema(2, new int[] {MEMBER}, new int[] {0})));

        kernel.commit(schema(1, new int[] {MEMBER}, new int[] {RelationCatalog.MEMBERSHIP}));

        assertEquals(1, kernel.catalog().version());
        assertTrue(kernel.catalog().has(MEMBER));
        assertThrows(SchemaViolationException.class,
                () -> kernel.commit(schema(1, new int[] {MEMBER}, new int[] {0})));
    }

    @Test
    void aRelationThatHoldsTuplesCannotBecomeATuplesetUntilItIsEmpty() {
        kernel.commit(schema(1, new int[] {PARENT}, new int[] {0}));
        kernel.commit(tuple(RecordType.TUPLE_ADD, 1, PARENT, 0, 2));
        RecordBatch retyped = schema(2, new int[] {PARENT}, new int[] {RelationCatalog.TUPLESET});

        assertThrows(SchemaViolationException.class, () -> kernel.commit(retyped));
        assertEquals(1, kernel.catalog().version());
        kernel.commit(tuple(RecordType.TUPLE_REMOVE, 1, PARENT, 0, 2));
        kernel.commit(schema(2, new int[] {PARENT}, new int[] {RelationCatalog.TUPLESET}));

        assertTrue(kernel.catalog().isTupleset(PARENT));
    }

    @Test
    void aRetiredRelationCannotComeBack() {
        kernel.commit(schema(1, new int[] {MEMBER}, new int[] {0}));
        kernel.commit(schema(2, new int[] {MEMBER}, new int[] {RelationCatalog.RETIRED}));

        assertThrows(SchemaViolationException.class,
                () -> kernel.commit(schema(3, new int[] {MEMBER}, new int[] {0})));
    }

    @Test
    void aTransactionThatDoesNotFitTheMemoryLimitIsRefusedWhole() {
        GraphKernel small = new GraphKernel(new GraphKernel().memoryUsedBytes() + 4_000);
        RecordBatch batch = new RecordBatch();
        for (int i = 0; i < 400; i++) {
            batch.tuple(RecordType.TUPLE_ADD, i, VIEWER, 0, i + 1_000_000);
        }
        batch.commit();
        long before = small.memoryUsedBytes();

        assertThrows(MemoryLimitExceededException.class, () -> small.commit(batch));

        assertEquals(before, small.memoryUsedBytes());
        assertFalse(small.probeTuple(0, VIEWER, 0, 1_000_000));
        assertEquals(0, small.appliedLsn());
    }

    @Test
    void anEmptyOrUncommittedBatchIsRejected() {
        RecordBatch open = new RecordBatch();
        open.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 2);

        assertThrows(IllegalArgumentException.class, () -> kernel.commit(new RecordBatch()));
        assertThrows(IllegalArgumentException.class, () -> kernel.commit(open));
    }
}
