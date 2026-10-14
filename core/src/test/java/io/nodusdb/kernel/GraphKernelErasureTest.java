package io.nodusdb.kernel;

import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.kernel.symbols.Pseudonym;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelErasureTest {

    private static final byte[] SALT = new byte[Pseudonym.SALT_BYTES];
    private static final int VIEWER = 1;

    private final GraphKernel kernel = new GraphKernel();

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] pseudonym(String name) {
        return Pseudonym.of(SALT, utf8(name));
    }

    private static void symbol(RecordBatch batch, int id, String name) {
        byte[] bytes = utf8(name);
        batch.symbol(id, bytes, 0, bytes.length);
    }

    private static RecordBatch erasure(int id, String name) {
        RecordBatch batch = new RecordBatch();
        batch.erase(id, Pseudonym.padded(pseudonym(name)));
        batch.commit();
        return batch;
    }

    private static RecordBatch aliceAndBob() {
        RecordBatch batch = new RecordBatch();
        symbol(batch, 0, "user:alice");
        symbol(batch, 1, "document:readme");
        batch.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 0);
        batch.commit();
        return batch;
    }

    @Test
    void anErasureScrubsTheSymbolAndRecordsItsLsn() {
        kernel.commit(aliceAndBob());

        long lsn = kernel.commit(erasure(0, "user:alice"));

        assertTrue(kernel.symbols().isErased(0));
        assertArrayEquals(pseudonym("user:alice"), kernel.symbols().resolve(0));
        assertEquals(1, kernel.erasedSymbols());
        assertEquals(lsn, kernel.newestEraseLsn());
        assertEquals(lsn, kernel.appliedLsn());
    }

    @Test
    void aGraphWithoutErasuresReportsNone() {
        kernel.commit(aliceAndBob());

        assertEquals(0, kernel.erasedSymbols());
        assertEquals(0, kernel.newestEraseLsn());
    }

    @Test
    void anUndefinedSymbolIsRefusedBeforeTheLogIsWritten() {
        kernel.commit(aliceAndBob());
        long applied = kernel.appliedLsn();

        assertThrows(IllegalArgumentException.class, () -> kernel.commit(erasure(2, "user:carol")));

        assertEquals(applied, kernel.appliedLsn());
        assertEquals(0, kernel.erasedSymbols());
    }

    @Test
    void aMalformedPseudonymIsRefused() {
        kernel.commit(aliceAndBob());
        RecordBatch batch = new RecordBatch();
        byte[] field = new byte[Pseudonym.RECORD_BYTES];
        System.arraycopy(utf8("user:alice"), 0, field, 0, 10);
        batch.erase(0, field);
        batch.commit();

        assertThrows(IllegalArgumentException.class, () -> kernel.commit(batch));

        assertFalse(kernel.symbols().isErased(0));
    }

    @Test
    void aSymbolDefinedInTheSameTransactionCanBeErased() {
        RecordBatch batch = new RecordBatch();
        symbol(batch, 0, "user:alice");
        batch.erase(0, Pseudonym.padded(pseudonym("user:alice")));
        batch.commit();

        kernel.commit(batch);

        assertTrue(kernel.symbols().isErased(0));
        assertEquals(-1, kernel.symbols().lookup(utf8("user:alice"), 0, 10));
    }

    @Test
    void aSymbolNamedByTheSchemaIsRefused() {
        RecordBatch schema = new RecordBatch();
        symbol(schema, 0, "document");
        symbol(schema, 1, "viewer");
        schema.schema(1, new byte[32], new int[] {VIEWER}, new int[] {0}, new int[] {1}, new int[] {0},
                utf8("schema 1"));
        schema.commit();
        kernel.commit(schema);
        long applied = kernel.appliedLsn();

        assertThrows(SchemaViolationException.class, () -> kernel.commit(erasure(0, "document")));
        assertThrows(SchemaViolationException.class, () -> kernel.commit(erasure(1, "viewer")));

        assertEquals(applied, kernel.appliedLsn());
    }

    @Test
    void thePseudonymBytesAreReservedBeforeTheLogIsWritten() {
        GraphKernel measured = new GraphKernel();
        measured.commit(slabFilling());
        GraphKernel limited = new GraphKernel(measured.memoryUsedBytes());
        limited.commit(slabFilling());
        long applied = limited.appliedLsn();

        assertThrows(MemoryLimitExceededException.class, () -> limited.commit(erasure(0, slabName('a'))));

        assertEquals(applied, limited.appliedLsn());
        assertFalse(limited.symbols().isErased(0));
        RecordBatch removal = new RecordBatch();
        removal.tuple(RecordType.TUPLE_REMOVE, 0, VIEWER, 0, 1);
        removal.commit();
        assertEquals(applied + 2, limited.commit(removal));
    }

    private static String slabName(char fill) {
        return "user:" + String.valueOf(fill).repeat(2048 - 5);
    }

    private static RecordBatch slabFilling() {
        RecordBatch batch = new RecordBatch();
        symbol(batch, 0, slabName('a'));
        symbol(batch, 1, slabName('b'));
        batch.commit();
        return batch;
    }

    @Test
    void aFollowerAppliesAnErasureFromTheStream() {
        GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
        ReplicatedStream stream = new ReplicatedStream();
        stream.transaction(batch -> {
            symbol(batch, 0, "user:alice");
            symbol(batch, 1, "document:readme");
            batch.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 0);
        });
        stream.transaction(batch -> batch.tuple(RecordType.TUPLE_REMOVE, 1, VIEWER, 0, 0));
        stream.transaction(batch -> batch.erase(0, Pseudonym.padded(pseudonym("user:alice"))));

        stream.drain().applyTo(replica);

        assertTrue(replica.symbols().isErased(0));
        assertArrayEquals(pseudonym("user:alice"), replica.symbols().resolve(0));
        assertEquals(stream.lastLsn(), replica.newestEraseLsn());
        assertEquals(1, replica.erasedSymbols());
    }
}
