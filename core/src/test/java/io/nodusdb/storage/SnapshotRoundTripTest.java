package io.nodusdb.storage;

import io.nodusdb.kernel.EpochHistory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotReader;
import io.nodusdb.storage.snapshot.SnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotRoundTripTest {

    private static final int MEMBER = 1;
    private static final int PARENT = 2;
    private static final int VIEWER = 3;
    private static final byte[] DOCUMENT = "schema 1\ntype user".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path directory;

    private static byte[] salt() {
        byte[] salt = new byte[32];
        for (int i = 0; i < salt.length; i++) {
            salt[i] = (byte) (i * 7 + 1);
        }
        return salt;
    }

    private static GraphKernel typedGraph(boolean withSchema) {
        GraphKernel kernel = new GraphKernel();
        RecordBatch names = new RecordBatch();
        for (int id = 0; id < 3; id++) {
            byte[] name = ("node:" + id).getBytes(StandardCharsets.UTF_8);
            names.symbol(id, name, 0, name.length);
        }
        names.graphConfig(KeyKind.STRING.code());
        names.commit();
        kernel.commit(names);
        if (withSchema) {
            RecordBatch schema = new RecordBatch();
            schema.schema(1, new byte[32], new int[] {MEMBER, PARENT, VIEWER}, new int[] {0, 1, 2},
                    new int[] {1, 2, 0}, new int[] {RelationCatalog.MEMBERSHIP, RelationCatalog.TUPLESET, 0},
                    DOCUMENT);
            schema.commit();
            kernel.commit(schema);
        }
        RecordBatch tuples = new RecordBatch();
        tuples.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 0);
        tuples.tuple(RecordType.TUPLE_ADD, 1, VIEWER, MEMBER, 2);
        tuples.tuple(RecordType.TUPLE_ADD, 2, PARENT, 0, 1);
        tuples.tuple(RecordType.TUPLE_ADD, 40, MEMBER, 0, 41);
        tuples.commit();
        kernel.commit(tuples);
        kernel.addEdge(5, 6);
        kernel.recordEpoch(1, 1, 0);
        kernel.recordEpoch(2, 9, 8);
        return kernel;
    }

    @Test
    void aTypedGraphSurvivesTheRoundTripWithEverySection() throws IOException {
        GraphKernel original = typedGraph(true);
        SnapshotMeta meta = new SnapshotMeta(77, 2, 123_456_789L, salt());
        Path file = directory.resolve("snapshot.bin");
        SnapshotWriter.write(original, meta, file);

        GraphKernel loaded = new GraphKernel();
        SnapshotMeta read = SnapshotReader.load(file, loaded);

        assertEquals(77, read.lsn());
        assertEquals(2, read.epoch());
        assertEquals(123_456_789L, read.lastCommitMicros());
        assertArrayEquals(salt(), read.salt());
        assertEquals(GraphDigest.of(original), GraphDigest.of(loaded));
        assertEquals(KeyKind.STRING, loaded.keyKind());
        assertEquals(3, loaded.symbols().size());
        assertArrayEquals("node:2".getBytes(StandardCharsets.UTF_8), loaded.symbols().resolve(2));
        assertEquals(1, loaded.catalog().version());
        assertArrayEquals(DOCUMENT, loaded.catalog().document());
        assertTrue(loaded.catalog().isTupleset(PARENT));
        assertEquals(2, loaded.epochHistory().size());
        assertEquals(new EpochHistory.Tenure(2, 9, 8), loaded.epochHistory().tenureAt(1));
        assertTrue(loaded.probeTuple(1, VIEWER, 0, 0));
        assertTrue(loaded.probeTuple(1, VIEWER, MEMBER, 2));
        assertTrue(loaded.probeTuple(2, PARENT, 0, 1));
        assertTrue(loaded.hasEdge(5, 6));
        assertEquals(1, loaded.getInDegree(6));
        assertEquals(1, loaded.degree(Partition.INDIRECT, true, 1));
        assertEquals(1, loaded.degree(Partition.INDIRECT, true, 2));
        assertEquals(1, loaded.degree(Partition.DIRECT, true, 40));
    }

    @Test
    void anEmptyGraphRoundTrips() throws IOException {
        GraphKernel empty = new GraphKernel();
        Path file = directory.resolve("snapshot.bin");
        SnapshotWriter.write(empty, new SnapshotMeta(0, 0, 0, salt()), file);

        GraphKernel loaded = new GraphKernel();
        SnapshotMeta read = SnapshotReader.load(file, loaded);

        assertEquals(0, read.lsn());
        assertEquals(KeyKind.UNSET, loaded.keyKind());
        assertEquals(0, loaded.symbols().size());
        assertEquals(RelationCatalog.EMPTY.version(), loaded.catalog().version());
        assertEquals(0, loaded.epochHistory().size());
    }

    @Test
    void aKeyThatNamesARelationTheSchemaDoesNotKnowIsRejected() throws IOException {
        GraphKernel withoutSchema = typedGraph(false);
        Path file = directory.resolve("snapshot.bin");
        SnapshotWriter.write(withoutSchema, new SnapshotMeta(1, 1, 1, salt()), file);

        IOException failure = assertThrows(IOException.class, () -> SnapshotReader.load(file, new GraphKernel()));

        assertTrue(failure.getMessage().contains("unknown relation"), failure.getMessage());
    }

    @Test
    void theMetaDoesNotShareItsSalt() {
        byte[] salt = salt();
        SnapshotMeta meta = new SnapshotMeta(1, 1, 1, salt);

        salt[0] = 0;
        meta.salt()[1] = 0;

        assertEquals(1, meta.salt()[0]);
        assertEquals(8, meta.salt()[1]);
        assertThrows(IllegalArgumentException.class, () -> new SnapshotMeta(1, 1, 1, new byte[5]));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotMeta(-1, 1, 1, salt()));
    }
}
