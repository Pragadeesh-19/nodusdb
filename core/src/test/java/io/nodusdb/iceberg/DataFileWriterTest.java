package io.nodusdb.iceberg;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.objectstore.DirectoryObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataFileWriterTest {

    @TempDir
    Path root;


    private IcebergTable table(FaultyObjectStore store) {
        return new IcebergTable(store, root.resolve("b").toAbsolutePath().toString().replace('\\', '/') + "/iceberg",
                "iceberg/", () -> 1_700_000_000_000L);
    }

    private static EdgeLogRows rows(int count) {
        return IcebergFixture.rows(1, count, IcebergFixture.START_MICROS, 1);
    }

    private long stored(DirectoryObjectStore store) throws IOException {
        try (Stream<Path> files = Files.walk(root.resolve("b"))) {
            return files.filter(path -> path.toString().endsWith(".parquet")).count();
        }
    }

    @Test
    void aWrittenFileIsStoredAndDescribedWithItsDigestAndMetrics() throws IOException {
        DirectoryObjectStore store = new DirectoryObjectStore(root.resolve("b"));
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        IcebergTable table = table(faulty);
        DataFileWriter writer = new DataFileWriter(faulty, table, root.resolve("scratch"), ParquetCodec.SNAPPY);

        DataFile file = writer.write(rows(40), 19_000);

        assertEquals(40, file.recordCount());
        assertEquals(19_000, file.partitionDay());
        assertEquals(13, file.columns().size());
        Path stored = root.resolve("b").resolve(table.keyOf(file.path()));
        assertEquals(Files.size(stored), file.fileSizeBytes());
        assertEquals(ChainHash.sha256(stored), file.sha256());
        assertEquals(0, Files.list(root.resolve("scratch")).count(), "the local copy is removed");
    }

    @Test
    void anUploadThatFailsAfterLandingIsDeletedSoNoStrayCopyRemains() throws IOException {
        DirectoryObjectStore store = new DirectoryObjectStore(root.resolve("b"));
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        DataFileWriter writer = new DataFileWriter(faulty, table(faulty), root.resolve("scratch"), ParquetCodec.SNAPPY);
        faulty.failNext(Operation.PUT_FILE, Fault.FAIL_AFTER);

        assertThrows(TransientStoreException.class, () -> writer.write(rows(10), 19_000));

        assertEquals(0, stored(store));
        assertEquals(1, faulty.count(Operation.DELETE));
        assertEquals(0, Files.list(root.resolve("scratch")).count());
    }

    @Test
    void aFailureToCleanUpIsAttachedToTheUploadFailure() throws IOException {
        DirectoryObjectStore store = new DirectoryObjectStore(root.resolve("b"));
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        DataFileWriter writer = new DataFileWriter(faulty, table(faulty), root.resolve("scratch"), ParquetCodec.SNAPPY);
        faulty.failNext(Operation.PUT_FILE, Fault.FAIL_AFTER);
        faulty.failNext(Operation.DELETE, Fault.FAIL_BEFORE);

        TransientStoreException failure = assertThrows(TransientStoreException.class,
                () -> writer.write(rows(10), 19_000));

        assertEquals(1, failure.getSuppressed().length);
        assertEquals(1, stored(store));
    }

    @Test
    void aRetryAfterAFailureStoresExactlyOneFile() throws IOException {
        DirectoryObjectStore store = new DirectoryObjectStore(root.resolve("b"));
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        DataFileWriter writer = new DataFileWriter(faulty, table(faulty), root.resolve("scratch"), ParquetCodec.SNAPPY);
        faulty.failNext(Operation.PUT_FILE, Fault.FAIL_AFTER);
        assertThrows(TransientStoreException.class, () -> writer.write(rows(10), 19_000));

        DataFile file = writer.write(rows(10), 19_000);

        assertEquals(1, stored(store));
        assertTrue(Files.exists(root.resolve("b").resolve(table(faulty).keyOf(file.path()))));
    }

    @Test
    void anEmptyBufferIsRefused() {
        DirectoryObjectStore store = new DirectoryObjectStore(root.resolve("b"));
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        DataFileWriter writer = new DataFileWriter(faulty, table(faulty), root.resolve("scratch"), ParquetCodec.SNAPPY);

        assertThrows(IllegalArgumentException.class, () -> writer.write(new EdgeLogRows(), 19_000));
        assertEquals(List.of(), faulty.calls());
    }
}
