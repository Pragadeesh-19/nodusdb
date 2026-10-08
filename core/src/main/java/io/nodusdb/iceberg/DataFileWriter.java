package io.nodusdb.iceberg;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.lake.parquet.ParquetWriter;
import io.nodusdb.lake.parquet.WrittenFile;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.ObjectStoreException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class DataFileWriter {

    private final ObjectStore store;
    private final IcebergTable table;
    private final Path scratch;
    private final ParquetCodec codec;

    public DataFileWriter(ObjectStore store, IcebergTable table, Path scratch, ParquetCodec codec) {
        this.store = store;
        this.table = table;
        this.scratch = scratch;
        this.codec = codec;
    }

    public DataFile write(EdgeLogRows rows, int partitionDay) throws IOException {
        if (rows.size() == 0) {
            throw new IllegalArgumentException("a data file needs at least one row");
        }
        Files.createDirectories(scratch);
        Path local = Files.createTempFile(scratch, "datafile-", ".parquet");
        try {
            WrittenFile written = ParquetWriter.write(local, rows, codec);
            String key = table.newDataKey(rows.firstLsn(), rows.lastLsn());
            upload(key, local);
            return new DataFile(table.uriOf(key), written.rowCount(), written.fileBytes(), partitionDay,
                    written.columns(), ChainHash.sha256(local));
        } finally {
            Files.deleteIfExists(local);
        }
    }

    private void upload(String key, Path local) {
        try {
            store.putFile(key, local, Map.of());
        } catch (ObjectStoreException failure) {
            discard(key, failure);
            throw failure;
        }
    }

    private void discard(String key, ObjectStoreException failure) {
        try {
            store.delete(key);
        } catch (ObjectStoreException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }
}
