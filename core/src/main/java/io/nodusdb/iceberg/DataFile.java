package io.nodusdb.iceberg;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.lake.parquet.ColumnMetrics;

import java.util.List;
import java.util.Objects;

public record DataFile(String path, long recordCount, long fileSizeBytes, int partitionDay,
                       List<ColumnMetrics> columns, ChainHash sha256) {

    public DataFile {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(sha256, "sha256");
        if (recordCount < 0 || fileSizeBytes < 0) {
            throw new IllegalArgumentException("a data file has non-negative counts and size");
        }
        columns = List.copyOf(columns);
    }
}
