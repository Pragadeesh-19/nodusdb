package io.nodusdb.iceberg;

import io.nodusdb.lake.parquet.ColumnMetrics;

import java.util.List;
import java.util.Objects;

public record ManifestEntry(Status status, long snapshotId, long dataSequenceNumber, long fileSequenceNumber,
                            String path, int partitionDay, long recordCount, long fileSizeBytes,
                            List<ColumnMetrics> columns) {

    public enum Status {
        EXISTING(0), ADDED(1), DELETED(2);

        final int code;

        Status(int code) {
            this.code = code;
        }

        static Status fromCode(int code) {
            for (Status status : values()) {
                if (status.code == code) {
                    return status;
                }
            }
            throw new IllegalArgumentException("unknown manifest entry status " + code);
        }
    }

    public static final long INHERITED = -1;

    public ManifestEntry {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(path, "path");
        if (recordCount < 0 || fileSizeBytes < 0) {
            throw new IllegalArgumentException("a manifest entry has non-negative counts and size");
        }
        boolean inherits = dataSequenceNumber == INHERITED || fileSequenceNumber == INHERITED;
        if (inherits && status != Status.ADDED) {
            throw new IllegalArgumentException("only an added entry inherits its sequence numbers");
        }
        columns = List.copyOf(columns);
    }

    public static ManifestEntry added(DataFile file, long snapshotId) {
        return new ManifestEntry(Status.ADDED, snapshotId, INHERITED, INHERITED, file.path(), file.partitionDay(),
                file.recordCount(), file.fileSizeBytes(), file.columns());
    }

    public boolean live() {
        return status != Status.DELETED;
    }

    public ManifestEntry existing() {
        return new ManifestEntry(Status.EXISTING, snapshotId, dataSequenceNumber, fileSequenceNumber, path,
                partitionDay, recordCount, fileSizeBytes, columns);
    }

    public ManifestEntry deletedBy(long deletingSnapshotId) {
        return new ManifestEntry(Status.DELETED, deletingSnapshotId, dataSequenceNumber, fileSequenceNumber, path,
                partitionDay, recordCount, fileSizeBytes, List.of());
    }
}
