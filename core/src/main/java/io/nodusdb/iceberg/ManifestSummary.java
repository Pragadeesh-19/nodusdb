package io.nodusdb.iceberg;

import java.util.Objects;

public record ManifestSummary(String path, long length, long sequenceNumber, long minSequenceNumber,
                              long addedSnapshotId, int addedFiles, int existingFiles, int deletedFiles,
                              long addedRows, long existingRows, long deletedRows, int lowerDay, int upperDay) {

    public ManifestSummary {
        Objects.requireNonNull(path, "path");
        if (length < 0 || addedFiles < 0 || existingFiles < 0 || deletedFiles < 0 || addedRows < 0
                || existingRows < 0 || deletedRows < 0) {
            throw new IllegalArgumentException("a manifest has non-negative counts and length");
        }
        if (lowerDay > upperDay) {
            throw new IllegalArgumentException("the partition bounds are reversed: " + lowerDay + " > " + upperDay);
        }
    }
}
