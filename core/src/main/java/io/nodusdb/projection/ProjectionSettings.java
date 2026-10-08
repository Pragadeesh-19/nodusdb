package io.nodusdb.projection;

import io.nodusdb.lake.codec.ParquetCodec;

import java.time.Duration;
import java.util.Objects;

public record ProjectionSettings(Duration commitInterval, int flushRows, Duration snapshotRetention,
                                 Duration orphanSweepInterval, Duration orphanGrace, int objectsPerStep,
                                 Duration retryBase, Duration retryMax, Duration failedRetry, ParquetCodec codec) {

    public static final int MAX_FLUSH_ROWS = 122_880;

    public ProjectionSettings {
        requirePositive(commitInterval, "commit interval");
        requirePositive(snapshotRetention, "snapshot retention");
        requirePositive(orphanSweepInterval, "orphan sweep interval");
        requirePositive(orphanGrace, "orphan grace");
        requirePositive(retryBase, "retry base");
        requirePositive(retryMax, "retry maximum");
        requirePositive(failedRetry, "failed-state retry");
        Objects.requireNonNull(codec, "codec");
        if (flushRows < 1 || flushRows > MAX_FLUSH_ROWS) {
            throw new IllegalArgumentException("rows per data file must be between 1 and " + MAX_FLUSH_ROWS);
        }
        if (objectsPerStep < 1) {
            throw new IllegalArgumentException("objects per step must be at least 1");
        }
        if (retryMax.compareTo(retryBase) < 0) {
            throw new IllegalArgumentException("the retry maximum must not be below the retry base");
        }
    }

    public static ProjectionSettings defaults() {
        return new ProjectionSettings(Duration.ofSeconds(60), MAX_FLUSH_ROWS, Duration.ofDays(7),
                Duration.ofHours(1), Duration.ofHours(1), 64, Duration.ofMillis(100), Duration.ofSeconds(10),
                Duration.ofSeconds(30), ParquetCodec.SNAPPY);
    }

    public ProjectionSettings withCommitInterval(Duration interval) {
        return new ProjectionSettings(interval, flushRows, snapshotRetention, orphanSweepInterval, orphanGrace,
                objectsPerStep, retryBase, retryMax, failedRetry, codec);
    }

    public ProjectionSettings withFlushRows(int rows) {
        return new ProjectionSettings(commitInterval, rows, snapshotRetention, orphanSweepInterval, orphanGrace,
                objectsPerStep, retryBase, retryMax, failedRetry, codec);
    }

    public ProjectionSettings withSnapshotRetention(Duration retention) {
        return new ProjectionSettings(commitInterval, flushRows, retention, orphanSweepInterval, orphanGrace,
                objectsPerStep, retryBase, retryMax, failedRetry, codec);
    }

    public ProjectionSettings withObjectsPerStep(int objects) {
        return new ProjectionSettings(commitInterval, flushRows, snapshotRetention, orphanSweepInterval,
                orphanGrace, objects, retryBase, retryMax, failedRetry, codec);
    }

    public ProjectionSettings withOrphans(Duration sweepInterval, Duration grace) {
        return new ProjectionSettings(commitInterval, flushRows, snapshotRetention, sweepInterval, grace,
                objectsPerStep, retryBase, retryMax, failedRetry, codec);
    }

    public ProjectionSettings withRetries(Duration base, Duration max, Duration failed) {
        return new ProjectionSettings(commitInterval, flushRows, snapshotRetention, orphanSweepInterval,
                orphanGrace, objectsPerStep, base, max, failed, codec);
    }

    public ProjectionSettings withCodec(ParquetCodec newCodec) {
        return new ProjectionSettings(commitInterval, flushRows, snapshotRetention, orphanSweepInterval,
                orphanGrace, objectsPerStep, retryBase, retryMax, failedRetry, newCodec);
    }

    public long backoffNanos(int attempt) {
        long base = retryBase.toNanos();
        int shift = Math.min(Math.max(attempt - 1, 0), 30);
        long scaled = base << shift;
        return scaled < 0 || scaled > retryMax.toNanos() ? retryMax.toNanos() : scaled;
    }

    private static void requirePositive(Duration duration, String name) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("the " + name + " must be positive");
        }
    }
}
