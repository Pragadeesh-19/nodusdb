package io.nodusdb.ship;

import io.nodusdb.chain.ChainCodec;
import io.nodusdb.log.record.RecordFormat;

import java.time.Duration;

public record ShipSettings(Duration interval, int maxObjectBytes, long backlogCapBytes, Duration retention,
                           Duration retryBase, Duration retryMax, Duration failedRetry, int conflictRetries,
                           int claimAttempts) {

    public static final double WARN_FRACTION = 0.5;
    public static final double RESUME_FRACTION = 0.9;

    private static final Duration MIN_INTERVAL = Duration.ofMillis(10);
    private static final long MIN_BACKLOG_CAP = 1L << 20;

    public ShipSettings {
        if (interval == null || interval.compareTo(MIN_INTERVAL) < 0) {
            throw new IllegalArgumentException("the shipping interval must be at least " + MIN_INTERVAL.toMillis()
                    + " ms");
        }
        if (maxObjectBytes < RecordFormat.WRITE_LIMIT_BYTES || maxObjectBytes > ChainCodec.MAX_OBJECT_BYTES / 2) {
            throw new IllegalArgumentException("the object size limit must be between "
                    + RecordFormat.WRITE_LIMIT_BYTES + " and " + ChainCodec.MAX_OBJECT_BYTES / 2 + " bytes");
        }
        if (backlogCapBytes < MIN_BACKLOG_CAP) {
            throw new IllegalArgumentException("the backlog cap must be at least " + MIN_BACKLOG_CAP + " bytes");
        }
        requirePositive(retention, "retention");
        requirePositive(retryBase, "retry base");
        requirePositive(retryMax, "retry maximum");
        requirePositive(failedRetry, "failed-state retry");
        if (retryMax.compareTo(retryBase) < 0) {
            throw new IllegalArgumentException("the retry maximum must not be below the retry base");
        }
        if (conflictRetries < 1 || claimAttempts < 1) {
            throw new IllegalArgumentException("retry and claim attempt counts must be at least 1");
        }
    }

    public static ShipSettings defaults() {
        return new ShipSettings(Duration.ofMillis(100), 1 << 20, 1L << 30, Duration.ofDays(7), Duration.ofMillis(50),
                Duration.ofSeconds(5), Duration.ofSeconds(30), 5, 32);
    }

    public ShipSettings withInterval(Duration newInterval) {
        return new ShipSettings(newInterval, maxObjectBytes, backlogCapBytes, retention, retryBase, retryMax,
                failedRetry, conflictRetries, claimAttempts);
    }

    public ShipSettings withMaxObjectBytes(int bytes) {
        return new ShipSettings(interval, bytes, backlogCapBytes, retention, retryBase, retryMax, failedRetry,
                conflictRetries, claimAttempts);
    }

    public ShipSettings withBacklogCapBytes(long bytes) {
        return new ShipSettings(interval, maxObjectBytes, bytes, retention, retryBase, retryMax, failedRetry,
                conflictRetries, claimAttempts);
    }

    public ShipSettings withRetention(Duration newRetention) {
        return new ShipSettings(interval, maxObjectBytes, backlogCapBytes, newRetention, retryBase, retryMax,
                failedRetry, conflictRetries, claimAttempts);
    }

    public ShipSettings withRetries(Duration base, Duration max, Duration failed) {
        return new ShipSettings(interval, maxObjectBytes, backlogCapBytes, retention, base, max, failed,
                conflictRetries, claimAttempts);
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
