package io.nodusdb.log;

import io.nodusdb.log.record.RecordFormat;

public record LogConfig(SyncMode syncMode, long flushIntervalMillis, int bufferBytes, long segmentBytes,
                        long markIntervalMillis) {

    public static final LogConfig DEFAULT = new LogConfig(SyncMode.ASYNC, 10L, 2 << 20, 64L << 20, 100L);

    private static final long MIN_SEGMENT_BYTES = 1 << 10;

    public LogConfig {
        if (syncMode == null) {
            throw new IllegalArgumentException("sync mode is required");
        }
        if (flushIntervalMillis < 1) {
            throw new IllegalArgumentException("flush interval must be at least 1 ms: " + flushIntervalMillis);
        }
        if (bufferBytes < RecordFormat.WRITE_LIMIT_BYTES) {
            throw new IllegalArgumentException("buffer must hold the largest record: " + bufferBytes);
        }
        if (segmentBytes < MIN_SEGMENT_BYTES) {
            throw new IllegalArgumentException("segments must be at least " + MIN_SEGMENT_BYTES + " bytes");
        }
        if (markIntervalMillis < 1) {
            throw new IllegalArgumentException("mark interval must be at least 1 ms: " + markIntervalMillis);
        }
    }

    public static LogConfig withSyncMode(SyncMode syncMode) {
        return new LogConfig(syncMode, DEFAULT.flushIntervalMillis(), DEFAULT.bufferBytes(),
                DEFAULT.segmentBytes(), DEFAULT.markIntervalMillis());
    }

    public LogConfig withSegmentBytes(long bytes) {
        return new LogConfig(syncMode, flushIntervalMillis, bufferBytes, bytes, markIntervalMillis);
    }
}
