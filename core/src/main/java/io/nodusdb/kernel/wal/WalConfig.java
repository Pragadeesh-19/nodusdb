package io.nodusdb.kernel.wal;

public record WalConfig(SyncMode syncMode, long flushIntervalMillis, int bufferFrames) {

    public static final WalConfig DEFAULT = new WalConfig(SyncMode.ASYNC, 10L, 1 << 16);

    public WalConfig {
        if (syncMode == null) {
            throw new IllegalArgumentException("sync mode is required");
        }
        if (flushIntervalMillis < 1) {
            throw new IllegalArgumentException("flush interval must be at least 1 ms: " + flushIntervalMillis);
        }
        if (bufferFrames < 1 || bufferFrames > (1 << 24)) {
            throw new IllegalArgumentException("buffer must hold 1 to 2^24 frames: " + bufferFrames);
        }
    }

    public static WalConfig withSyncMode(SyncMode syncMode) {
        return new WalConfig(syncMode, DEFAULT.flushIntervalMillis(), DEFAULT.bufferFrames());
    }
}
