package io.nodusdb.replica;

import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.TransientStoreException;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Optional;

final class RangeReader {

    private static final int ATTEMPTS = 5;
    private static final long MAX_PAUSE_MILLIS = 2_000;

    private final ObjectStore store;
    private final Duration pause;

    RangeReader(ObjectStore store, Duration pause) {
        this.store = store;
        this.pause = pause;
    }

    byte[] read(String key, long offset, int length) {
        TransientStoreException last = null;
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            try {
                return exact(key, offset, length);
            } catch (TransientStoreException failure) {
                last = failure;
                if (attempt < ATTEMPTS) {
                    sleepBeforeRetry(attempt);
                }
            }
        }
        throw last;
    }

    private byte[] exact(String key, long offset, int length) {
        Optional<byte[]> bytes = store.getRange(key, offset, length);
        if (bytes.isEmpty()) {
            throw new SnapshotUnavailableException("snapshot object " + key + " disappeared during the download");
        }
        if (bytes.get().length != length) {
            throw new TransientStoreException("a range of " + key + " returned " + bytes.get().length
                    + " bytes instead of " + length, 0);
        }
        return bytes.get();
    }

    private void sleepBeforeRetry(int attempt) {
        long millis = Math.min(MAX_PAUSE_MILLIS, pause.toMillis() << (attempt - 1));
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new InterruptedIOException("the download was interrupted"));
        }
    }
}
