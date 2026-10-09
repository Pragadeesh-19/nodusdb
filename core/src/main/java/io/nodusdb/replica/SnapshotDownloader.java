package io.nodusdb.replica;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class SnapshotDownloader {

    public static final int DEFAULT_RANGE_BYTES = 8 << 20;
    public static final int MAX_PARALLELISM = 16;

    private static final Duration DEFAULT_RETRY_PAUSE = Duration.ofMillis(100);

    private final ObjectStore store;
    private final int parallelism;
    private final int rangeBytes;
    private final RangeReader reader;

    public SnapshotDownloader(ObjectStore store, int parallelism) {
        this(store, parallelism, DEFAULT_RANGE_BYTES, DEFAULT_RETRY_PAUSE);
    }

    SnapshotDownloader(ObjectStore store, int parallelism, int rangeBytes, Duration retryPause) {
        if (parallelism < 1 || parallelism > MAX_PARALLELISM) {
            throw new IllegalArgumentException("the download parallelism must be between 1 and " + MAX_PARALLELISM
                    + ": " + parallelism);
        }
        if (rangeBytes < 1) {
            throw new IllegalArgumentException("a range must hold at least one byte");
        }
        this.store = store;
        this.parallelism = parallelism;
        this.rangeBytes = rangeBytes;
        this.reader = new RangeReader(store, retryPause);
    }

    public Path download(String key, ChainHash expected, Path directory) throws IOException {
        ObjectInfo info = store.head(key).orElseThrow(() -> new SnapshotUnavailableException(
                "snapshot object " + key + " does not exist"));
        Path part = Files.createTempFile(directory, "snapshot-", ".part");
        try {
            fetch(key, info.size(), part);
            verify(key, expected, part);
            return part;
        } catch (IOException | RuntimeException | Error failure) {
            discard(part, failure);
            throw failure;
        }
    }

    private void fetch(String key, long size, Path part) throws IOException {
        int ranges = (int) ((size + rangeBytes - 1) / rangeBytes);
        if (ranges == 0) {
            return;
        }
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(parallelism, ranges), threads());
        AtomicBoolean failed = new AtomicBoolean();
        try (FileChannel channel = FileChannel.open(part, StandardOpenOption.WRITE)) {
            List<Future<?>> pending = new ArrayList<>(ranges);
            for (long offset = 0; offset < size; offset += rangeBytes) {
                long start = offset;
                int length = (int) Math.min(rangeBytes, size - offset);
                pending.add(pool.submit(() -> copy(channel, key, start, length, failed)));
            }
            await(pending);
        } finally {
            pool.shutdownNow();
        }
    }

    private void copy(FileChannel channel, String key, long offset, int length, AtomicBoolean failed) {
        if (failed.get()) {
            return;
        }
        try {
            write(channel, offset, reader.read(key, offset, length));
        } catch (RuntimeException | Error failure) {
            failed.set(true);
            throw failure;
        }
    }

    private static void write(FileChannel channel, long offset, byte[] bytes) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            long position = offset;
            while (buffer.hasRemaining()) {
                position += channel.write(buffer, position);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void await(List<Future<?>> pending) throws IOException {
        try {
            for (Future<?> range : pending) {
                range.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("the download was interrupted");
        } catch (ExecutionException e) {
            rethrow(e.getCause());
        }
    }

    private static void rethrow(Throwable cause) throws IOException {
        if (cause instanceof UncheckedIOException io) {
            throw io.getCause();
        }
        if (cause instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        throw new IOException(cause);
    }

    private static void verify(String key, ChainHash expected, Path part) throws IOException {
        if (!ChainHash.sha256(part).equals(expected)) {
            throw new ChainTrustException("the snapshot object " + key
                    + " does not match the hash in its signed reference");
        }
    }

    private static void discard(Path part, Throwable cause) {
        try {
            Files.deleteIfExists(part);
        } catch (IOException e) {
            cause.addSuppressed(e);
        }
    }

    private static ThreadFactory threads() {
        AtomicInteger counter = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "nodus-snapshot-download-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
