package io.nodusdb.storage;

import io.nodusdb.error.UpgradeRequiredException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.LogRecovery;
import io.nodusdb.log.RecoveryResult;
import io.nodusdb.log.ReplaySink;
import io.nodusdb.log.SegmentedLog;
import io.nodusdb.log.io.DirectoryLogFileSystem;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.storage.DirectoryFormat.Layout;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Objects;

public final class DurableGraph {

    private static final int LOCAL_WRITER_KEY = 0;
    private static final int SALT_BYTES = 32;
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;

    private static final class KernelSink implements ReplaySink {

        private final GraphKernel kernel;
        private long records;

        KernelSink(GraphKernel kernel) {
            this.kernel = kernel;
        }

        @Override
        public void apply(RecordReader record) {
            kernel.replay(record);
            records++;
        }

        @Override
        public void committed(long commitLsn, long commitMicros) {
        }

        long records() {
            return records;
        }
    }

    private DurableGraph() {
    }

    public static Recovery open(Path directory, LogConfig config) throws IOException {
        return open(directory, config, GraphKernel.NO_MEMORY_LIMIT);
    }

    public static Recovery open(Path directory, LogConfig config, long maxMemoryBytes) throws IOException {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(config, "config");
        Files.createDirectories(directory);
        DirectoryLock lock = DirectoryLock.acquire(directory);
        try {
            return openLocked(directory, config, maxMemoryBytes, lock);
        } catch (IOException | RuntimeException e) {
            releaseQuietly(lock, e);
            throw e;
        }
    }

    private static Recovery openLocked(Path directory, LogConfig config, long maxMemoryBytes, DirectoryLock lock)
            throws IOException {
        Layout layout = DirectoryFormat.detect(directory);
        requireOpenable(layout, directory);
        if (layout == Layout.NEW) {
            DirectoryFormat.writeTripwire(directory);
        }
        Files.deleteIfExists(directory.resolve(GraphFiles.SNAPSHOT_TEMP));
        GraphKernel kernel = new GraphKernel(maxMemoryBytes);
        SnapshotMeta snapshot = loadSnapshot(directory, kernel);
        long afterLsn = snapshot == null ? 0 : snapshot.lsn();
        LogFileSystem files = new DirectoryLogFileSystem(directory.resolve(GraphFiles.LOG_DIRECTORY));
        KernelSink sink = new KernelSink(kernel);
        RecoveryResult recovered = LogRecovery.recover(files, afterLsn, sink);
        if (snapshot != null) {
            recovered = recovered.withCommitMicrosFloor(snapshot.lastCommitMicros());
        }
        long epoch = kernel.epochHistory().latestEpoch() + 1;
        SegmentedLog log = SegmentedLog.open(files, config, recovered, epoch, LOCAL_WRITER_KEY,
                DurableGraph::wallClockMicros);
        try {
            byte[] salt = snapshot == null ? newSalt() : snapshot.salt();
            kernel.recordEpoch(epoch, recovered.lastLsn() + 1, recovered.lastLsn());
            kernel.attachLog(log, new DurableStore(directory, lock, salt));
            if (layout == Layout.NEW) {
                kernel.checkpoint();
                DirectoryFormat.writeFormat(directory);
            }
        } catch (IOException | RuntimeException e) {
            log.abort();
            throw e;
        }
        return new Recovery(kernel, sink.records(), recovered.discardedRecords(), recovered.truncatedBytes());
    }

    private static void requireOpenable(Layout layout, Path directory) {
        if (layout == Layout.LEGACY) {
            throw new UpgradeRequiredException("graph directory " + directory
                    + " was written by an earlier version; run upgrade before opening it");
        }
        if (layout == Layout.INTERRUPTED_UPGRADE) {
            throw new UpgradeRequiredException("an upgrade of " + directory
                    + " was interrupted; run upgrade again to finish it");
        }
    }

    private static SnapshotMeta loadSnapshot(Path directory, GraphKernel kernel) throws IOException {
        Path snapshot = directory.resolve(GraphFiles.SNAPSHOT);
        return Files.isRegularFile(snapshot) ? SnapshotReader.load(snapshot, kernel) : null;
    }

    private static byte[] newSalt() {
        byte[] salt = new byte[SALT_BYTES];
        new SecureRandom().nextBytes(salt);
        return salt;
    }

    static long wallClockMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * MICROS_PER_SECOND + now.getNano() / NANOS_PER_MICRO;
    }

    private static void releaseQuietly(DirectoryLock lock, Exception cause) {
        try {
            lock.close();
        } catch (IOException e) {
            cause.addSuppressed(e);
        }
    }
}
