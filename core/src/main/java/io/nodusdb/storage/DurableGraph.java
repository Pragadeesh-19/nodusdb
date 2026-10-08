package io.nodusdb.storage;

import io.nodusdb.error.UpgradeRequiredException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.LogRecovery;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.RecoveryResult;
import io.nodusdb.log.ReplaySink;
import io.nodusdb.log.SegmentedLog;
import io.nodusdb.log.io.DirectoryLogFileSystem;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.ship.OpenReconciler;
import io.nodusdb.ship.ShippingBootstrap;
import io.nodusdb.ship.ShippingBootstrap.Reconciled;
import io.nodusdb.ship.ShippingConfig;
import io.nodusdb.ship.ShippingLogStore;
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
        return open(directory, config, maxMemoryBytes, null);
    }

    public static Recovery open(Path directory, LogConfig config, long maxMemoryBytes, ShippingConfig shipping)
            throws IOException {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(config, "config");
        Files.createDirectories(directory);
        DirectoryLock lock = DirectoryLock.acquire(directory);
        try {
            return openLocked(directory, config, maxMemoryBytes, shipping, lock);
        } catch (IOException | RuntimeException e) {
            releaseQuietly(lock, e);
            throw e;
        }
    }

    private static Recovery openLocked(Path directory, LogConfig config, long maxMemoryBytes,
                                       ShippingConfig shipping, DirectoryLock lock) throws IOException {
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
        byte[] salt = snapshot == null ? newSalt() : snapshot.salt();
        long latestEpoch = kernel.epochHistory().latestEpoch();
        SnapshotStaging staging = new SnapshotStaging(directory.resolve(GraphFiles.SHIP_STAGING));
        ShippingRuntime runtime = null;
        long epoch = latestEpoch + 1;
        if (shipping != null) {
            staging.reset();
            SnapshotMeta openingState = new SnapshotMeta(recovered.lastLsn(), latestEpoch,
                    recovered.lastCommitMicros(), salt);
            Reconciled reconciled = ShippingBootstrap.reconcile(shipping,
                    new OpenReconciler.Local(recovered.lastLsn(), latestEpoch, oldestRetainedLsn(recovered)),
                    staging.writing(kernel, openingState), DurableGraph::wallClockMicros);
            runtime = ShippingRuntime.begin(reconciled, staging);
            epoch = reconciled.start().epoch();
        }
        SegmentedLog log;
        try {
            log = SegmentedLog.open(files, config, recovered, epoch, LOCAL_WRITER_KEY, DurableGraph::wallClockMicros);
        } catch (IOException | RuntimeException e) {
            closeQuietly(runtime, e);
            throw e;
        }
        try {
            LogStore attached = runtime == null ? log : new ShippingLogStore(log, runtime.state(), runtime::close);
            CheckpointListener listener = runtime == null ? CheckpointListener.NONE : runtime.checkpointListener();
            kernel.recordEpoch(epoch, recovered.lastLsn() + 1, recovered.lastLsn());
            kernel.attachLog(attached, new DurableStore(directory, lock, salt, listener));
            if (layout == Layout.NEW) {
                kernel.checkpoint();
                DirectoryFormat.writeFormat(directory);
            }
            if (runtime != null) {
                runtime.startShipping(kernel, log, files, directory.resolve(GraphFiles.SHIP_SCRATCH));
                kernel.attachShipping(runtime.state());
            }
        } catch (IOException | RuntimeException e) {
            closeQuietly(runtime, e);
            log.abort();
            throw e;
        }
        return new Recovery(kernel, sink.records(), recovered.discardedRecords(), recovered.truncatedBytes());
    }

    private static long oldestRetainedLsn(RecoveryResult recovered) {
        return recovered.segmentBases().stream().mapToLong(Long::longValue).min().orElse(recovered.lastLsn() + 1);
    }

    private static void closeQuietly(ShippingRuntime runtime, Exception cause) {
        if (runtime == null) {
            return;
        }
        try {
            runtime.close();
        } catch (RuntimeException e) {
            cause.addSuppressed(e);
        }
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
