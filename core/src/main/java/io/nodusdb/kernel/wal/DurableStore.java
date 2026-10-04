package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Persistence;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

final class DurableStore implements Persistence {

    static final String SNAPSHOT = "snapshot.bin";
    static final String SNAPSHOT_TEMP = "snapshot.bin.tmp";
    static final String LOG = "nodus.wal";
    static final String LOG_TEMP = "nodus.wal.tmp";

    private final Path directory;
    private final GraphKernel kernel;
    private final WalWriter log;
    private final DirectoryLock lock;
    private boolean closed;

    DurableStore(Path directory, GraphKernel kernel, WalWriter log, DirectoryLock lock) {
        this.directory = directory;
        this.kernel = kernel;
        this.log = log;
        this.lock = lock;
    }

    @Override
    public void recordAdd(long u, long v) {
        log.append(WalFormat.OP_ADD_EDGE, u, v);
    }

    @Override
    public void recordRemove(long u, long v) {
        log.append(WalFormat.OP_REMOVE_EDGE, u, v);
    }

    @Override
    public void sync() {
        log.drain();
    }

    @Override
    public void beginBatch() {
        log.deferWaits(true);
    }

    @Override
    public void endBatch() {
        log.deferWaits(false);
        log.commitDeferred();
    }

    @Override
    public synchronized void checkpoint() {
        if (closed) {
            throw new IllegalStateException("graph is closed");
        }
        writeCheckpoint();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writeCheckpoint();
        } finally {
            releaseResources();
        }
    }

    synchronized void abandon() {
        if (closed) {
            return;
        }
        closed = true;
        log.abort();
        closeLock();
    }

    private void writeCheckpoint() {
        try {
            Path snapshotTemp = directory.resolve(SNAPSHOT_TEMP);
            SnapshotFile.write(kernel, snapshotTemp);
            Files.move(snapshotTemp, directory.resolve(SNAPSHOT),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Path logTemp = directory.resolve(LOG_TEMP);
            WalFormat.writeEmptyLog(logTemp);
            log.replaceLog(logTemp);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void releaseResources() {
        try {
            log.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeLock();
        }
    }

    private void closeLock() {
        try {
            lock.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
