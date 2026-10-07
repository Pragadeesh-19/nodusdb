package io.nodusdb.storage;

import io.nodusdb.io.FileSync;
import io.nodusdb.kernel.DurableStorage;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogStore;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

final class DurableStore implements DurableStorage {

    private final Path directory;
    private final DirectoryLock lock;
    private final byte[] salt;

    DurableStore(Path directory, DirectoryLock lock, byte[] salt) {
        this.directory = directory;
        this.lock = lock;
        this.salt = salt.clone();
    }

    @Override
    public void checkpoint(GraphKernel kernel, LogStore log) {
        long lsn = log.lastLsn();
        SnapshotMeta meta = new SnapshotMeta(lsn, log.epoch(), log.lastCommitMicros(), salt);
        try {
            Path temp = directory.resolve(GraphFiles.SNAPSHOT_TEMP);
            SnapshotWriter.write(kernel, meta, temp);
            Files.move(temp, directory.resolve(GraphFiles.SNAPSHOT),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            FileSync.directory(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        log.rollSegment();
        log.trim(lsn);
    }

    @Override
    public void close() {
        try {
            lock.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
