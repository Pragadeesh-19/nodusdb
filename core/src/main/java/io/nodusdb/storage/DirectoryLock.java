package io.nodusdb.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class DirectoryLock implements AutoCloseable {

    private final FileChannel channel;
    private final FileLock lock;

    private DirectoryLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    @FunctionalInterface
    interface Action<T> {

        T run() throws IOException;
    }

    static <T> T whileHeld(Path directory, Action<T> action) throws IOException {
        DirectoryLock lock = acquire(directory);
        T result;
        try {
            result = action.run();
        } catch (IOException | RuntimeException e) {
            try {
                lock.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        lock.close();
        return result;
    }

    static DirectoryLock acquire(Path directory) throws IOException {
        FileChannel channel = FileChannel.open(directory.resolve(GraphFiles.LOCK),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            lock = null;
        }
        if (lock == null) {
            channel.close();
            throw new IllegalStateException("graph directory is already open: " + directory);
        }
        return new DirectoryLock(channel, lock);
    }

    @Override
    public void close() throws IOException {
        try {
            lock.release();
        } finally {
            channel.close();
        }
    }
}
