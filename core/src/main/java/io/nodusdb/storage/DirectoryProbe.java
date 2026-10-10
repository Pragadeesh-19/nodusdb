package io.nodusdb.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class DirectoryProbe {

    private DirectoryProbe() {
    }

    public static void requireNotOpen(Path directory) throws IOException {
        Path lockFile = directory.resolve(GraphFiles.LOCK);
        if (!Files.isRegularFile(lockFile)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ)) {
            FileLock shared;
            try {
                shared = channel.tryLock(0, Long.MAX_VALUE, true);
            } catch (OverlappingFileLockException sameProcess) {
                shared = null;
            }
            if (shared == null) {
                throw new IllegalStateException("graph directory is already open: " + directory);
            }
            shared.release();
        }
    }
}
