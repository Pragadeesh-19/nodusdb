package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.ship.SnapshotSource;
import io.nodusdb.ship.StagedSnapshot;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotWriter;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

final class SnapshotStaging {

    private static final String SUFFIX = ".nsnap";

    private final Path directory;
    private final AtomicLong sequence = new AtomicLong();

    SnapshotStaging(Path directory) {
        this.directory = directory;
    }

    void reset() throws IOException {
        Files.createDirectories(directory);
        try (DirectoryStream<Path> leftovers = Files.newDirectoryStream(directory)) {
            for (Path leftover : leftovers) {
                Files.deleteIfExists(leftover);
            }
        }
    }

    StagedSnapshot link(Path snapshot, long lsn) throws IOException {
        Path target = next(lsn);
        try {
            Files.createLink(target, snapshot);
        } catch (IOException | UnsupportedOperationException linkUnavailable) {
            Files.copy(snapshot, target);
        }
        return staged(target, lsn);
    }

    SnapshotSource writing(GraphKernel kernel, SnapshotMeta meta) {
        return () -> {
            Path target = next(meta.lsn());
            try {
                SnapshotWriter.write(kernel, meta, target);
            } catch (IOException | RuntimeException failure) {
                discardQuietly(target);
                throw failure;
            }
            return staged(target, meta.lsn());
        };
    }

    private Path next(long lsn) throws IOException {
        Files.createDirectories(directory);
        return directory.resolve(String.format("%020d-%d%s", lsn, sequence.incrementAndGet(), SUFFIX));
    }

    private static StagedSnapshot staged(Path file, long lsn) {
        return new StagedSnapshot(file, lsn, () -> discardQuietly(file));
    }

    private static void discardQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException leftover) {
            return;
        }
    }
}
