package io.nodusdb.ship;

import java.nio.file.Path;
import java.util.Objects;

public record StagedSnapshot(Path file, long lsn, Runnable release) implements AutoCloseable {

    public StagedSnapshot {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(release, "release");
        if (lsn < 0) {
            throw new IllegalArgumentException("a snapshot LSN must not be negative: " + lsn);
        }
    }

    @Override
    public void close() {
        release.run();
    }
}
