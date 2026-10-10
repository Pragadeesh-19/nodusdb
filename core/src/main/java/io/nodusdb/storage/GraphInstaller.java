package io.nodusdb.storage;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.io.FileSync;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

public final class GraphInstaller {

    static final String STAGING_SUFFIX = ".restore-tmp";

    private GraphInstaller() {
    }

    public static void install(GraphKernel kernel, byte[] salt, Path target) throws IOException {
        install(kernel, salt, target, step -> { });
    }

    static void install(GraphKernel kernel, byte[] salt, Path target, Consumer<InstallStep> observer)
            throws IOException {
        Objects.requireNonNull(kernel, "kernel");
        Objects.requireNonNull(salt, "salt");
        Objects.requireNonNull(target, "target");
        Path destination = target.toAbsolutePath();
        Path parent = destination.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("a restore target needs a parent directory: " + target);
        }
        requireVacant(destination);
        SnapshotMeta meta = new SnapshotMeta(kernel.appliedLsn(), kernel.epoch(), kernel.lastCommitMicros(), salt);
        Files.createDirectories(parent);
        Path staging = parent.resolve(destination.getFileName() + STAGING_SUFFIX);
        FileTrees.deleteRecursively(staging);
        try {
            stage(kernel, meta, staging, observer);
            clear(destination, observer);
            Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE);
            FileSync.directory(parent);
            observer.accept(InstallStep.RENAMED);
        } catch (IOException | RuntimeException failure) {
            discard(staging, failure);
            throw failure;
        }
    }

    public static void requireVacant(Path target) throws IOException {
        if (Files.notExists(target)) {
            return;
        }
        if (!Files.isDirectory(target) || !isEmpty(target)) {
            throw new UnsupportedFeatureException("the restore target " + target
                    + " must not exist or must be an empty directory");
        }
    }

    private static boolean isEmpty(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.findAny().isEmpty();
        }
    }

    private static void stage(GraphKernel kernel, SnapshotMeta meta, Path staging, Consumer<InstallStep> observer)
            throws IOException {
        Files.createDirectories(staging.resolve(GraphFiles.LOG_DIRECTORY));
        observer.accept(InstallStep.STAGING_CREATED);
        SnapshotWriter.write(kernel, meta, staging.resolve(GraphFiles.SNAPSHOT));
        observer.accept(InstallStep.SNAPSHOT_WRITTEN);
        DirectoryFormat.writeTripwire(staging);
        observer.accept(InstallStep.TRIPWIRE_WRITTEN);
        DirectoryFormat.writeFormat(staging);
        observer.accept(InstallStep.FORMAT_WRITTEN);
        FileSync.directory(staging);
    }

    private static void clear(Path target, Consumer<InstallStep> observer) throws IOException {
        if (Files.exists(target)) {
            Files.delete(target);
            observer.accept(InstallStep.TARGET_CLEARED);
        }
    }

    private static void discard(Path staging, Exception failure) {
        try {
            FileTrees.deleteRecursively(staging);
        } catch (IOException cleanup) {
            failure.addSuppressed(cleanup);
        }
    }
}
