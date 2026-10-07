package io.nodusdb.storage;

import io.nodusdb.error.UpgradeFailedException;
import io.nodusdb.io.FileSync;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.storage.DirectoryFormat.Layout;
import io.nodusdb.storage.legacy.LegacyGraph;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

public final class GraphUpgrade {

    public record Report(boolean performed, long edges, int symbols) {

        static Report notNeeded() {
            return new Report(false, 0, 0);
        }
    }

    private static final int SALT_BYTES = 32;
    private static final int SPACE_FACTOR = 2;

    private GraphUpgrade() {
    }

    public static Report upgrade(Path directory) throws IOException {
        return upgrade(directory, step -> { });
    }

    public static void cleanup(Path directory) throws IOException {
        Objects.requireNonNull(directory, "directory");
        DirectoryLock.whileHeld(directory, () -> {
            if (DirectoryFormat.detect(directory) != Layout.CURRENT) {
                throw new IllegalStateException("only a graph in the current format has a backup to remove");
            }
            FileTrees.deleteRecursively(LegacyBackup.location(directory));
            FileSync.directory(directory);
            return null;
        });
    }

    static Report upgrade(Path directory, Consumer<UpgradeStep> observer) throws IOException {
        Objects.requireNonNull(directory, "directory");
        if (!Files.isDirectory(directory)) {
            throw new IOException("no graph directory at " + directory);
        }
        Optional<GraphDigest> converted = DirectoryLock.whileHeld(directory,
                () -> convertIfNeeded(directory, observer));
        if (converted.isEmpty()) {
            return Report.notNeeded();
        }
        GraphDigest expected = converted.get();
        verify(directory, expected);
        return new Report(true, expected.edges(), expected.symbols());
    }

    private static Optional<GraphDigest> convertIfNeeded(Path directory, Consumer<UpgradeStep> observer)
            throws IOException {
        Layout layout = DirectoryFormat.detect(directory);
        if (layout == Layout.CURRENT) {
            removeLeftovers(directory);
            return Optional.empty();
        }
        if (layout == Layout.NEW) {
            return Optional.empty();
        }
        return Optional.of(convert(directory, observer));
    }

    private static GraphDigest convert(Path directory, Consumer<UpgradeStep> observer) throws IOException {
        boolean backedUp = LegacyBackup.exists(directory);
        Path source = backedUp ? LegacyBackup.location(directory) : directory;
        if (!backedUp) {
            requireSpace(directory);
        }
        GraphKernel legacy = new GraphKernel();
        LegacyGraph.recover(source, legacy);
        GraphDigest expected = GraphDigest.of(legacy);
        observer.accept(UpgradeStep.LEGACY_RECOVERED);
        if (!backedUp) {
            LegacyBackup.create(directory, observer);
        }
        DirectoryFormat.writeTripwire(directory);
        observer.accept(UpgradeStep.TRIPWIRE_WRITTEN);
        Path staging = directory.resolve(GraphFiles.STAGING_DIRECTORY);
        FileTrees.deleteRecursively(staging);
        StagedGraph.build(staging, legacy, newSalt());
        observer.accept(UpgradeStep.STAGING_BUILT);
        install(directory, staging, observer);
        DirectoryFormat.writeFormat(directory);
        observer.accept(UpgradeStep.FORMAT_WRITTEN);
        removeLeftovers(directory);
        observer.accept(UpgradeStep.LEFTOVERS_REMOVED);
        return expected;
    }

    private static void requireSpace(Path directory) throws IOException {
        long needed = SPACE_FACTOR * LegacyBackup.legacyBytes(directory);
        long available = Files.getFileStore(directory).getUsableSpace();
        if (available < needed) {
            throw new UpgradeFailedException("upgrading " + directory + " needs " + needed
                    + " bytes of free space and only " + available + " are available");
        }
    }

    private static void install(Path directory, Path staging, Consumer<UpgradeStep> observer) throws IOException {
        Path log = directory.resolve(GraphFiles.LOG_DIRECTORY);
        FileTrees.deleteRecursively(log);
        Files.move(staging.resolve(GraphFiles.LOG_DIRECTORY), log, StandardCopyOption.ATOMIC_MOVE);
        FileSync.directory(directory);
        observer.accept(UpgradeStep.LOG_INSTALLED);
        Files.move(staging.resolve(GraphFiles.SNAPSHOT), directory.resolve(GraphFiles.SNAPSHOT),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        FileSync.directory(directory);
        observer.accept(UpgradeStep.SNAPSHOT_INSTALLED);
    }

    private static void removeLeftovers(Path directory) throws IOException {
        Files.deleteIfExists(directory.resolve(GraphFiles.LEGACY_SYMBOLS));
        Files.deleteIfExists(directory.resolve(GraphFiles.FORMAT + ".tmp"));
        FileTrees.deleteRecursively(directory.resolve(GraphFiles.STAGING_DIRECTORY));
        FileTrees.deleteRecursively(directory.resolve(GraphFiles.BACKUP_DIRECTORY + ".partial"));
        FileSync.directory(directory);
    }

    private static void verify(Path directory, GraphDigest expected) throws IOException {
        GraphKernel upgraded = DurableGraph.open(directory, LogConfig.DEFAULT).kernel();
        try {
            GraphDigest actual = GraphDigest.of(upgraded);
            if (!actual.equals(expected)) {
                throw new UpgradeFailedException("the upgraded graph differs from the original: expected "
                        + expected + " but found " + actual + "; the original files are kept in "
                        + GraphFiles.BACKUP_DIRECTORY);
            }
        } finally {
            upgraded.close();
        }
    }

    private static byte[] newSalt() {
        byte[] salt = new byte[SALT_BYTES];
        new SecureRandom().nextBytes(salt);
        return salt;
    }
}
