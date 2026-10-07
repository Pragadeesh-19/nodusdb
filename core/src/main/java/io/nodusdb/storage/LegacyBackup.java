package io.nodusdb.storage;

import io.nodusdb.storage.legacy.LegacyGraph;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Consumer;

final class LegacyBackup {

    private static final List<String> FILES = List.of(LegacyGraph.SNAPSHOT, LegacyGraph.LOG, LegacyGraph.SYMBOLS);

    private LegacyBackup() {
    }

    static boolean exists(Path directory) {
        return Files.isDirectory(directory.resolve(GraphFiles.BACKUP_DIRECTORY));
    }

    static Path location(Path directory) {
        return directory.resolve(GraphFiles.BACKUP_DIRECTORY);
    }

    static long legacyBytes(Path directory) throws IOException {
        long bytes = 0;
        for (String name : FILES) {
            Path file = directory.resolve(name);
            if (Files.isRegularFile(file)) {
                bytes += Files.size(file);
            }
        }
        return bytes;
    }

    static void create(Path directory, Consumer<UpgradeStep> observer) throws IOException {
        Path partial = directory.resolve(GraphFiles.BACKUP_DIRECTORY + ".partial");
        FileTrees.deleteRecursively(partial);
        Files.createDirectories(partial);
        for (String name : FILES) {
            Path source = directory.resolve(name);
            if (Files.isRegularFile(source)) {
                Path copy = partial.resolve(name);
                Files.copy(source, copy);
                force(copy);
                observer.accept(UpgradeStep.BACKUP_FILE_COPIED);
            }
        }
        DirectoryFormat.syncDirectory(partial);
        Files.move(partial, location(directory), StandardCopyOption.ATOMIC_MOVE);
        DirectoryFormat.syncDirectory(directory);
        observer.accept(UpgradeStep.BACKUP_INSTALLED);
    }

    private static void force(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }
}
