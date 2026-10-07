package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

final class StoredGraphs {

    static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    static final LogConfig SYNC_WITH_FAST_MARKS = new LogConfig(SyncMode.SYNC, 1L, LogConfig.DEFAULT.bufferBytes(),
            LogConfig.DEFAULT.segmentBytes(), 1L);

    private StoredGraphs() {
    }

    static GraphKernel open(Path directory) throws IOException {
        return open(directory, LogConfig.DEFAULT);
    }

    static GraphKernel open(Path directory, LogConfig config) throws IOException {
        return DurableGraph.open(directory, config).kernel();
    }

    static GraphKernel open(Path directory, LogConfig config, long maxMemoryBytes) throws IOException {
        return DurableGraph.open(directory, config, maxMemoryBytes).kernel();
    }

    static Recovery recover(Path directory) throws IOException {
        return DurableGraph.open(directory, LogConfig.DEFAULT);
    }

    static void crashImage(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes)
                    throws IOException {
                Files.createDirectories(target.resolve(source.relativize(directory)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                if (!file.getFileName().toString().equals(GraphFiles.LOCK)) {
                    Files.copy(file, target.resolve(source.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
