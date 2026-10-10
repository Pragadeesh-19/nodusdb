package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DirectoryProbeTest {

    @TempDir
    Path root;

    @Test
    void aDirectoryWithNoLockFileIsNotInUse() throws IOException {
        DirectoryProbe.requireNotOpen(root);

        assertFalse(Files.exists(root.resolve(GraphFiles.LOCK)));
    }

    @Test
    void aMissingDirectoryIsNotInUse() throws IOException {
        DirectoryProbe.requireNotOpen(root.resolve("absent"));
    }

    @Test
    void anOpenGraphDirectoryIsInUse() throws IOException {
        GraphKernel kernel = DurableGraph.open(root, LogConfig.withSyncMode(SyncMode.SYNC)).kernel();
        try {
            assertThrows(IllegalStateException.class, () -> DirectoryProbe.requireNotOpen(root));
        } finally {
            kernel.close();
        }
    }

    @Test
    void aClosedGraphDirectoryIsNotInUseAndTheProbeLeavesItUnchanged() throws IOException {
        DurableGraph.open(root, LogConfig.withSyncMode(SyncMode.SYNC)).kernel().close();
        List<String> before = listing();

        DirectoryProbe.requireNotOpen(root);

        assertEquals(before, listing());
    }

    private List<String> listing() throws IOException {
        try (var files = Files.walk(root)) {
            return files.map(path -> root.relativize(path) + ":" + path.toFile().length()).sorted().toList();
        }
    }
}
