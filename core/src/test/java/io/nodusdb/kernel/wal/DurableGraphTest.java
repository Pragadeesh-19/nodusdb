package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableGraphTest {

    private static final int NODES = 1_000;

    @TempDir
    Path directory;

    @Test
    void duplicateAddAndMissingRemoveWriteNothingToTheLog() throws IOException {
        Path log = directory.resolve(DurableStore.LOG);
        GraphKernel graph = GraphKernel.open(directory, WalConfig.DEFAULT);
        try {
            assertTrue(graph.addEdge(1, 2));
            graph.sync();
            long sizeAfterAdd = Files.size(log);

            for (int i = 0; i < 1_000; i++) {
                assertFalse(graph.addEdge(1, 2));
                assertFalse(graph.removeEdge(7, 8));
            }
            graph.sync();

            assertEquals(sizeAfterAdd, Files.size(log));
        } finally {
            graph.close();
        }
    }

    @Test
    void batchMutationsReplayAfterCleanClose() throws IOException {
        long[] adds = {1, 2, 2, 3, 3, 4, 4, 5, 1, 5};
        long[] removes = {2, 3, 9, 9};
        GraphKernel graph = GraphKernel.open(directory, WalConfig.DEFAULT);
        assertEquals(5, graph.addEdges(adds, 5));
        assertEquals(1, graph.removeEdges(removes, 2));
        graph.close();

        GraphKernel reopened = GraphKernel.open(directory, WalConfig.DEFAULT);
        try {
            assertEquals(4, GraphFixtures.edgeCount(reopened, NODES));
            assertFalse(reopened.hasEdge(2, 3));
            assertTrue(reopened.hasEdge(4, 5));
            assertTrue(reopened.hasEdge(1, 5));
        } finally {
            reopened.close();
        }
    }

    @Test
    void cleanCloseLeavesASnapshotAndAnEmptyLog() throws IOException {
        GraphKernel graph = GraphKernel.open(directory, WalConfig.DEFAULT);
        graph.addEdge(10, 20);
        graph.close();

        assertTrue(Files.exists(directory.resolve(DurableStore.SNAPSHOT)));
        assertEquals(WalFormat.HEADER_BYTES, Files.size(directory.resolve(DurableStore.LOG)));
    }

    @Test
    void directoryCannotBeOpenedTwiceAtTheSameTime() throws IOException {
        GraphKernel first = GraphKernel.open(directory, WalConfig.DEFAULT);
        try {
            assertThrows(IllegalStateException.class, () -> GraphKernel.open(directory, WalConfig.DEFAULT));
        } finally {
            first.close();
        }
        GraphKernel reopened = GraphKernel.open(directory, WalConfig.DEFAULT);
        reopened.close();
    }

    @Test
    void mutationsAfterCloseAreRejected() throws IOException {
        GraphKernel graph = GraphKernel.open(directory, WalConfig.DEFAULT);
        graph.close();

        assertThrows(IllegalStateException.class, () -> graph.addEdge(1, 2));
        assertThrows(IllegalStateException.class, graph::checkpoint);
    }

    @Test
    void inMemoryGraphHasNothingToCheckpointOrSync() {
        GraphKernel graph = GraphKernel.openInMemory();

        assertFalse(graph.isDurable());
        assertThrows(IllegalStateException.class, graph::checkpoint);
        assertThrows(IllegalStateException.class, graph::sync);
        graph.close();
    }
}
