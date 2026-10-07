package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableMemoryLimitTest {

    private static final int CHAIN_BOUND = 2_000_000;

    @TempDir
    Path directory;

    @TempDir
    Path image;

    @Test
    void aRejectedWriteIsNeverJournaled() throws IOException {
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC, initialFootprint() + 30_000);
        long rejected;
        try {
            rejected = fillChainUntilRejected(live);
            assertTrue(rejected > 0);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        try (GraphKernel reopened = StoredGraphs.open(image)) {
            for (long node = 0; node < rejected; node++) {
                assertTrue(reopened.hasEdge(node, node + 1), "accepted edge " + node + " was lost");
            }
            assertFalse(reopened.hasEdge(rejected, rejected + 1), "the rejected edge reached the log");
            assertEquals(0, reopened.getDegree(rejected));
        }
    }

    @Test
    void aRejectedBatchKeepsOnlyTheEdgesItAppliedInTheLog() throws IOException {
        int pairs = 100_000;
        long[] batch = new long[2 * pairs];
        for (int i = 0; i < pairs; i++) {
            batch[2 * i] = i % 500;
            batch[2 * i + 1] = 500 + (i / 500) % 500;
        }
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC, initialFootprint() + 80_000);
        int applied = 0;
        try {
            assertThrows(MemoryLimitExceededException.class, () -> live.addEdges(batch, pairs));
            while (applied < pairs && live.hasEdge(batch[2 * applied], batch[2 * applied + 1])) {
                applied++;
            }
            assertTrue(applied > 0 && applied < pairs);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        try (GraphKernel reopened = StoredGraphs.open(image)) {
            for (int i = 0; i < pairs; i++) {
                assertEquals(i < applied, reopened.hasEdge(batch[2 * i], batch[2 * i + 1]), "edge " + i);
            }
        }
    }

    @Test
    void openingAStoredGraphUnderASmallerLimitFailsAndReleasesTheDirectory() throws IOException {
        try (GraphKernel kernel = StoredGraphs.open(directory, StoredGraphs.SYNC)) {
            for (long node = 0; node < 5_000; node++) {
                kernel.addEdge(node, node + 1);
            }
        }

        assertThrows(MemoryLimitExceededException.class,
                () -> StoredGraphs.open(directory, StoredGraphs.SYNC, initialFootprint() + 1_000));

        try (GraphKernel reopened = StoredGraphs.open(directory)) {
            assertTrue(reopened.hasEdge(0, 1));
            assertTrue(reopened.hasEdge(4_999, 5_000));
        }
    }

    @Test
    void aStoredGraphOpensUnderALimitThatFitsIt() throws IOException {
        try (GraphKernel kernel = StoredGraphs.open(directory, StoredGraphs.SYNC)) {
            for (long node = 0; node < 500; node++) {
                kernel.addEdge(node, node + 1);
            }
        }

        try (GraphKernel reopened = StoredGraphs.open(directory, StoredGraphs.SYNC, 8L << 20)) {
            assertTrue(reopened.hasEdge(0, 1));
            assertTrue(reopened.hasEdge(499, 500));
            assertEquals(8L << 20, reopened.memoryLimitBytes());
        }
    }

    private static long fillChainUntilRejected(GraphKernel kernel) {
        for (long node = 0; node < CHAIN_BOUND; node++) {
            try {
                kernel.addEdge(node, node + 1);
            } catch (MemoryLimitExceededException expected) {
                return node;
            }
        }
        throw new AssertionError("the limit was never reached");
    }

    private static long initialFootprint() {
        return new GraphKernel().memoryUsedBytes();
    }
}
