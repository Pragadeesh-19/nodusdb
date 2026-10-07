package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrashRecoveryTest {

    private static final int NODES = 50_000;
    private static final int EDGES = 100_000;

    @TempDir
    Path directory;

    @TempDir
    Path image;

    @Test
    void syncModeRecoversEveryAcceptedEdgeAfterProcessDeath() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(11L, EDGES, NODES);
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            live.addEdges(pairs, EDGES);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        GraphKernel recovered = StoredGraphs.open(image);
        try {
            assertEquals(EDGES, GraphFixtures.edgeCount(recovered, NODES));
            for (int i = 0; i < EDGES; i++) {
                assertTrue(recovered.hasEdge(pairs[2 * i], pairs[2 * i + 1]), "edge " + i + " lost");
            }
            GraphFixtures.assertInDegreesMatchForward(recovered, NODES);
        } finally {
            recovered.close();
        }
    }

    @Test
    void singleCallSyncWritesAreDurableBeforeTheyReturn() throws IOException {
        int edges = 2_000;
        long[] pairs = GraphFixtures.skewedPairs(14L, edges, NODES);
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            for (int i = 0; i < edges; i++) {
                live.addEdge(pairs[2 * i], pairs[2 * i + 1]);
            }
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        GraphKernel recovered = StoredGraphs.open(image);
        try {
            assertEquals(edges, GraphFixtures.edgeCount(recovered, NODES));
        } finally {
            recovered.close();
        }
    }

    @Test
    void asyncModeLosesOnlyAnUnflushedTailAndKeepsAnOrderedPrefix() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(12L, EDGES, NODES);
        GraphKernel live = StoredGraphs.open(directory);
        try {
            for (int i = 0; i < EDGES; i++) {
                live.addEdge(pairs[2 * i], pairs[2 * i + 1]);
            }
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        GraphKernel recovered = StoredGraphs.open(image);
        try {
            int count = GraphFixtures.edgeCount(recovered, NODES);
            assertTrue(count <= EDGES, "recovered more edges than were written");
            for (int i = 0; i < EDGES; i++) {
                boolean present = recovered.hasEdge(pairs[2 * i], pairs[2 * i + 1]);
                assertEquals(i < count, present, "prefix broken at edge " + i);
            }
            GraphFixtures.assertInDegreesMatchForward(recovered, NODES);
        } finally {
            recovered.close();
        }
    }

    @Test
    void syncedAsyncWritesSurviveACrashAfterExplicitSync() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(13L, EDGES, NODES);
        GraphKernel live = StoredGraphs.open(directory);
        try {
            for (int i = 0; i < EDGES; i++) {
                live.addEdge(pairs[2 * i], pairs[2 * i + 1]);
            }
            live.sync();
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        GraphKernel recovered = StoredGraphs.open(image);
        try {
            assertEquals(EDGES, GraphFixtures.edgeCount(recovered, NODES));
            for (int i = 0; i < EDGES; i++) {
                assertTrue(recovered.hasEdge(pairs[2 * i], pairs[2 * i + 1]), "edge " + i + " lost after sync");
            }
        } finally {
            recovered.close();
        }
    }
}
