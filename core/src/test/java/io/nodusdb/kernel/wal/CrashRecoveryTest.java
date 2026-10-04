package io.nodusdb.kernel.wal;

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

    @Test
    void syncModeRecoversEveryAcceptedEdgeAfterProcessDeath() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(11L, EDGES, NODES);
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.withSyncMode(SyncMode.SYNC));
        opened.kernel().addEdges(pairs, EDGES);
        opened.store().abandon();

        GraphKernel recovered = GraphKernel.open(directory, WalConfig.DEFAULT);
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
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.withSyncMode(SyncMode.SYNC));
        for (int i = 0; i < edges; i++) {
            opened.kernel().addEdge(pairs[2 * i], pairs[2 * i + 1]);
        }
        opened.store().abandon();

        GraphKernel recovered = GraphKernel.open(directory, WalConfig.DEFAULT);
        try {
            assertEquals(edges, GraphFixtures.edgeCount(recovered, NODES));
        } finally {
            recovered.close();
        }
    }

    @Test
    void asyncModeLosesOnlyAnUnflushedTailAndKeepsAnOrderedPrefix() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(12L, EDGES, NODES);
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.DEFAULT);
        for (int i = 0; i < EDGES; i++) {
            opened.kernel().addEdge(pairs[2 * i], pairs[2 * i + 1]);
        }
        opened.store().abandon();

        GraphKernel recovered = GraphKernel.open(directory, WalConfig.DEFAULT);
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
    void syncedAsyncWritesSurviveAbandonAfterExplicitSync() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(13L, EDGES, NODES);
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.DEFAULT);
        for (int i = 0; i < EDGES; i++) {
            opened.kernel().addEdge(pairs[2 * i], pairs[2 * i + 1]);
        }
        opened.kernel().sync();
        opened.store().abandon();

        GraphKernel recovered = GraphKernel.open(directory, WalConfig.DEFAULT);
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
