package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotRollTest {

    private static final int NODES = 60_000;
    private static final int BASE_EDGES = 500_000;
    private static final int DELTA_EDGES = 10_000;

    @TempDir
    Path directory;

    @Test
    void checkpointRollsTheLogAndRecoveryReplaysOnlyTheDelta() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(31L, BASE_EDGES + DELTA_EDGES, NODES);
        long[] base = Arrays.copyOf(pairs, 2 * BASE_EDGES);
        long[] delta = Arrays.copyOfRange(pairs, 2 * BASE_EDGES, 2 * (BASE_EDGES + DELTA_EDGES));
        Path log = directory.resolve(DurableStore.LOG);

        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.DEFAULT);
        opened.kernel().addEdges(base, BASE_EDGES);
        opened.kernel().checkpoint();

        assertTrue(Files.exists(directory.resolve(DurableStore.SNAPSHOT)), "snapshot missing after checkpoint");
        assertEquals(WalFormat.HEADER_BYTES, Files.size(log), "log was not rolled");

        opened.kernel().addEdges(delta, DELTA_EDGES);
        opened.kernel().sync();
        opened.store().abandon();

        RecoveryManager.Recovery recovery = RecoveryManager.recover(directory, WalConfig.DEFAULT);
        try {
            assertEquals(DELTA_EDGES, recovery.framesApplied());
            assertEquals(0L, recovery.truncatedBytes());
            assertEquals(BASE_EDGES + DELTA_EDGES, GraphFixtures.edgeCount(recovery.kernel(), NODES));
            GraphFixtures.assertInDegreesMatchForward(recovery.kernel(), NODES);
            assertEveryEdgePresent(recovery.kernel(), pairs);
        } finally {
            recovery.kernel().close();
        }
    }

    private static void assertEveryEdgePresent(GraphKernel kernel, long[] pairs) {
        for (int i = 0; i < pairs.length / 2; i++) {
            assertTrue(kernel.hasEdge(pairs[2 * i], pairs[2 * i + 1]), "edge " + i + " lost");
        }
    }
}
