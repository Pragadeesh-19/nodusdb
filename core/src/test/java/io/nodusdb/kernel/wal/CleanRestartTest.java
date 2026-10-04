package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CleanRestartTest {

    private static final int NODES = 60_000;
    private static final int EDGES = 200_000;
    private static final int REMOVALS = 50_000;
    private static final int QUERIES = 200;

    @TempDir
    Path directory;

    @Test
    void reopenedGraphMatchesPreShutdownStateBitForBit() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(42L, EDGES, NODES);
        int[] starts = new int[QUERIES];
        Random random = new Random(7L);
        for (int i = 0; i < QUERIES; i++) {
            starts[i] = random.nextInt(NODES);
        }

        GraphKernel graph = GraphKernel.open(directory, WalConfig.DEFAULT);
        graph.addEdges(pairs, EDGES);
        long[] removals = new long[2 * REMOVALS];
        for (int i = 0; i < REMOVALS; i++) {
            removals[2 * i] = pairs[2 * (3 * i)];
            removals[2 * i + 1] = pairs[2 * (3 * i) + 1];
        }
        assertEquals(REMOVALS, graph.removeEdges(removals, REMOVALS));
        GraphFixtures.GraphState before = GraphFixtures.capture(graph, NODES, starts, 3);
        graph.close();

        GraphKernel reopened = GraphKernel.open(directory, WalConfig.DEFAULT);
        try {
            GraphFixtures.GraphState after = GraphFixtures.capture(reopened, NODES, starts, 3);
            assertEquals(EDGES - REMOVALS, GraphFixtures.edgeCount(reopened, NODES));
            assertEquals(before, after);
            GraphFixtures.assertInDegreesMatchForward(reopened, NODES);
        } finally {
            reopened.close();
        }
    }
}
