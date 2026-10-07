package io.nodusdb.kernel;

import io.nodusdb.kernel.adjacency.AdjacencyTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelBatchTest {

    @Test
    void addEdgesCountsOnlyNewEdges() {
        GraphKernel kernel = new GraphKernel();
        long[] pairs = {0, 1, 0, 2, 0, 1, 3, 4};

        int added = kernel.addEdges(pairs, 4);

        assertEquals(3, added);
        assertTrue(kernel.hasEdge(0L, 1L));
        assertTrue(kernel.hasEdge(3L, 4L));
        assertEquals(2, kernel.getDegree(0L));
    }

    @Test
    void emptyBatchIsANoOp() {
        GraphKernel kernel = new GraphKernel();

        assertEquals(0, kernel.addEdges(new long[0], 0));
        assertEquals(0, kernel.removeEdges(new long[0], 0));
    }

    @Test
    void invalidNodeAnywhereRejectsTheWholeBatch() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(7L, 8L);
        long[] pairs = {1, 2, 3, -4, 5, 6};

        assertThrows(IllegalArgumentException.class, () -> kernel.addEdges(pairs, 3));

        assertFalse(kernel.hasEdge(1L, 2L));
        assertFalse(kernel.hasEdge(5L, 6L));
        assertEquals(1, kernel.getDegree(7L));
    }

    @Test
    void invalidRemoveBatchRemovesNothing() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(1L, 2L);
        long[] pairs = {1, 2, NodeIds.MAX_NODE_ID + 1, 0};

        assertThrows(IllegalArgumentException.class, () -> kernel.removeEdges(pairs, 2));

        assertTrue(kernel.hasEdge(1L, 2L));
    }

    @Test
    void pairCountBeyondBufferIsRejected() {
        GraphKernel kernel = new GraphKernel();

        assertThrows(IllegalArgumentException.class, () -> kernel.addEdges(new long[4], 3));
        assertThrows(IllegalArgumentException.class, () -> kernel.addEdges(new long[4], -1));
    }

    @Test
    void removeEdgesCountsOnlyExistingEdges() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        kernel.addEdge(0L, 2L);

        int removed = kernel.removeEdges(new long[] {0, 1, 0, 1, 0, 2, 5, 5}, 4);

        assertEquals(2, removed);
        assertEquals(0, kernel.getDegree(0L));
        assertEquals(0, kernel.getInDegree(2L));
    }

    @Test
    void batchGrowsNodeCapacityOnce() {
        GraphKernel kernel = new GraphKernel();
        long[] pairs = {0, 9_000, 9_000, 1};

        assertEquals(2, kernel.addEdges(pairs, 2));

        assertTrue(kernel.hasEdge(0L, 9_000L));
        assertTrue(kernel.hasEdge(9_000L, 1L));
    }

    @Test
    void batchCrossesPromotionAndDemotionThresholds() {
        GraphKernel kernel = new GraphKernel();
        int fanOut = 40;
        long[] pairs = new long[2 * fanOut];
        for (int i = 0; i < fanOut; i++) {
            pairs[2 * i] = 0;
            pairs[2 * i + 1] = i + 1;
        }

        assertEquals(fanOut, kernel.addEdges(pairs, fanOut));
        assertTrue(kernel.isHighDegree(0L));

        assertEquals(fanOut - AdjacencyTable.DEMOTION_DEGREE, kernel.removeEdges(pairs, fanOut - AdjacencyTable.DEMOTION_DEGREE));
        assertEquals(AdjacencyTable.DEMOTION_DEGREE, kernel.getDegree(0L));
        assertFalse(kernel.isHighDegree(0L));
    }

    @ParameterizedTest
    @ValueSource(longs = {601L, 602L, 603L, 604L, 605L})
    void batchResultsMatchSequentialSingleEdgeOperations(long seed) {
        Random random = new Random(seed);
        GraphKernel batched = new GraphKernel();
        GraphKernel sequential = new GraphKernel();
        int nodes = 48;

        for (int round = 0; round < 200; round++) {
            int count = random.nextInt(60);
            long[] pairs = new long[2 * count];
            for (int i = 0; i < pairs.length; i++) {
                pairs[i] = random.nextInt(nodes);
            }
            boolean insertRound = random.nextBoolean();

            int expected = 0;
            for (int i = 0; i < count; i++) {
                boolean changed = insertRound
                        ? sequential.addEdge(pairs[2 * i], pairs[2 * i + 1])
                        : sequential.removeEdge(pairs[2 * i], pairs[2 * i + 1]);
                if (changed) {
                    expected++;
                }
            }
            int actual = insertRound ? batched.addEdges(pairs, count) : batched.removeEdges(pairs, count);

            assertEquals(expected, actual, "seed=" + seed + " round=" + round);
            for (long u = 0; u < nodes; u++) {
                assertEquals(sequential.getDegree(u), batched.getDegree(u), "degree u=" + u);
                for (long v = 0; v < nodes; v++) {
                    assertEquals(sequential.hasEdge(u, v), batched.hasEdge(u, v), "u=" + u + " v=" + v);
                }
            }
        }
    }
}
