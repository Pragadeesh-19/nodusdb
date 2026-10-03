package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelTest {

    private static final int PROMOTION_DEGREE = AdjacencyTable.MAX_LOW_DEGREE + 1;

    @Test
    void emptyKernelHasNoEdges() {
        GraphKernel kernel = new GraphKernel();

        assertFalse(kernel.hasEdge(0L, 1L));
        assertEquals(0, kernel.getDegree(0L));
        assertEquals(0, kernel.getInDegree(0L));
        assertEquals(0, kernel.kHop(0L, 5, new long[4]));
    }

    @Test
    void triangleFollowsDirectedCycle() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        kernel.addEdge(1L, 2L);
        kernel.addEdge(2L, 0L);
        long[] out = new long[8];

        assertEquals(1, kernel.kHop(0L, 1, out));
        assertEquals(1L, out[0]);
        assertEquals(2, kernel.kHop(0L, 2, out));
        assertEquals(1, kernel.getDegree(0L));
        assertEquals(1, kernel.getInDegree(0L));
        assertEquals(0, kernel.commonNeighbors(0L, 1L, out));
    }

    @Test
    void cliqueNodesAreHighDegreeAndShareAllOtherMembers() {
        int size = 20;
        GraphKernel kernel = new GraphKernel();
        for (long u = 0; u < size; u++) {
            for (long v = 0; v < size; v++) {
                if (u != v) {
                    kernel.addEdge(u, v);
                }
            }
        }
        long[] out = new long[size];

        assertEquals(size - 1, kernel.getDegree(0L));
        assertTrue(kernel.isHighDegree(0L));
        assertEquals(size - 2, kernel.commonNeighbors(0L, 1L, out));
        assertEquals(size - 1, kernel.kHop(0L, 1, out));
    }

    @Test
    void pathOnlyReachesNodesAheadWithinDepth() {
        GraphKernel kernel = new GraphKernel();
        for (long u = 0; u < 9; u++) {
            kernel.addEdge(u, u + 1);
        }
        long[] out = new long[10];

        assertEquals(3, kernel.kHop(0L, 3, out));
        assertEquals(Set.of(1L, 2L, 3L), Set.of(out[0], out[1], out[2]));
        assertEquals(0, kernel.kHop(9L, 3, out));
    }

    @Test
    void selfLoopIsItsOwnNeighborAndNotReportedByKHop() {
        GraphKernel kernel = new GraphKernel();

        assertTrue(kernel.addEdge(5L, 5L));

        assertTrue(kernel.hasEdge(5L, 5L));
        assertEquals(1, kernel.getDegree(5L));
        assertEquals(1, kernel.getInDegree(5L));
        assertEquals(0, kernel.kHop(5L, 3, new long[2]));
        assertEquals(1, kernel.commonNeighbors(5L, 5L, new long[2]));
    }

    @Test
    void duplicateAddAndAbsentRemoveReturnFalse() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(1L, 2L);

        assertFalse(kernel.addEdge(1L, 2L));
        assertFalse(kernel.removeEdge(2L, 1L));
        assertFalse(kernel.removeEdge(9L, 9L));
        assertEquals(1, kernel.getDegree(1L));
        assertEquals(1, kernel.getInDegree(2L));
    }

    @Test
    void edgesAreDirected() {
        GraphKernel kernel = new GraphKernel();

        kernel.addEdge(1L, 2L);

        assertTrue(kernel.hasEdge(1L, 2L));
        assertFalse(kernel.hasEdge(2L, 1L));
        assertEquals(0, kernel.getDegree(2L));
        assertEquals(1, kernel.getInDegree(2L));
        assertEquals(0, kernel.kHop(2L, 2, new long[2]));
    }

    @Test
    void forwardAndBackwardTablesStayMirroredAcrossMutations() {
        GraphKernel kernel = new GraphKernel();
        long[][] edges = {{0, 1}, {0, 2}, {3, 0}, {2, 3}, {1, 3}, {3, 3}};
        for (long[] edge : edges) {
            kernel.addEdge(edge[0], edge[1]);
        }
        kernel.removeEdge(0L, 2L);
        kernel.removeEdge(3L, 3L);

        for (long u = 0; u < 4; u++) {
            for (long v = 0; v < 4; v++) {
                assertEquals(kernel.hasEdge(u, v), kernel.hasIncoming(v, u), "u=" + u + " v=" + v);
            }
        }
    }

    @Test
    void promotionHappensOnTheSixteenthEdge() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 1; v <= AdjacencyTable.MAX_LOW_DEGREE; v++) {
            kernel.addEdge(0L, v);
        }

        assertEquals(AdjacencyTable.MAX_LOW_DEGREE, kernel.getDegree(0L));
        assertFalse(kernel.isHighDegree(0L));

        kernel.addEdge(0L, PROMOTION_DEGREE);

        assertEquals(PROMOTION_DEGREE, kernel.getDegree(0L));
        assertTrue(kernel.isHighDegree(0L));
    }

    @Test
    void highNodeStaysHighAboveDemotionThreshold() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 1; v <= PROMOTION_DEGREE; v++) {
            kernel.addEdge(0L, v);
        }

        assertTrue(kernel.removeEdge(0L, PROMOTION_DEGREE));

        assertEquals(AdjacencyTable.MAX_LOW_DEGREE, kernel.getDegree(0L));
        assertTrue(kernel.isHighDegree(0L));
    }

    @Test
    void demotionHappensWhenDegreeReachesEight() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 1; v <= PROMOTION_DEGREE; v++) {
            kernel.addEdge(0L, v);
        }

        for (long v = PROMOTION_DEGREE; v > AdjacencyTable.DEMOTION_DEGREE; v--) {
            assertTrue(kernel.removeEdge(0L, v));
        }

        assertEquals(AdjacencyTable.DEMOTION_DEGREE, kernel.getDegree(0L));
        assertFalse(kernel.isHighDegree(0L));
    }

    @Test
    void contentsSurviveDemotionAtEight() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 1; v <= PROMOTION_DEGREE; v++) {
            kernel.addEdge(0L, v);
        }

        kernel.removeEdge(0L, 4L);
        for (long v = PROMOTION_DEGREE; v > AdjacencyTable.DEMOTION_DEGREE + 1; v--) {
            kernel.removeEdge(0L, v);
        }

        assertFalse(kernel.isHighDegree(0L));
        for (long v = 1; v <= PROMOTION_DEGREE; v++) {
            boolean expected = v != 4L && v <= AdjacencyTable.DEMOTION_DEGREE + 1;
            assertEquals(expected, kernel.hasEdge(0L, v), "after demotion v=" + v);
        }
    }

    @Test
    void reinsertingAfterDemotionPromotesAgainAtSixteen() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 1; v <= PROMOTION_DEGREE; v++) {
            kernel.addEdge(0L, v);
        }
        for (long v = PROMOTION_DEGREE; v > AdjacencyTable.DEMOTION_DEGREE; v--) {
            kernel.removeEdge(0L, v);
        }

        for (long v = AdjacencyTable.DEMOTION_DEGREE + 1; v <= AdjacencyTable.MAX_LOW_DEGREE; v++) {
            assertTrue(kernel.addEdge(0L, v));
        }
        assertFalse(kernel.isHighDegree(0L));

        assertTrue(kernel.addEdge(0L, PROMOTION_DEGREE));

        assertTrue(kernel.isHighDegree(0L));
        assertEquals(PROMOTION_DEGREE, kernel.getDegree(0L));
    }

    @Test
    void growthPastInitialNodeCountKeepsExistingEdges() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        kernel.addEdge(1L, 0L);

        kernel.addEdge(0L, 5_000L);

        assertTrue(kernel.hasEdge(0L, 1L));
        assertTrue(kernel.hasEdge(1L, 0L));
        assertTrue(kernel.hasEdge(0L, 5_000L));
        assertEquals(1, kernel.getInDegree(5_000L));
        assertEquals(0, kernel.getDegree(4_999L));
        assertFalse(kernel.hasEdge(4_999L, 0L));
    }

    @Test
    void rejectsInvalidNodeIds() {
        GraphKernel kernel = new GraphKernel();

        assertThrows(IllegalArgumentException.class, () -> kernel.addEdge(-1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> kernel.addEdge(0L, NodeIds.MAX_NODE_ID + 1));
        assertThrows(IllegalArgumentException.class, () -> kernel.hasEdge(-1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> kernel.kHop(-1L, 1, new long[1]));
    }
}
