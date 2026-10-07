package io.nodusdb.kernel;

import io.nodusdb.kernel.memory.MemoryLimitExceededException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelMemoryLimitTest {

    private static final int NODES = 200;
    private static final int HUBS = 8;
    private static final long FILL_BOUND = 5_000_000L;

    @Test
    void anUnlimitedKernelReportsNoLimitAndCountsItsStorage() {
        GraphKernel kernel = new GraphKernel();

        assertEquals(GraphKernel.NO_MEMORY_LIMIT, kernel.memoryLimitBytes());
        assertTrue(kernel.memoryUsedBytes() > 0);
    }

    @Test
    void aLimitBelowTheInitialStorageIsRefusedAtConstruction() {
        assertThrows(MemoryLimitExceededException.class, () -> new GraphKernel(1));
    }

    @Test
    void aNegativeLimitIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new GraphKernel(-1));
    }

    @Test
    void theLimitIsReportedBack() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 4_096);

        assertEquals(initialFootprint() + 4_096, kernel.memoryLimitBytes());
    }

    @Test
    void writesPastTheLimitAreRejectedAndEarlierDataStaysQueryable() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 20_000);

        long rejected = fillChainUntilRejected(kernel);

        assertFalse(kernel.hasEdge(rejected, rejected + 1));
        assertEquals(0, kernel.getDegree(rejected));
        assertEquals(0, kernel.getInDegree(rejected + 1));
        for (long node = 0; node < rejected; node++) {
            assertTrue(kernel.hasEdge(node, node + 1), "edge " + node);
            assertTrue(kernel.hasIncoming(node + 1, node), "mirror of edge " + node);
        }
    }

    @Test
    void aRejectedWriteDoesNotPoisonLaterWritesThatNeedNoNewStorage() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 20_000);
        long rejected = fillChainUntilRejected(kernel);
        assertTrue(rejected > 10);

        assertTrue(kernel.addEdge(0, 5));

        assertTrue(kernel.hasEdge(0, 5));
        assertEquals(2, kernel.getDegree(0));
        assertEquals(2, kernel.getInDegree(5));
    }

    @Test
    void anOversizedNodeIdIsRejectedWithoutSpendingTheBudget() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 100_000);
        kernel.addEdge(1, 2);
        long used = kernel.memoryUsedBytes();
        int capacity = kernel.nodeCapacity();

        assertThrows(MemoryLimitExceededException.class, () -> kernel.addEdge(0, 2_000_000_000L));

        assertEquals(used, kernel.memoryUsedBytes());
        assertEquals(capacity, kernel.nodeCapacity());
        assertTrue(kernel.addEdge(3, 4));
        assertTrue(kernel.hasEdge(1, 2));
        assertEquals(0, kernel.getInDegree(2_000_000_000L));
    }

    @Test
    void duplicateAddsAtTheLimitStillReturnFalseInsteadOfThrowing() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 20_000);
        long rejected = fillChainUntilRejected(kernel);

        assertFalse(kernel.addEdge(0, 1));
        assertFalse(kernel.addEdge(rejected - 1, rejected));
    }

    @Test
    void deletesAlwaysSucceedAtTheLimitAndAFreedEdgeCanBeAddedBack() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 60_000);
        Set<Long> edges = new HashSet<>();
        boolean limitReached = false;
        for (long next = 0; next < FILL_BOUND && !limitReached; next++) {
            long source = next / 40;
            long target = 1_000 + next % 40;
            try {
                if (kernel.addEdge(source, target)) {
                    edges.add(source * 10_000 + target);
                }
            } catch (MemoryLimitExceededException expected) {
                limitReached = true;
            }
        }
        assertTrue(limitReached, "the limit was never reached");

        for (long encoded : edges) {
            long source = encoded / 10_000;
            long target = encoded % 10_000;
            assertTrue(kernel.removeEdge(source, target));
            assertTrue(kernel.addEdge(source, target));
            assertTrue(kernel.removeEdge(source, target));
        }
        for (long encoded : edges) {
            assertFalse(kernel.hasEdge(encoded / 10_000, encoded % 10_000));
        }
    }

    @Test
    void aHighDegreeNodeCanBeEmptiedAndRefilledAtTheLimit() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 40_000);
        for (long target = 1; target <= 40; target++) {
            kernel.addEdge(0, target);
        }
        fillChainUntilRejected(kernel, 100);

        for (long target = 40; target >= 1; target--) {
            assertTrue(kernel.removeEdge(0, target));
            assertEquals((int) (target - 1), kernel.getDegree(0));
        }
        for (long target = 1; target <= 40; target++) {
            assertFalse(kernel.hasEdge(0, target));
            assertTrue(kernel.addEdge(0, target));
        }
        assertEquals(40, kernel.getDegree(0));
    }

    @Test
    void aBatchStopsAtTheFirstEdgeThatDoesNotFitAndKeepsEarlierOnesConsistent() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 80_000);
        int pairs = 100_000;
        long[] batch = new long[2 * pairs];
        for (int i = 0; i < pairs; i++) {
            batch[2 * i] = batchSource(i);
            batch[2 * i + 1] = batchTarget(i);
        }

        assertThrows(MemoryLimitExceededException.class, () -> kernel.addEdges(batch, pairs));

        int applied = 0;
        while (applied < pairs && kernel.hasEdge(batchSource(applied), batchTarget(applied))) {
            applied++;
        }
        assertTrue(applied > 0 && applied < pairs, "applied " + applied + " of " + pairs);
        for (int i = 0; i < applied; i++) {
            assertTrue(kernel.hasIncoming(batchTarget(i), batchSource(i)), "mirror of edge " + i);
        }
        for (int i = applied; i < pairs; i++) {
            assertFalse(kernel.hasEdge(batchSource(i), batchTarget(i)), "edge " + i + " applied after the stop");
        }
    }

    @Test
    void aBatchWhoseNodeIdsDoNotFitIsRefusedBeforeAnyEdgeIsApplied() {
        GraphKernel kernel = new GraphKernel(initialFootprint() + 20_000);
        long[] batch = {1, 2, 3, 4, 5, 3_000_000L};

        assertThrows(MemoryLimitExceededException.class, () -> kernel.addEdges(batch, 3));

        assertFalse(kernel.hasEdge(1, 2));
        assertFalse(kernel.hasEdge(3, 4));
        assertFalse(kernel.hasEdge(5, 3_000_000L));
    }

    private static long batchSource(int i) {
        return i % 500;
    }

    private static long batchTarget(int i) {
        return 500 + (i / 500) % 500;
    }

    @ParameterizedTest
    @ValueSource(longs = {12_000, 30_000, 90_000, 300_000})
    void randomChurnUnderATightLimitMatchesAnOracleAfterEveryRejection(long headroom) {
        for (long seed = 1; seed <= 4; seed++) {
            runChurn(initialFootprint() + headroom, seed);
        }
    }

    private static void runChurn(long limit, long seed) {
        GraphKernel kernel = new GraphKernel(limit);
        Map<Long, Set<Long>> out = new HashMap<>();
        Map<Long, Set<Long>> in = new HashMap<>();
        Random random = new Random(seed);
        int rejected = 0;
        int accepted = 0;
        for (int op = 0; op < 20_000; op++) {
            long u = random.nextInt(100) < 70 ? random.nextInt(HUBS) : random.nextInt(NODES);
            long v = random.nextInt(NODES);
            String where = "limit=" + limit + " seed=" + seed + " op=" + op;
            if (random.nextInt(100) < 65) {
                boolean expectedNew = !out.getOrDefault(u, Set.of()).contains(v);
                try {
                    assertEquals(expectedNew, kernel.addEdge(u, v), where);
                    if (expectedNew) {
                        out.computeIfAbsent(u, key -> new HashSet<>()).add(v);
                        in.computeIfAbsent(v, key -> new HashSet<>()).add(u);
                        accepted++;
                    }
                } catch (MemoryLimitExceededException rejection) {
                    rejected++;
                    assertTrue(expectedNew, "a duplicate add must not be rejected: " + where);
                    assertNode(kernel, out, in, u, where);
                    assertNode(kernel, out, in, v, where);
                    if (rejected <= 40 || rejected % 50 == 0) {
                        assertEveryNode(kernel, out, in, where);
                    }
                }
            } else {
                boolean expectedPresent = out.getOrDefault(u, Set.of()).contains(v);
                assertEquals(expectedPresent, kernel.removeEdge(u, v), where);
                if (expectedPresent) {
                    out.get(u).remove(v);
                    in.get(v).remove(u);
                }
            }
            if (op % 500 == 0) {
                assertEveryNode(kernel, out, in, where);
            }
        }
        assertTrue(rejected > 0, "the limit was never reached: limit=" + limit + " seed=" + seed);
        assertTrue(accepted > 0, "nothing was accepted: limit=" + limit + " seed=" + seed);
        assertEveryNode(kernel, out, in, "final limit=" + limit + " seed=" + seed);
    }

    private static void assertEveryNode(GraphKernel kernel, Map<Long, Set<Long>> out, Map<Long, Set<Long>> in,
                                        String where) {
        for (long node = 0; node < NODES; node++) {
            assertNode(kernel, out, in, node, where);
        }
    }

    private static void assertNode(GraphKernel kernel, Map<Long, Set<Long>> out, Map<Long, Set<Long>> in,
                                   long node, String where) {
        Set<Long> expectedOut = out.getOrDefault(node, Set.of());
        Set<Long> expectedIn = in.getOrDefault(node, Set.of());
        assertEquals(expectedOut.size(), kernel.getDegree(node), "out-degree of " + node + " " + where);
        assertEquals(expectedIn.size(), kernel.getInDegree(node), "in-degree of " + node + " " + where);
        Set<Long> actualOut = new HashSet<>();
        for (int i = 0; i < expectedOut.size(); i++) {
            actualOut.add(kernel.outgoingNeighbor(node, i));
        }
        Set<Long> actualIn = new HashSet<>();
        for (int i = 0; i < expectedIn.size(); i++) {
            actualIn.add(kernel.incomingNeighbor(node, i));
        }
        assertEquals(expectedOut, actualOut, "out-neighbors of " + node + " " + where);
        assertEquals(expectedIn, actualIn, "in-neighbors of " + node + " " + where);
    }

    private static long fillChainUntilRejected(GraphKernel kernel) {
        return fillChainUntilRejected(kernel, 0);
    }

    private static long fillChainUntilRejected(GraphKernel kernel, long first) {
        for (long node = first; node < first + FILL_BOUND; node++) {
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
