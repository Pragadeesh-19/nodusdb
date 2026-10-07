package io.nodusdb.kernel;

import io.nodusdb.kernel.adjacency.AdjacencyTable;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelOracleTest {

    private static final int OPERATIONS = 100_000;
    private static final int NODES = 64;
    private static final int SWEEP_INTERVAL = 1_000;
    private static final int MAX_DEPTH = 3;

    @ParameterizedTest
    @ValueSource(longs = {401L, 402L, 403L, 404L, 405L, 406L, 407L, 408L, 409L, 410L})
    void denseGraphAgreesWithReferenceModel(long seed) {
        runScenario(seed, 45, 25);
    }

    @ParameterizedTest
    @ValueSource(longs = {501L, 502L, 503L, 504L, 505L, 506L, 507L, 508L, 509L, 510L})
    void sparseGraphCrossesPromotionBoundaryAgainstReferenceModel(long seed) {
        runScenario(seed, 4, 20);
    }

    private static void runScenario(long seed, int addPercent, int removePercent) {
        Random random = new Random(seed);
        GraphKernel kernel = new GraphKernel();
        ReferenceGraph reference = new ReferenceGraph();
        long[] buffer = new long[NODES];

        for (int op = 0; op < OPERATIONS; op++) {
            long u = random.nextInt(NODES);
            long v = random.nextInt(NODES);
            int roll = random.nextInt(100);
            String where = "seed=" + seed + " op=" + op + " u=" + u + " v=" + v;

            if (roll < addPercent) {
                assertEquals(reference.addEdge(u, v), kernel.addEdge(u, v), where);
            } else if (roll < addPercent + removePercent) {
                assertEquals(reference.removeEdge(u, v), kernel.removeEdge(u, v), where);
            } else if (roll < 90) {
                assertEquals(reference.hasEdge(u, v), kernel.hasEdge(u, v), where);
                assertEquals(reference.degree(u), kernel.getDegree(u), where);
            } else if (roll < 95) {
                int count = kernel.commonNeighbors(u, v, buffer);
                assertEquals(reference.commonNeighbors(u, v), toSet(buffer, count), where);
            } else {
                int depth = random.nextInt(MAX_DEPTH + 1);
                int count = kernel.kHop(u, depth, buffer);
                assertEquals(reference.kHop(u, depth), toSet(buffer, count), where + " depth=" + depth);
            }

            if (op % SWEEP_INTERVAL == 0) {
                verifyAll(kernel, reference, "seed=" + seed + " op=" + op);
            }
        }
        verifyAll(kernel, reference, "seed=" + seed + " final");
    }

    private static void verifyAll(GraphKernel kernel, ReferenceGraph reference, String where) {
        for (long source = 0; source < NODES; source++) {
            long u = source;
            int degree = reference.degree(u);
            assertEquals(degree, kernel.getDegree(u), () -> where + " degree u=" + u);
            assertEquals(reference.inDegree(u), kernel.getInDegree(u), () -> where + " inDegree u=" + u);
            boolean high = kernel.isHighDegree(u);
            assertTrue(degree < AdjacencyTable.PROMOTED_CAPACITY || high,
                    () -> where + " degree " + degree + " must be high u=" + u);
            assertTrue(degree > AdjacencyTable.DEMOTION_DEGREE || !high,
                    () -> where + " degree " + degree + " must be low u=" + u);
            for (long target = 0; target < NODES; target++) {
                long v = target;
                boolean expected = reference.hasEdge(u, v);
                assertEquals(expected, kernel.hasEdge(u, v), () -> where + " hasEdge u=" + u + " v=" + v);
                assertEquals(expected, kernel.hasIncoming(v, u), () -> where + " mirror u=" + u + " v=" + v);
            }
        }
    }

    private static Set<Long> toSet(long[] values, int count) {
        Set<Long> result = new HashSet<>();
        for (int i = 0; i < count; i++) {
            result.add(values[i]);
        }
        assertEquals(count, result.size(), "duplicates in kernel output");
        return result;
    }

    private static final class ReferenceGraph {

        private final Map<Long, Set<Long>> outgoing = new HashMap<>();
        private final Map<Long, Set<Long>> incoming = new HashMap<>();

        boolean addEdge(long u, long v) {
            boolean added = outgoing.computeIfAbsent(u, k -> new HashSet<>()).add(v);
            if (added) {
                incoming.computeIfAbsent(v, k -> new HashSet<>()).add(u);
            }
            return added;
        }

        boolean removeEdge(long u, long v) {
            Set<Long> targets = outgoing.get(u);
            if (targets == null || !targets.remove(v)) {
                return false;
            }
            if (targets.isEmpty()) {
                outgoing.remove(u);
            }
            Set<Long> sources = incoming.get(v);
            sources.remove(u);
            if (sources.isEmpty()) {
                incoming.remove(v);
            }
            return true;
        }

        boolean hasEdge(long u, long v) {
            Set<Long> targets = outgoing.get(u);
            return targets != null && targets.contains(v);
        }

        int degree(long u) {
            Set<Long> targets = outgoing.get(u);
            return targets == null ? 0 : targets.size();
        }

        int inDegree(long v) {
            Set<Long> sources = incoming.get(v);
            return sources == null ? 0 : sources.size();
        }

        Set<Long> commonNeighbors(long u, long v) {
            Set<Long> result = new HashSet<>(outgoing.getOrDefault(u, Set.of()));
            result.retainAll(outgoing.getOrDefault(v, Set.of()));
            return result;
        }

        Set<Long> kHop(long start, int depth) {
            Set<Long> seen = new HashSet<>();
            seen.add(start);
            Set<Long> result = new HashSet<>();
            Set<Long> frontier = new HashSet<>();
            frontier.add(start);
            for (int d = 0; d < depth && !frontier.isEmpty(); d++) {
                Set<Long> next = new HashSet<>();
                for (long node : frontier) {
                    for (long neighbor : outgoing.getOrDefault(node, Set.of())) {
                        if (seen.add(neighbor)) {
                            result.add(neighbor);
                            next.add(neighbor);
                        }
                    }
                }
                frontier = next;
            }
            return result;
        }
    }
}
