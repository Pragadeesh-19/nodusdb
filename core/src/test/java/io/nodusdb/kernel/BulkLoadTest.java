package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulkLoadTest {

    private static final int SAMPLE = 60;

    @Test
    void bulkLoadedGraphMatchesTheEdgeByEdgeBuildAndStaysEqualUnderChurn() {
        List<Set<Long>> outgoing = fixtureOutgoing();
        GraphKernel reference = edgeByEdge(outgoing);
        GraphKernel bulk = new GraphKernel();
        load(bulk, outgoing, incomingOf(outgoing));
        assertSameGraph(reference, bulk, outgoing.size());

        Random random = new Random(91L);
        for (int op = 0; op < 4_000; op++) {
            long u = random.nextInt(SAMPLE);
            long v = random.nextInt(SAMPLE);
            if (random.nextBoolean()) {
                assertEquals(reference.addEdge(u, v), bulk.addEdge(u, v), "add " + u + "->" + v + " at op " + op);
            } else {
                assertEquals(reference.removeEdge(u, v), bulk.removeEdge(u, v), "remove " + u + "->" + v + " at op " + op);
            }
        }
        assertSameGraph(reference, bulk, outgoing.size());
    }

    @Test
    void degreeBoundariesLoadWithTheRightTier() {
        List<Set<Long>> outgoing = fixtureOutgoing();
        GraphKernel kernel = new GraphKernel();
        load(kernel, outgoing, incomingOf(outgoing));

        assertEquals(0, kernel.getDegree(5), "empty node");
        assertEquals(1, kernel.getDegree(4), "one edge");
        assertEquals(15, kernel.getDegree(2), "largest low-degree node");
        assertEquals(16, kernel.getDegree(1), "promotion boundary");
        assertEquals(17, kernel.getDegree(3), "first node above the boundary");
        assertEquals(300, kernel.getDegree(0), "hub");
        assertTrue(kernel.isHighDegree(1));
        assertFalse(kernel.isHighDegree(2));
    }

    @Test
    void removingDownToDemotionThenAddingBackKeepsTheGraphCorrect() {
        List<Set<Long>> outgoing = fixtureOutgoing();
        GraphKernel reference = edgeByEdge(outgoing);
        GraphKernel bulk = new GraphKernel();
        load(bulk, outgoing, incomingOf(outgoing));
        for (long v = 2; v <= 9; v++) {
            assertEquals(reference.removeEdge(1, v), bulk.removeEdge(1, v));
        }
        assertEquals(8, bulk.getDegree(1), "node 1 demotes at eight edges");
        assertSameGraph(reference, bulk, outgoing.size());
        assertTrue(bulk.addEdge(1, 200));
        assertTrue(reference.addEdge(1, 200));
        assertSameGraph(reference, bulk, outgoing.size());
    }

    @Test
    void concurrentFillOfDistinctNodesMatchesASequentialFill() throws InterruptedException {
        List<Set<Long>> outgoing = fixtureOutgoing();
        List<Set<Long>> incoming = incomingOf(outgoing);
        GraphKernel reference = new GraphKernel();
        load(reference, outgoing, incoming);

        GraphKernel parallel = new GraphKernel();
        parallel.prepareBulkLoad(degrees(outgoing), degrees(incoming));
        int nodes = outgoing.size();
        int threads = 4;
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            int from = nodes * t / threads;
            int to = nodes * (t + 1) / threads;
            workers[t] = new Thread(() -> fill(parallel, outgoing, incoming, from, to));
            workers[t].start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertSameGraph(reference, parallel, nodes);
    }

    @Test
    void parallelFillOfManyHighDegreeNodesMatchesASequentialFill() throws InterruptedException {
        int nodes = 256;
        Random random = new Random(41L);
        List<Set<Long>> outgoing = new ArrayList<>();
        for (int u = 0; u < nodes; u++) {
            Set<Long> targets = new HashSet<>();
            int degree = 17 + (u * 7) % 40;
            while (targets.size() < degree) {
                targets.add((long) random.nextInt(nodes));
            }
            outgoing.add(targets);
        }
        List<Set<Long>> incoming = incomingOf(outgoing);
        GraphKernel reference = new GraphKernel();
        load(reference, outgoing, incoming);

        GraphKernel parallel = new GraphKernel();
        parallel.prepareBulkLoad(degrees(outgoing), degrees(incoming));
        int threads = 8;
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            int from = nodes * t / threads;
            int to = nodes * (t + 1) / threads;
            workers[t] = new Thread(() -> fill(parallel, outgoing, incoming, from, to));
            workers[t].start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        for (int u = 0; u < nodes; u++) {
            assertEquals(reference.getDegree(u), parallel.getDegree(u), "out-degree of " + u);
            assertEquals(neighbors(reference, u, true), neighbors(parallel, u, true), "out-neighbors of " + u);
            assertEquals(neighbors(reference, u, false), neighbors(parallel, u, false), "in-neighbors of " + u);
        }
    }

    @Test
    void preparingANonEmptyGraphIsRejected() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0, 1);
        int[] degrees = new int[4];

        assertThrows(IllegalStateException.class, () -> kernel.prepareBulkLoad(degrees, degrees));
    }

    @Test
    void degreeLargerThanTheNeighborArrayIsRejected() {
        GraphKernel kernel = new GraphKernel();
        kernel.prepareBulkLoad(new int[4], new int[4]);

        assertThrows(IllegalArgumentException.class, () -> kernel.loadBulkNode(true, 0, new long[] {1}, 2));
    }

    private static List<Set<Long>> fixtureOutgoing() {
        List<Set<Long>> outgoing = new ArrayList<>();
        for (int i = 0; i < 301; i++) {
            outgoing.add(new HashSet<>());
        }
        for (long v = 1; v <= 300; v++) {
            outgoing.get(0).add(v);
        }
        for (long v = 2; v <= 17; v++) {
            outgoing.get(1).add(v);
        }
        for (long v = 3; v <= 17; v++) {
            outgoing.get(2).add(v);
        }
        for (long v = 4; v <= 20; v++) {
            outgoing.get(3).add(v);
        }
        outgoing.get(4).add(1L);
        Random random = new Random(7L);
        for (int i = 0; i < 400; i++) {
            long u = 6 + random.nextInt(SAMPLE - 6);
            long v = random.nextInt(SAMPLE);
            outgoing.get((int) u).add(v);
        }
        return outgoing;
    }

    private static List<Set<Long>> incomingOf(List<Set<Long>> outgoing) {
        List<Set<Long>> incoming = new ArrayList<>();
        for (int i = 0; i < outgoing.size(); i++) {
            incoming.add(new HashSet<>());
        }
        for (int u = 0; u < outgoing.size(); u++) {
            for (long v : outgoing.get(u)) {
                incoming.get((int) v).add((long) u);
            }
        }
        return incoming;
    }

    private static GraphKernel edgeByEdge(List<Set<Long>> outgoing) {
        GraphKernel kernel = new GraphKernel();
        for (int u = 0; u < outgoing.size(); u++) {
            for (long v : outgoing.get(u)) {
                kernel.addEdge(u, v);
            }
        }
        return kernel;
    }

    private static void load(GraphKernel kernel, List<Set<Long>> outgoing, List<Set<Long>> incoming) {
        kernel.prepareBulkLoad(degrees(outgoing), degrees(incoming));
        fill(kernel, outgoing, incoming, 0, outgoing.size());
    }

    private static void fill(GraphKernel kernel, List<Set<Long>> outgoing, List<Set<Long>> incoming, int from, int to) {
        for (int node = from; node < to; node++) {
            fillSet(kernel, true, node, outgoing.get(node));
            fillSet(kernel, false, node, incoming.get(node));
        }
    }

    private static void fillSet(GraphKernel kernel, boolean forward, int node, Set<Long> values) {
        if (values.isEmpty()) {
            return;
        }
        long[] array = values.stream().mapToLong(Long::longValue).toArray();
        kernel.loadBulkNode(forward, node, array, array.length);
    }

    private static int[] degrees(List<Set<Long>> sets) {
        int[] degrees = new int[sets.size()];
        for (int i = 0; i < degrees.length; i++) {
            degrees[i] = sets.get(i).size();
        }
        return degrees;
    }

    private static void assertSameGraph(GraphKernel reference, GraphKernel bulk, int nodes) {
        for (int u = 0; u < nodes; u++) {
            assertEquals(reference.getDegree(u), bulk.getDegree(u), "out-degree of " + u);
            assertEquals(reference.getInDegree(u), bulk.getInDegree(u), "in-degree of " + u);
            assertEquals(neighbors(reference, u, true), neighbors(bulk, u, true), "out-neighbors of " + u);
            assertEquals(neighbors(reference, u, false), neighbors(bulk, u, false), "in-neighbors of " + u);
        }
        for (int u = 0; u < SAMPLE; u++) {
            for (int v = 0; v < SAMPLE; v++) {
                assertEquals(reference.hasEdge(u, v), bulk.hasEdge(u, v), "edge " + u + "->" + v);
            }
        }
    }

    private static Set<Long> neighbors(GraphKernel kernel, int node, boolean forward) {
        Set<Long> found = new HashSet<>();
        int degree = forward ? kernel.getDegree(node) : kernel.getInDegree(node);
        for (int i = 0; i < degree; i++) {
            found.add(forward ? kernel.outgoingNeighbor(node, i) : kernel.incomingNeighbor(node, i));
        }
        return found;
    }
}
