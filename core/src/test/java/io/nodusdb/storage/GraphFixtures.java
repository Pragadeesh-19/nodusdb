package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

final class GraphFixtures {

    static final int HUB_COUNT = 200;
    static final double HUB_SHARE = 0.4;

    private GraphFixtures() {
    }

    static long[] skewedPairs(long seed, int count, int nodes) {
        Random random = new Random(seed);
        Set<Long> seen = new HashSet<>(count * 2);
        long[] pairs = new long[2 * count];
        int produced = 0;
        while (produced < count) {
            long u = random.nextDouble() < HUB_SHARE
                    ? random.nextInt(HUB_COUNT)
                    : random.nextInt(nodes);
            long v = random.nextInt(nodes);
            if (u == v || !seen.add(u * nodes + v)) {
                continue;
            }
            pairs[2 * produced] = u;
            pairs[2 * produced + 1] = v;
            produced++;
        }
        return pairs;
    }

    static int edgeCount(GraphKernel kernel, int nodes) {
        int total = 0;
        for (int node = 0; node < nodes; node++) {
            total += kernel.getDegree(node);
        }
        return total;
    }

    static void assertInDegreesMatchForward(GraphKernel kernel, int nodes) {
        int[] inDegree = new int[nodes];
        for (int node = 0; node < nodes; node++) {
            for (int i = 0; i < kernel.getDegree(node); i++) {
                inDegree[(int) kernel.outgoingNeighbor(node, i)]++;
            }
        }
        for (int node = 0; node < nodes; node++) {
            if (kernel.getInDegree(node) != inDegree[node]) {
                throw new AssertionError("in-degree of " + node + " is " + kernel.getInDegree(node)
                        + " but forward edges give " + inDegree[node]);
            }
        }
    }

    static GraphState capture(GraphKernel kernel, int nodes, int[] starts, int depth) {
        int[] degrees = new int[nodes];
        int[] inDegrees = new int[nodes];
        for (int node = 0; node < nodes; node++) {
            degrees[node] = kernel.getDegree(node);
            inDegrees[node] = kernel.getInDegree(node);
        }
        long[][] reach = new long[starts.length][];
        long[] buffer = new long[nodes];
        for (int i = 0; i < starts.length; i++) {
            int count = kernel.kHop(starts[i], depth, buffer);
            long[] found = Arrays.copyOf(buffer, count);
            Arrays.sort(found);
            reach[i] = found;
        }
        return new GraphState(degrees, inDegrees, reach);
    }

    record GraphState(int[] degrees, int[] inDegrees, long[][] reach) {

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof GraphState state)) {
                return false;
            }
            return Arrays.equals(degrees, state.degrees)
                    && Arrays.equals(inDegrees, state.inDegrees)
                    && Arrays.deepEquals(reach, state.reach);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(degrees) * 31 + Arrays.hashCode(inDegrees);
        }
    }
}
