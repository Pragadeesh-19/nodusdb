package io.nodusdb.kernel;

import java.util.Arrays;

final class KHopTraversal {

    private static final int INITIAL_CAPACITY = 16;

    private int[] visited = new int[INITIAL_CAPACITY];
    private long[] queue = new long[INITIAL_CAPACITY];
    private int generation;

    void ensureCapacity(int nodeCount) {
        if (nodeCount <= visited.length) {
            return;
        }
        visited = Arrays.copyOf(visited, nodeCount);
        queue = Arrays.copyOf(queue, nodeCount);
    }

    int kHop(AdjacencyTable table, long start, int maxDepth, long[] out) {
        if (maxDepth <= 0 || start >= table.capacity()) {
            return 0;
        }
        int stamp = nextGeneration();
        visited[(int) start] = stamp;
        queue[0] = start;
        int head = 0;
        int tail = 1;
        int count = 0;
        for (int depth = 0; depth < maxDepth && head < tail; depth++) {
            int levelEnd = tail;
            while (head < levelEnd) {
                long node = queue[head++];
                int degree = table.degreeOf(node);
                if (degree == 0) {
                    continue;
                }
                long[] neighbors = table.neighborArray(node);
                int base = table.neighborBase(node);
                for (int i = 0; i < degree; i++) {
                    long neighbor = neighbors[base + i];
                    int slot = (int) neighbor;
                    if (visited[slot] != stamp) {
                        visited[slot] = stamp;
                        if (count == out.length) {
                            throw new OutputBufferTooSmallException("output buffer too small for k-hop result");
                        }
                        out[count++] = neighbor;
                        queue[tail++] = neighbor;
                    }
                }
            }
        }
        return count;
    }

    private int nextGeneration() {
        generation++;
        if (generation == Integer.MAX_VALUE) {
            Arrays.fill(visited, 0);
            generation = 1;
        }
        return generation;
    }
}
