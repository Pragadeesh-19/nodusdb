package io.nodusdb.kernel.traversal;

import io.nodusdb.kernel.adjacency.AdjacencyTable;
import io.nodusdb.kernel.adjacency.EdgeKey;

import java.util.Arrays;

public final class KHopTraversal {

    public static final int OVERFLOW = -1;

    private static final int INITIAL_CAPACITY = 16;

    private int[] visited = new int[INITIAL_CAPACITY];
    private long[] queue = new long[INITIAL_CAPACITY];
    private long[] scratch = new long[AdjacencyTable.MAX_LOW_DEGREE];
    private int generation;

    public void ensureCapacity(int nodeCount) {
        if (nodeCount <= visited.length) {
            return;
        }
        visited = Arrays.copyOf(visited, nodeCount);
        queue = Arrays.copyOf(queue, nodeCount);
    }

    public int kHop(AdjacencyTable table, long start, int maxDepth, long[] out) {
        int limit = Math.min(table.capacity(), visited.length);
        if (maxDepth <= 0 || start >= limit) {
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
                if (scratch.length < degree) {
                    scratch = new long[degree];
                }
                long[] neighbors = table.neighborsOf(node, scratch);
                if (degree > neighbors.length) {
                    continue;
                }
                for (int i = 0; i < degree; i++) {
                    long neighbor = EdgeKey.id(neighbors[i]);
                    if (neighbor < 0 || neighbor >= limit) {
                        continue;
                    }
                    int slot = (int) neighbor;
                    if (visited[slot] != stamp) {
                        visited[slot] = stamp;
                        if (count == out.length) {
                            return OVERFLOW;
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
