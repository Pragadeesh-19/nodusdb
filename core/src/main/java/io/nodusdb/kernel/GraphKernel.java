package io.nodusdb.kernel;

public final class GraphKernel {

    private final AdjacencyTable outgoing = new AdjacencyTable();
    private final AdjacencyTable incoming = new AdjacencyTable();
    private final KHopTraversal traversal = new KHopTraversal();

    public boolean addEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        ensureCapacity(Math.max(u, v) + 1);
        return insert(u, v);
    }

    public boolean removeEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        return delete(u, v);
    }

    public int addEdges(long[] pairs, int pairCount) {
        long largest = validatePairs(pairs, pairCount);
        ensureCapacity(largest + 1);
        int added = 0;
        for (int i = 0; i < pairCount; i++) {
            if (insert(pairs[2 * i], pairs[2 * i + 1])) {
                added++;
            }
        }
        return added;
    }

    public int removeEdges(long[] pairs, int pairCount) {
        validatePairs(pairs, pairCount);
        int removed = 0;
        for (int i = 0; i < pairCount; i++) {
            if (delete(pairs[2 * i], pairs[2 * i + 1])) {
                removed++;
            }
        }
        return removed;
    }

    public boolean hasEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        return outgoing.contains(u, v);
    }

    public int getDegree(long u) {
        NodeIds.checkValid(u);
        return outgoing.degreeOf(u);
    }

    public int getInDegree(long v) {
        NodeIds.checkValid(v);
        return incoming.degreeOf(v);
    }

    public int commonNeighbors(long u, long v, long[] out) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        boolean uIsSmaller = outgoing.degreeOf(u) <= outgoing.degreeOf(v);
        long smaller = uIsSmaller ? u : v;
        long larger = uIsSmaller ? v : u;
        int smallerDegree = outgoing.degreeOf(smaller);
        if (out.length < smallerDegree) {
            throw new OutputBufferTooSmallException(
                    "output buffer too small: " + out.length + " < " + smallerDegree);
        }
        int count = 0;
        for (int i = 0; i < smallerDegree; i++) {
            long candidate = outgoing.neighborAt(smaller, i);
            if (outgoing.contains(larger, candidate)) {
                out[count++] = candidate;
            }
        }
        return count;
    }

    public int kHop(long start, int maxDepth, long[] out) {
        NodeIds.checkValid(start);
        return traversal.kHop(outgoing, start, maxDepth, out);
    }

    boolean isHighDegree(long u) {
        return outgoing.isHighDegree(u);
    }

    boolean hasIncoming(long v, long u) {
        return incoming.contains(v, u);
    }

    private boolean insert(long u, long v) {
        boolean added = outgoing.add(u, v);
        if (added) {
            boolean mirrored = incoming.add(v, u);
            assert mirrored : "forward and backward tables diverged on add";
        }
        return added;
    }

    private boolean delete(long u, long v) {
        boolean removed = outgoing.remove(u, v);
        if (removed) {
            boolean mirrored = incoming.remove(v, u);
            assert mirrored : "forward and backward tables diverged on remove";
        }
        return removed;
    }

    private static long validatePairs(long[] pairs, int pairCount) {
        if (pairCount < 0 || 2L * pairCount > pairs.length) {
            throw new IllegalArgumentException(
                    "pair count " + pairCount + " does not fit a buffer of " + pairs.length + " longs");
        }
        long largest = 0;
        for (int i = 0; i < 2 * pairCount; i++) {
            NodeIds.checkValid(pairs[i]);
            largest = Math.max(largest, pairs[i]);
        }
        return largest;
    }

    private void ensureCapacity(long requiredNodes) {
        if (requiredNodes <= outgoing.capacity()) {
            return;
        }
        int nodes = (int) requiredNodes;
        outgoing.ensureCapacity(nodes);
        incoming.ensureCapacity(nodes);
        traversal.ensureCapacity(outgoing.capacity());
    }
}
