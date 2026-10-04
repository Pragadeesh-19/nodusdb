package io.nodusdb.kernel;

import io.nodusdb.kernel.wal.RecoveryManager;
import io.nodusdb.kernel.wal.WalConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public final class GraphKernel implements AutoCloseable {

    private final AdjacencyTable outgoing = new AdjacencyTable();
    private final AdjacencyTable incoming = new AdjacencyTable();
    private final KHopTraversal traversal = new KHopTraversal();
    private Persistence persistence = Persistence.NONE;

    public GraphKernel() {
    }

    public static GraphKernel openInMemory() {
        return new GraphKernel();
    }

    public static GraphKernel open(Path directory, WalConfig config) throws IOException {
        return RecoveryManager.recover(directory, config).kernel();
    }

    public void attachPersistence(Persistence persistence) {
        Objects.requireNonNull(persistence, "persistence");
        if (this.persistence != Persistence.NONE) {
            throw new IllegalStateException("a persistence store is already attached");
        }
        this.persistence = persistence;
    }

    public boolean isDurable() {
        return persistence != Persistence.NONE;
    }

    public void checkpoint() {
        persistence.checkpoint();
    }

    public void sync() {
        persistence.sync();
    }

    @Override
    public void close() {
        persistence.close();
    }

    public boolean addEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        if (outgoing.contains(u, v)) {
            return false;
        }
        persistence.recordAdd(u, v);
        ensureCapacity(Math.max(u, v) + 1);
        return insert(u, v);
    }

    public boolean removeEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        if (!outgoing.contains(u, v)) {
            return false;
        }
        persistence.recordRemove(u, v);
        return delete(u, v);
    }

    public int addEdges(long[] pairs, int pairCount) {
        long largest = validatePairs(pairs, pairCount);
        ensureCapacity(largest + 1);
        int added = 0;
        persistence.beginBatch();
        try {
            for (int i = 0; i < pairCount; i++) {
                long u = pairs[2 * i];
                long v = pairs[2 * i + 1];
                if (!outgoing.contains(u, v)) {
                    persistence.recordAdd(u, v);
                    insert(u, v);
                    added++;
                }
            }
        } finally {
            persistence.endBatch();
        }
        return added;
    }

    public int removeEdges(long[] pairs, int pairCount) {
        validatePairs(pairs, pairCount);
        int removed = 0;
        persistence.beginBatch();
        try {
            for (int i = 0; i < pairCount; i++) {
                long u = pairs[2 * i];
                long v = pairs[2 * i + 1];
                if (outgoing.contains(u, v)) {
                    persistence.recordRemove(u, v);
                    delete(u, v);
                    removed++;
                }
            }
        } finally {
            persistence.endBatch();
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

    public int nodeCapacity() {
        return outgoing.capacity();
    }

    public long outgoingNeighbor(long u, int index) {
        NodeIds.checkValid(u);
        Objects.checkIndex(index, outgoing.degreeOf(u));
        return outgoing.neighborAt(u, index);
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
