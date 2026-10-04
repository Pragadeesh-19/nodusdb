package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.OutputBufferTooSmallException;

public final class GraphSession implements AutoCloseable {

    private static final int INITIAL_RESULT_CAPACITY = 64;
    private static final int MAX_BATCH_PAIRS = Integer.MAX_VALUE / 4;

    private final GraphKernel kernel;
    private long[] results = new long[INITIAL_RESULT_CAPACITY];
    private long[] edges = new long[INITIAL_RESULT_CAPACITY];

    public GraphSession(GraphKernel kernel) {
        this.kernel = kernel;
    }

    public void checkpoint() {
        kernel.checkpoint();
    }

    public void sync() {
        kernel.sync();
    }

    @Override
    public void close() {
        kernel.close();
    }

    public boolean addEdge(long u, long v) {
        return kernel.addEdge(u, v);
    }

    public boolean removeEdge(long u, long v) {
        return kernel.removeEdge(u, v);
    }

    public boolean hasEdge(long u, long v) {
        return kernel.hasEdge(u, v);
    }

    public int degree(long u) {
        return kernel.getDegree(u);
    }

    public int inDegree(long v) {
        return kernel.getInDegree(v);
    }

    public long[] edgeBuffer(int pairCount) {
        if (pairCount < 0 || pairCount > MAX_BATCH_PAIRS) {
            throw new IllegalArgumentException("batch size out of range: " + pairCount);
        }
        if (edges.length < 2 * pairCount) {
            edges = new long[Math.max(2 * pairCount, edges.length << 1)];
        }
        return edges;
    }

    public int addEdges(int pairCount) {
        return kernel.addEdges(edges, pairCount);
    }

    public int removeEdges(int pairCount) {
        return kernel.removeEdges(edges, pairCount);
    }

    public int commonNeighbors(long u, long v) {
        while (true) {
            try {
                return kernel.commonNeighbors(u, v, results);
            } catch (OutputBufferTooSmallException e) {
                grow();
            }
        }
    }

    public int khop(long start, int maxDepth) {
        while (true) {
            try {
                return kernel.kHop(start, maxDepth, results);
            } catch (OutputBufferTooSmallException e) {
                grow();
            }
        }
    }

    public long result(int index) {
        return results[index];
    }

    private void grow() {
        results = new long[results.length << 1];
    }
}
