package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.traversal.OutputBufferTooSmallException;

public final class GraphSession implements AutoCloseable {

    private static final int INITIAL_RESULT_CAPACITY = 64;
    private static final int MAX_BATCH_PAIRS = Integer.MAX_VALUE / 4;

    private final GraphKernel kernel;
    private final StringKeys strings;
    private boolean closed;
    private long[] results = new long[INITIAL_RESULT_CAPACITY];
    private long[] edges = new long[INITIAL_RESULT_CAPACITY];
    private long[] keyIds = new long[INITIAL_RESULT_CAPACITY];

    public GraphSession(GraphKernel kernel) {
        this.kernel = kernel;
        this.strings = new StringKeys(kernel);
        if (kernel.keyKind() == KeyKind.UNSET && kernel.hasEdges()) {
            kernel.claimKeyKind(KeyKind.INTEGER);
        }
    }

    public synchronized void checkpoint() {
        requireOpen();
        kernel.checkpoint();
    }

    public synchronized void sync() {
        requireOpen();
        kernel.sync();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        kernel.close();
    }

    public synchronized KeyKind keyKind() {
        requireOpen();
        return kernel.keyKind();
    }

    public synchronized KeyKind claimKeys(KeyKind wanted) {
        requireOpen();
        if (kernel.keyKind() == KeyKind.UNSET) {
            kernel.claimKeyKind(wanted);
        }
        return kernel.keyKind();
    }

    public synchronized long intern(byte[] utf8, int offset, int length) {
        requireOpen();
        requireStringKeys();
        return strings.intern(utf8, offset, length);
    }

    public synchronized long lookup(byte[] utf8, int offset, int length) {
        requireOpen();
        if (kernel.keyKind() == KeyKind.INTEGER) {
            throw new IllegalStateException("graph is keyed by integers");
        }
        return strings.lookup(utf8, offset, length);
    }

    public synchronized byte[] resolve(long id) {
        requireOpen();
        return strings.resolve(id);
    }

    public synchronized int addStringEdges(byte[] utf8, int[] lengths, int pairCount) {
        requireOpen();
        requireStringKeys();
        if (pairCount < 0 || pairCount > MAX_BATCH_PAIRS) {
            throw new IllegalArgumentException("batch size out of range: " + pairCount);
        }
        int keys = 2 * pairCount;
        if (keyIds.length < keys) {
            keyIds = new long[Math.max(keys, keyIds.length << 1)];
        }
        strings.internAll(utf8, lengths, keys, keyIds);
        long[] staged = edgeBuffer(pairCount);
        System.arraycopy(keyIds, 0, staged, 0, keys);
        return kernel.addEdges(staged, pairCount);
    }

    public synchronized boolean addEdge(long u, long v) {
        requireOpen();
        return kernel.addEdge(u, v);
    }

    public synchronized boolean removeEdge(long u, long v) {
        requireOpen();
        return kernel.removeEdge(u, v);
    }

    public synchronized boolean hasEdge(long u, long v) {
        requireOpen();
        return kernel.hasEdge(u, v);
    }

    public synchronized int degree(long u) {
        requireOpen();
        return kernel.getDegree(u);
    }

    public synchronized int inDegree(long v) {
        requireOpen();
        return kernel.getInDegree(v);
    }

    public synchronized long[] edgeBuffer(int pairCount) {
        requireOpen();
        if (pairCount < 0 || pairCount > MAX_BATCH_PAIRS) {
            throw new IllegalArgumentException("batch size out of range: " + pairCount);
        }
        if (edges.length < 2 * pairCount) {
            edges = new long[Math.max(2 * pairCount, edges.length << 1)];
        }
        return edges;
    }

    public synchronized int addEdges(int pairCount) {
        requireOpen();
        return kernel.addEdges(edges, pairCount);
    }

    public synchronized int removeEdges(int pairCount) {
        requireOpen();
        return kernel.removeEdges(edges, pairCount);
    }

    public synchronized int commonNeighbors(long u, long v) {
        requireOpen();
        while (true) {
            try {
                return kernel.commonNeighbors(u, v, results);
            } catch (OutputBufferTooSmallException e) {
                grow();
            }
        }
    }

    public synchronized int khop(long start, int maxDepth) {
        requireOpen();
        while (true) {
            try {
                return kernel.kHop(start, maxDepth, results);
            } catch (OutputBufferTooSmallException e) {
                grow();
            }
        }
    }

    public synchronized long result(int index) {
        requireOpen();
        return results[index];
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("graph is closed");
        }
    }

    private void requireStringKeys() {
        if (claimKeys(KeyKind.STRING) != KeyKind.STRING) {
            throw new IllegalStateException("graph is keyed by integers");
        }
    }

    private void grow() {
        results = new long[results.length << 1];
    }
}
