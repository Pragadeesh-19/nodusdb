package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.symbols.StringInterner;
import io.nodusdb.kernel.traversal.OutputBufferTooSmallException;
import io.nodusdb.kernel.wal.SymbolLog;

import java.io.IOException;
import java.io.UncheckedIOException;

public final class GraphSession implements AutoCloseable {

    private static final int INITIAL_RESULT_CAPACITY = 64;
    private static final int MAX_BATCH_PAIRS = Integer.MAX_VALUE / 4;

    private final GraphKernel kernel;
    private final StringInterner strings;
    private final SymbolLog symbols;
    private KeyKind keyKind;
    private boolean symbolsFailed;
    private boolean closed;
    private long[] results = new long[INITIAL_RESULT_CAPACITY];
    private long[] edges = new long[INITIAL_RESULT_CAPACITY];
    private long[] keyIds = new long[INITIAL_RESULT_CAPACITY];

    public GraphSession(GraphKernel kernel) {
        this(kernel, new StringInterner(), null);
    }

    public GraphSession(GraphKernel kernel, StringInterner strings, SymbolLog symbols) {
        this.kernel = kernel;
        this.strings = strings;
        this.symbols = symbols;
        this.keyKind = symbols == null ? KeyKind.UNSET : symbols.kind();
        if (keyKind == KeyKind.UNSET && kernel.hasEdges()) {
            claimKeys(KeyKind.INTEGER);
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
        try {
            kernel.close();
        } finally {
            closeSymbols();
        }
    }

    public synchronized KeyKind keyKind() {
        requireOpen();
        return keyKind;
    }

    public synchronized KeyKind claimKeys(KeyKind wanted) {
        requireOpen();
        if (keyKind == KeyKind.UNSET) {
            if (symbols != null) {
                persistKind(wanted);
            }
            keyKind = wanted;
        }
        return keyKind;
    }

    public synchronized long intern(byte[] utf8, int offset, int length) {
        requireOpen();
        requireStringKeys();
        long before = strings.size();
        long id = strings.intern(utf8, offset, length);
        persistStrings(before);
        return id;
    }

    public synchronized long lookup(byte[] utf8, int offset, int length) {
        requireOpen();
        if (keyKind == KeyKind.INTEGER) {
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
        long before = strings.size();
        int cursor = 0;
        for (int i = 0; i < keys; i++) {
            int length = lengths[i];
            if (length < 0 || length > utf8.length - cursor) {
                throw new IllegalArgumentException("string lengths exceed the supplied bytes");
            }
            keyIds[i] = strings.intern(utf8, cursor, length);
            cursor += length;
        }
        persistStrings(before);
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
        if (symbolsFailed) {
            throw new IllegalStateException("the string table could not be saved; reopen the graph");
        }
        if (claimKeys(KeyKind.STRING) != KeyKind.STRING) {
            throw new IllegalStateException("graph is keyed by integers");
        }
    }

    private void persistStrings(long before) {
        long after = strings.size();
        if (symbols == null || after == before) {
            return;
        }
        try {
            symbols.append(strings, before, after);
        } catch (IOException e) {
            symbolsFailed = true;
            throw new UncheckedIOException(e);
        }
    }

    private void persistKind(KeyKind wanted) {
        try {
            symbols.setKind(wanted);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void closeSymbols() {
        if (symbols == null) {
            return;
        }
        try {
            symbols.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void grow() {
        results = new long[results.length << 1];
    }
}
