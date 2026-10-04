package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.OutputBufferTooSmallException;
import io.nodusdb.kernel.StringInterner;
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

    public void checkpoint() {
        kernel.checkpoint();
    }

    public void sync() {
        kernel.sync();
    }

    @Override
    public void close() {
        try {
            kernel.close();
        } finally {
            closeSymbols();
        }
    }

    public KeyKind keyKind() {
        return keyKind;
    }

    public KeyKind claimKeys(KeyKind wanted) {
        if (keyKind == KeyKind.UNSET) {
            if (symbols != null) {
                persistKind(wanted);
            }
            keyKind = wanted;
        }
        return keyKind;
    }

    public long intern(byte[] utf8, int offset, int length) {
        requireStringKeys();
        long before = strings.size();
        long id = strings.intern(utf8, offset, length);
        persistStrings(before);
        return id;
    }

    public long lookup(byte[] utf8, int offset, int length) {
        if (keyKind == KeyKind.INTEGER) {
            throw new IllegalStateException("graph is keyed by integers");
        }
        return strings.lookup(utf8, offset, length);
    }

    public byte[] resolve(long id) {
        return strings.resolve(id);
    }

    public int addStringEdges(byte[] utf8, int[] lengths, int pairCount) {
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
