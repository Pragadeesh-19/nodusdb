package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.traversal.OutputBufferTooSmallException;
import io.nodusdb.replica.FollowerRuntime;
import io.nodusdb.ship.ShipStats;

import java.time.Duration;
import java.util.Arrays;

public final class GraphSession implements AutoCloseable {

    private static final int INITIAL_RESULT_CAPACITY = 64;
    private static final int MAX_BATCH_PAIRS = Integer.MAX_VALUE / 4;

    private final GraphKernel kernel;
    private final StringKeys strings;
    private final TupleStore tuples;
    private final FollowerRuntime follower;
    private final ThreadLocal<long[]> results = ThreadLocal.withInitial(() -> new long[INITIAL_RESULT_CAPACITY]);
    private volatile boolean closed;
    private long[] edges = new long[INITIAL_RESULT_CAPACITY];
    private long[] keyIds = new long[INITIAL_RESULT_CAPACITY];

    public GraphSession(GraphKernel kernel) {
        this.kernel = kernel;
        this.strings = new StringKeys(kernel);
        if (kernel.keyKind() == KeyKind.UNSET && kernel.hasEdges()) {
            kernel.claimKeyKind(KeyKind.INTEGER);
        }
        this.tuples = TupleStore.open(kernel);
        this.follower = null;
    }

    public GraphSession(FollowerRuntime follower) {
        this.kernel = follower.kernel();
        this.strings = new StringKeys(kernel);
        this.tuples = follower.tuples();
        this.follower = follower;
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
        if (follower != null) {
            follower.close();
            return;
        }
        kernel.close();
    }

    public KeyKind keyKind() {
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

    public long lookup(byte[] utf8, int offset, int length) {
        requireReadable();
        if (kernel.keyKind() == KeyKind.INTEGER) {
            throw new IllegalStateException("graph is keyed by integers");
        }
        return strings.lookup(utf8, offset, length);
    }

    public byte[] resolve(long id) {
        requireReadable();
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

    public boolean hasEdge(long u, long v) {
        requireReadable();
        return kernel.hasEdge(u, v);
    }

    public int degree(long u) {
        requireReadable();
        return kernel.getDegree(u);
    }

    public int inDegree(long v) {
        requireReadable();
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

    public int commonNeighbors(long u, long v) {
        requireReadable();
        while (true) {
            try {
                return kernel.commonNeighbors(u, v, results.get());
            } catch (OutputBufferTooSmallException e) {
                grow();
            }
        }
    }

    public int khop(long start, int maxDepth) {
        requireReadable();
        while (true) {
            try {
                return kernel.kHop(start, maxDepth, results.get());
            } catch (OutputBufferTooSmallException e) {
                grow();
            }
        }
    }

    public long result(int index) {
        requireReadable();
        return results.get()[index];
    }

    public synchronized Token applySchema(String document) {
        requireOpen();
        return tuples.applySchema(document);
    }

    public Token writeTuples(TupleTransaction transaction, Durability durability) {
        if (durability != Durability.LAKE) {
            return writeLocally(transaction);
        }
        tuples.requireShipping();
        Token token = writeLocally(transaction);
        awaitShipped(token, TupleStore.DEFAULT_SHIP_WAIT);
        return token;
    }

    public void awaitShipped(Token token, Duration timeout) {
        requireOpen();
        tuples.awaitShipped(token, timeout);
    }

    public String statsJson() {
        requireOpen();
        if (follower != null) {
            return follower.statsJson();
        }
        return ShipStats.json(kernel.epoch(), kernel.appliedLsn(), kernel.shipWatermark());
    }

    private synchronized Token writeLocally(TupleTransaction transaction) {
        requireOpen();
        return tuples.write(transaction, Durability.LOCAL);
    }

    public boolean check(String object, String permission, String subject, Token atLeast) {
        requireOpen();
        if (follower != null && atLeast != null) {
            follower.gate().awaitToken(atLeast);
        }
        requireReadable();
        return tuples.check(object, permission, subject, atLeast);
    }

    public Token token() {
        requireReadable();
        return tuples.token();
    }

    public int schemaVersion() {
        requireReadable();
        return tuples.schemaVersion();
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("graph is closed");
        }
    }

    private void requireReadable() {
        requireOpen();
        if (follower != null) {
            follower.gate().requireReadable();
        }
    }

    private void requireStringKeys() {
        if (claimKeys(KeyKind.STRING) != KeyKind.STRING) {
            throw new IllegalStateException("graph is keyed by integers");
        }
    }

    private void grow() {
        long[] current = results.get();
        results.set(Arrays.copyOf(current, current.length << 1));
    }
}
