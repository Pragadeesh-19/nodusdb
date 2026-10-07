package io.nodusdb.kernel;

import io.nodusdb.kernel.adjacency.AdjacencyTable;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.kernel.traversal.KHopTraversal;
import io.nodusdb.kernel.traversal.OutputBufferTooSmallException;
import io.nodusdb.kernel.wal.RecoveryManager;
import io.nodusdb.kernel.wal.WalConfig;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.Objects;

public final class GraphKernel implements AutoCloseable {

    private static final VarHandle SEQUENCE;

    static {
        try {
            SEQUENCE = MethodHandles.lookup().findVarHandle(GraphKernel.class, "sequence", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static final long NO_MEMORY_LIMIT = 0L;

    private final Arena arena = Arena.ofAuto();
    private final MemoryBudget budget;
    private final AdjacencyTable outgoing;
    private final AdjacencyTable incoming;
    private boolean closed;
    private final ThreadLocal<KHopTraversal> traversals = ThreadLocal.withInitial(KHopTraversal::new);
    private Persistence persistence = Persistence.NONE;
    private long sequence;

    public GraphKernel() {
        this(NO_MEMORY_LIMIT);
    }

    public GraphKernel(long maxMemoryBytes) {
        if (maxMemoryBytes < NO_MEMORY_LIMIT) {
            throw new IllegalArgumentException("memory limit must not be negative: " + maxMemoryBytes);
        }
        this.budget = maxMemoryBytes == NO_MEMORY_LIMIT ? MemoryBudget.unlimited() : MemoryBudget.limitedTo(maxMemoryBytes);
        this.outgoing = new AdjacencyTable(arena, budget);
        this.incoming = new AdjacencyTable(arena, budget);
    }

    public static GraphKernel openInMemory() {
        return new GraphKernel();
    }

    public static GraphKernel open(Path directory, WalConfig config) throws IOException {
        return open(directory, config, NO_MEMORY_LIMIT);
    }

    public static GraphKernel open(Path directory, WalConfig config, long maxMemoryBytes) throws IOException {
        return RecoveryManager.recover(directory, config, maxMemoryBytes).kernel();
    }

    public long memoryUsedBytes() {
        return budget.used();
    }

    public long memoryLimitBytes() {
        return budget.isLimited() ? budget.limit() : NO_MEMORY_LIMIT;
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
        if (closed) {
            return;
        }
        closed = true;
        persistence.close();
    }

    public boolean addEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        beginWrite();
        try {
            if (outgoing.contains(u, v)) {
                return false;
            }
            ensureCapacity(Math.max(u, v) + 1);
            reserveHeadroom(u, v);
            persistence.recordAdd(u, v);
            return insert(u, v);
        } finally {
            endWrite();
        }
    }

    public boolean removeEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        beginWrite();
        try {
            if (!outgoing.contains(u, v)) {
                return false;
            }
            persistence.recordRemove(u, v);
            return delete(u, v);
        } finally {
            endWrite();
        }
    }

    public int addEdges(long[] pairs, int pairCount) {
        long largest = validatePairs(pairs, pairCount);
        beginWrite();
        try {
            ensureCapacity(largest + 1);
            int added = 0;
            persistence.beginBatch();
            try {
                for (int i = 0; i < pairCount; i++) {
                    long u = pairs[2 * i];
                    long v = pairs[2 * i + 1];
                    if (!outgoing.contains(u, v)) {
                        reserveHeadroom(u, v);
                        persistence.recordAdd(u, v);
                        insert(u, v);
                        added++;
                    }
                }
            } finally {
                persistence.endBatch();
            }
            return added;
        } finally {
            endWrite();
        }
    }

    public int removeEdges(long[] pairs, int pairCount) {
        validatePairs(pairs, pairCount);
        beginWrite();
        try {
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
        } finally {
            endWrite();
        }
    }

    public boolean hasEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        while (true) {
            long started = beginRead();
            boolean present = outgoing.contains(u, v);
            if (endRead(started)) {
                return present;
            }
        }
    }

    public int getDegree(long u) {
        NodeIds.checkValid(u);
        while (true) {
            long started = beginRead();
            int degree = outgoing.degreeOf(u);
            if (endRead(started)) {
                return degree;
            }
        }
    }

    public int getInDegree(long v) {
        NodeIds.checkValid(v);
        while (true) {
            long started = beginRead();
            int degree = incoming.degreeOf(v);
            if (endRead(started)) {
                return degree;
            }
        }
    }

    public boolean hasEdges() {
        int capacity = nodeCapacity();
        for (int u = 0; u < capacity; u++) {
            if (getDegree(u) > 0) {
                return true;
            }
        }
        return false;
    }

    public int nodeCapacity() {
        while (true) {
            long started = beginRead();
            int capacity = outgoing.capacity();
            if (endRead(started)) {
                return capacity;
            }
        }
    }

    public long outgoingNeighbor(long u, int index) {
        return neighborAt(outgoing, u, index);
    }

    public long incomingNeighbor(long v, int index) {
        return neighborAt(incoming, v, index);
    }

    public void prepareBulkLoad(int[] forwardDegrees, int[] backwardDegrees) {
        if (forwardDegrees.length != backwardDegrees.length) {
            throw new IllegalArgumentException("degree arrays differ in length");
        }
        if (hasEdges()) {
            throw new IllegalStateException("bulk load needs an empty graph");
        }
        ensureCapacity(forwardDegrees.length);
        outgoing.prepareBulkLoad(forwardDegrees);
        incoming.prepareBulkLoad(backwardDegrees);
    }

    public void loadBulkNode(boolean forward, long node, long[] neighbors, int degree) {
        if (degree <= 0 || degree > neighbors.length) {
            throw new IllegalArgumentException("degree " + degree + " does not fit " + neighbors.length + " neighbors");
        }
        AdjacencyTable table = forward ? outgoing : incoming;
        table.fillBulkNode((int) node, neighbors, degree);
    }

    public int commonNeighbors(long u, long v, long[] out) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        while (true) {
            long started = beginRead();
            int count = commonNeighborsOnce(u, v, out);
            if (endRead(started)) {
                if (count == KHopTraversal.OVERFLOW) {
                    throw new OutputBufferTooSmallException("output buffer too small for common neighbors");
                }
                return count;
            }
        }
    }

    public int kHop(long start, int maxDepth, long[] out) {
        NodeIds.checkValid(start);
        KHopTraversal traversal = traversals.get();
        while (true) {
            long started = beginRead();
            traversal.ensureCapacity(outgoing.capacity());
            int count = traversal.kHop(outgoing, start, maxDepth, out);
            if (endRead(started)) {
                if (count == KHopTraversal.OVERFLOW) {
                    throw new OutputBufferTooSmallException("output buffer too small for k-hop result");
                }
                return count;
            }
        }
    }

    private int commonNeighborsOnce(long u, long v, long[] out) {
        boolean uIsSmaller = outgoing.degreeOf(u) <= outgoing.degreeOf(v);
        long smaller = uIsSmaller ? u : v;
        long larger = uIsSmaller ? v : u;
        int smallerDegree = outgoing.degreeOf(smaller);
        if (out.length < smallerDegree) {
            return KHopTraversal.OVERFLOW;
        }
        int count = 0;
        for (int i = 0; i < smallerDegree; i++) {
            long candidate = outgoing.neighborAt(smaller, i);
            if (outgoing.contains(larger, candidate)) {
                if (count == out.length) {
                    return KHopTraversal.OVERFLOW;
                }
                out[count++] = candidate;
            }
        }
        return count;
    }

    boolean isHighDegree(long u) {
        return outgoing.isHighDegree(u);
    }

    boolean hasIncoming(long v, long u) {
        return incoming.contains(v, u);
    }

    private long neighborAt(AdjacencyTable table, long node, int index) {
        NodeIds.checkValid(node);
        while (true) {
            long started = beginRead();
            int degree = table.degreeOf(node);
            long neighbor = index >= 0 && index < degree ? table.neighborAt(node, index) : NodeIds.NONE;
            if (endRead(started)) {
                Objects.checkIndex(index, degree);
                return neighbor;
            }
        }
    }

    private void beginWrite() {
        SEQUENCE.set(this, (long) SEQUENCE.get(this) + 1);
        VarHandle.releaseFence();
    }

    private void endWrite() {
        VarHandle.releaseFence();
        SEQUENCE.setRelease(this, (long) SEQUENCE.get(this) + 1);
    }

    private long beginRead() {
        long started;
        while (((started = (long) SEQUENCE.getAcquire(this)) & 1L) != 0) {
            Thread.onSpinWait();
        }
        return started;
    }

    private boolean endRead(long started) {
        VarHandle.acquireFence();
        return (long) SEQUENCE.get(this) == started;
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

    private void reserveHeadroom(long u, long v) {
        outgoing.reserveForAdd(u);
        incoming.reserveForAdd(v);
    }

    private void ensureCapacity(long requiredNodes) {
        if (requiredNodes <= Math.min(outgoing.capacity(), incoming.capacity())) {
            return;
        }
        int nodes = (int) requiredNodes;
        budget.require(outgoing.bytesToHold(nodes) + incoming.bytesToHold(nodes));
        outgoing.ensureCapacity(nodes);
        incoming.ensureCapacity(nodes);
    }
}
