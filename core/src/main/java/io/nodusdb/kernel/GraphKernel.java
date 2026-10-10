package io.nodusdb.kernel;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.adjacency.AdjacencyTable;
import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.concurrency.WriteSequence;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.symbols.SymbolTable;
import io.nodusdb.kernel.traversal.KHopTraversal;
import io.nodusdb.kernel.traversal.OutputBufferTooSmallException;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.ShipWatermark;
import io.nodusdb.log.VolatileLog;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

public final class GraphKernel implements AutoCloseable {

    public static final long NO_MEMORY_LIMIT = 0L;
    public static final long UNCHANGED = -1L;

    private static final String FAULTED_MESSAGE =
            "the graph state is inconsistent with its log; reopen the graph";

    private final GraphState state;
    private final WriteSequence sequence;
    private final RecordApplier applier;
    private final WriteReservation reservation;
    private final ReplicaWriter replicaWriter;
    private final ReentrantLock writer = new ReentrantLock();
    private final RecordBatch scratch = new RecordBatch();
    private final RecordReader applyReader = new RecordReader();
    private final ThreadLocal<KHopTraversal> traversals = ThreadLocal.withInitial(KHopTraversal::new);
    private LogStore log = new VolatileLog();
    private DurableStorage storage;
    private volatile ShipWatermark shipWatermark = ShipWatermark.NONE;
    private boolean closed;

    public GraphKernel() {
        this(NO_MEMORY_LIMIT);
    }

    public GraphKernel(long maxMemoryBytes) {
        this(maxMemoryBytes, false);
    }

    private GraphKernel(long maxMemoryBytes, boolean replica) {
        if (maxMemoryBytes < NO_MEMORY_LIMIT) {
            throw new IllegalArgumentException("memory limit must not be negative: " + maxMemoryBytes);
        }
        MemoryBudget budget = maxMemoryBytes == NO_MEMORY_LIMIT
                ? MemoryBudget.unlimited() : MemoryBudget.limitedTo(maxMemoryBytes);
        this.state = new GraphState(budget);
        this.sequence = state.sequence();
        this.applier = new RecordApplier(state);
        this.reservation = new WriteReservation(state);
        this.replicaWriter = replica ? new ReplicaWriter(state, applier, reservation) : null;
    }

    public static GraphKernel openInMemory() {
        return new GraphKernel();
    }

    public static GraphKernel openReplica(long maxMemoryBytes) {
        return new GraphKernel(maxMemoryBytes, true);
    }

    public boolean isReplica() {
        return replicaWriter != null;
    }

    public long memoryUsedBytes() {
        return state.budget().used();
    }

    public long memoryLimitBytes() {
        return state.budget().isLimited() ? state.budget().limit() : NO_MEMORY_LIMIT;
    }

    public void attachLog(LogStore log, DurableStorage storage) {
        requireWritable();
        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(storage, "storage");
        writer.lock();
        try {
            if (this.storage != null) {
                throw new IllegalStateException("a durable store is already attached");
            }
            this.log = log;
            this.storage = storage;
            state.appliedLsn(log.lastLsn());
        } finally {
            writer.unlock();
        }
    }

    public void attachShipping(ShipWatermark watermark) {
        requireWritable();
        Objects.requireNonNull(watermark, "watermark");
        writer.lock();
        try {
            if (shipWatermark.configured()) {
                throw new IllegalStateException("shipping is already attached");
            }
            this.shipWatermark = watermark;
        } finally {
            writer.unlock();
        }
    }

    public ShipWatermark shipWatermark() {
        return shipWatermark;
    }

    public boolean isDurable() {
        return storage != null;
    }

    public long epoch() {
        return isReplica() ? state.epochs().latestEpoch() : log.epoch();
    }

    public long appliedLsn() {
        return state.appliedLsn();
    }

    public long lastCommitMicros() {
        return isReplica() ? state.lastCommitMicros() : log.lastCommitMicros();
    }

    public Token token() {
        return new Token(epoch(), state.appliedLsn());
    }

    public KeyKind keyKind() {
        return state.keyKind();
    }

    public RelationCatalog catalog() {
        return state.catalog();
    }

    public SymbolTable symbols() {
        return state.symbols();
    }

    public EpochHistory epochHistory() {
        return state.epochs();
    }

    public void checkpoint() {
        requireWritable();
        writer.lock();
        try {
            requireOpen();
            requireDurable("checkpoint");
            checkpointLocked();
        } finally {
            writer.unlock();
        }
    }

    public void sync() {
        writer.lock();
        try {
            requireOpen();
            requireDurable("sync");
            log.force();
        } finally {
            writer.unlock();
        }
    }

    @Override
    public void close() {
        writer.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (storage != null) {
                closeStorage();
            }
        } finally {
            writer.unlock();
        }
    }

    public long commit(RecordBatch batch) {
        requireWritable();
        if (batch.isEmpty() || !batch.isCommitted()) {
            throw new IllegalArgumentException("a transaction must hold records and end with a commit");
        }
        writer.lock();
        try {
            requireOpen();
            return commitLocked(batch, true);
        } finally {
            writer.unlock();
        }
    }

    public long claimKeyKind(KeyKind kind) {
        requireWritable();
        if (kind == KeyKind.UNSET) {
            throw new IllegalArgumentException("a graph cannot be claimed as unset");
        }
        writer.lock();
        try {
            requireOpen();
            if (state.keyKind() == kind) {
                return state.appliedLsn();
            }
            RecordBatch claim = new RecordBatch();
            claim.graphConfig(kind.code());
            claim.commit();
            return commitLocked(claim, true);
        } finally {
            writer.unlock();
        }
    }

    public boolean addEdge(long u, long v) {
        requireWritable();
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        return writeSingle(RecordType.TUPLE_ADD, (int) u, 0, 0, (int) v, true) != UNCHANGED;
    }

    public boolean removeEdge(long u, long v) {
        requireWritable();
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        return writeSingle(RecordType.TUPLE_REMOVE, (int) u, 0, 0, (int) v, true) != UNCHANGED;
    }

    public long addTuple(int object, int relation, int subjectRelation, int subject) {
        requireWritable();
        NodeIds.checkValid(object);
        NodeIds.checkValid(subject);
        return writeSingle(RecordType.TUPLE_ADD, object, relation, subjectRelation, subject, true);
    }

    public long removeTuple(int object, int relation, int subjectRelation, int subject) {
        requireWritable();
        NodeIds.checkValid(object);
        NodeIds.checkValid(subject);
        return writeSingle(RecordType.TUPLE_REMOVE, object, relation, subjectRelation, subject, true);
    }

    public int addEdges(long[] pairs, int pairCount) {
        requireWritable();
        validatePairs(pairs, pairCount);
        return writePairs(RecordType.TUPLE_ADD, pairs, pairCount);
    }

    public int removeEdges(long[] pairs, int pairCount) {
        requireWritable();
        validatePairs(pairs, pairCount);
        return writePairs(RecordType.TUPLE_REMOVE, pairs, pairCount);
    }

    public void requireWritable() {
        if (isReplica()) {
            throw new UnsupportedFeatureException("this graph is a follower and cannot be written");
        }
    }

    public boolean hasEdge(long u, long v) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        while (true) {
            long started = sequence.beginRead();
            boolean present = state.direct().contains(u, v);
            if (sequence.validate(started)) {
                return present;
            }
        }
    }

    public int getDegree(long u) {
        NodeIds.checkValid(u);
        while (true) {
            long started = sequence.beginRead();
            int degree = state.direct().degree(u);
            if (sequence.validate(started)) {
                return degree;
            }
        }
    }

    public int getInDegree(long v) {
        NodeIds.checkValid(v);
        while (true) {
            long started = sequence.beginRead();
            int degree = state.direct().inDegree(v);
            if (sequence.validate(started)) {
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
            long started = sequence.beginRead();
            int capacity = state.nodeCapacity();
            if (sequence.validate(started)) {
                return capacity;
            }
        }
    }

    public long outgoingNeighbor(long u, int index) {
        return neighborAt(true, u, index);
    }

    public long incomingNeighbor(long v, int index) {
        return neighborAt(false, v, index);
    }

    public long readStart() {
        return sequence.beginRead();
    }

    public boolean readStillValid(long started) {
        return sequence.validate(started);
    }

    public boolean probeTuple(int object, int relation, int subjectRelation, int subject) {
        EdgeTables tables = state.tables(state.partitionOf(relation, subjectRelation));
        return tables != null && tables.contains(object, EdgeKey.pack(relation, subjectRelation, subject));
    }

    public int probeDegree(Partition partition, int object) {
        EdgeTables tables = state.tables(partition);
        return tables == null ? 0 : tables.degree(object);
    }

    public long probeKey(Partition partition, int object, int index) {
        EdgeTables tables = state.tables(partition);
        return tables == null ? NodeIds.NONE : tables.outgoingKeyAt(object, index);
    }

    public int degree(Partition partition, boolean outgoing, int node) {
        EdgeTables tables = state.tables(partition);
        if (tables == null) {
            return 0;
        }
        return outgoing ? tables.degree(node) : tables.inDegree(node);
    }

    public long keyAt(Partition partition, boolean outgoing, int node, int index) {
        EdgeTables tables = state.tables(partition);
        return outgoing ? tables.outgoingKeyAt(node, index) : tables.incomingKeyAt(node, index);
    }

    public boolean hasPartition(Partition partition) {
        return state.tables(partition) != null;
    }

    public int commonNeighbors(long u, long v, long[] out) {
        NodeIds.checkValid(u);
        NodeIds.checkValid(v);
        while (true) {
            long started = sequence.beginRead();
            int count = commonNeighborsOnce(u, v, out);
            if (sequence.validate(started)) {
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
            long started = sequence.beginRead();
            AdjacencyTable table = state.direct().outgoingTable();
            traversal.ensureCapacity(table.capacity());
            int count = traversal.kHop(table, start, maxDepth, out);
            if (sequence.validate(started)) {
                if (count == KHopTraversal.OVERFLOW) {
                    throw new OutputBufferTooSmallException("output buffer too small for k-hop result");
                }
                return count;
            }
        }
    }

    public void replay(RecordReader record) {
        applier.apply(record);
    }

    public void restorePosition(long lsn, long lastCommitMicros) {
        requireReplica("restore a position");
        if (lsn < 0 || lastCommitMicros < 0) {
            throw new IllegalArgumentException("a position is non-negative: " + lsn + ", " + lastCommitMicros);
        }
        writer.lock();
        try {
            requireOpen();
            if (state.appliedLsn() != 0 || state.lastCommitMicros() != 0) {
                throw new IllegalStateException("the follower already holds position " + state.appliedLsn());
            }
            state.lastCommitMicros(lastCommitMicros);
            state.appliedLsn(lsn);
        } finally {
            writer.unlock();
        }
    }

    public void applyReplicated(byte[] records, long lsnFirst, long lsnLast) {
        Objects.requireNonNull(records, "records");
        requireReplica("apply replicated records");
        writer.lock();
        try {
            requireOpen();
            replicaWriter.apply(records, lsnFirst, lsnLast);
        } finally {
            writer.unlock();
        }
    }

    public void restoreSymbol(int id, byte[] utf8) {
        state.symbols().append(id, utf8, 0, utf8.length);
    }

    public void restoreCatalog(RelationCatalog catalog) {
        state.catalog(catalog);
    }

    public void restoreKeyKind(KeyKind kind) {
        state.keyKind(kind);
    }

    public void recordEpoch(long epoch, long firstLsn, long handoffLsn) {
        state.epochs().record(epoch, firstLsn, handoffLsn);
    }

    public void prepareBulkLoad(int[] forwardDegrees, int[] backwardDegrees) {
        prepareBulkLoad(Partition.DIRECT, forwardDegrees, backwardDegrees);
    }

    public void prepareBulkLoad(Partition partition, int[] forwardDegrees, int[] backwardDegrees) {
        if (forwardDegrees.length != backwardDegrees.length) {
            throw new IllegalArgumentException("degree arrays differ in length");
        }
        if (partition == Partition.DIRECT && hasEdges()) {
            throw new IllegalStateException("bulk load needs an empty graph");
        }
        state.ensureNodes(forwardDegrees.length);
        EdgeTables tables = partition == Partition.DIRECT ? state.direct() : state.ensureIndirect();
        tables.prepareBulkLoad(forwardDegrees, backwardDegrees);
    }

    public void loadBulkNode(boolean forward, long node, long[] neighbors, int degree) {
        loadBulkNode(Partition.DIRECT, forward, node, neighbors, degree);
    }

    public void loadBulkNode(Partition partition, boolean forward, long node, long[] neighbors, int degree) {
        if (degree <= 0 || degree > neighbors.length) {
            throw new IllegalArgumentException("degree " + degree + " does not fit " + neighbors.length + " neighbors");
        }
        state.tables(partition).fillBulkNode(forward, (int) node, neighbors, degree);
    }

    boolean isHighDegree(long u) {
        return state.direct().isHighDegree(u);
    }

    boolean hasIncoming(long v, long u) {
        return state.direct().containsIncoming(v, u);
    }

    private long writeSingle(RecordType type, int object, int relation, int subjectRelation, int subject,
                             boolean awaitDurable) {
        writer.lock();
        try {
            requireOpen();
            return writeSingleLocked(type, object, relation, subjectRelation, subject, awaitDurable);
        } finally {
            writer.unlock();
        }
    }

    private long writeSingleLocked(RecordType type, int object, int relation, int subjectRelation, int subject,
                                   boolean awaitDurable) {
        boolean add = type == RecordType.TUPLE_ADD;
        if (add == probeTuple(object, relation, subjectRelation, subject)) {
            return UNCHANGED;
        }
        scratch.clear();
        scratch.autocommitTuple(type, object, relation, subjectRelation, subject);
        if (add && !reservation.needsNoGrowth(object, relation, subjectRelation, subject)) {
            return commitLocked(scratch, awaitDurable);
        }
        long lsn = journal(scratch, awaitDurable);
        sequence.beginWrite();
        try {
            if (add) {
                applier.applyAbsentAddition(object, relation, subjectRelation, subject);
            } else {
                applier.applyTuple(false, object, relation, subjectRelation, subject);
            }
            state.appliedLsn(lsn);
        } catch (RuntimeException | Error e) {
            state.fault();
            throw e;
        } finally {
            sequence.endWrite();
        }
        return lsn;
    }

    private int writePairs(RecordType type, long[] pairs, int pairCount) {
        writer.lock();
        try {
            requireOpen();
            int changed = 0;
            long lastLsn = UNCHANGED;
            RuntimeException failure = null;
            try {
                if (type == RecordType.TUPLE_ADD) {
                    reserveNodes(pairs, pairCount);
                }
                for (int i = 0; i < pairCount; i++) {
                    long lsn = writeSingleLocked(type, (int) pairs[2 * i], 0, 0, (int) pairs[2 * i + 1], false);
                    if (lsn != UNCHANGED) {
                        changed++;
                        lastLsn = lsn;
                    }
                }
            } catch (RuntimeException e) {
                failure = e;
            }
            failure = awaitDurability(lastLsn, failure);
            if (failure != null) {
                throw failure;
            }
            return changed;
        } finally {
            writer.unlock();
        }
    }

    private void reserveNodes(long[] pairs, int pairCount) {
        long largest = -1;
        for (int i = 0; i < 2 * pairCount; i++) {
            largest = Math.max(largest, pairs[i]);
        }
        if (largest < 0) {
            return;
        }
        sequence.beginWrite();
        try {
            state.ensureNodes(largest + 1);
        } finally {
            sequence.endWrite();
        }
    }

    private RuntimeException awaitDurability(long lsn, RuntimeException failure) {
        if (lsn == UNCHANGED) {
            return failure;
        }
        try {
            log.awaitDurable(lsn);
            return failure;
        } catch (RuntimeException e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
            return failure;
        }
    }

    private long commitLocked(RecordBatch batch, boolean awaitDurable) {
        reservation.prepare(batch);
        sequence.beginWrite();
        try {
            reservation.reserve();
        } finally {
            sequence.endWrite();
        }
        long lsn = journal(batch, awaitDurable);
        sequence.beginWrite();
        try {
            applyBatch(batch);
            state.appliedLsn(lsn);
        } catch (RuntimeException | Error e) {
            state.fault();
            throw e;
        } finally {
            sequence.endWrite();
        }
        return lsn;
    }

    private long journal(RecordBatch batch, boolean awaitDurable) {
        long lsn = log.append(batch);
        if (awaitDurable) {
            log.awaitDurable(lsn);
        }
        return lsn;
    }

    private void applyBatch(RecordBatch batch) {
        applyReader.wrap(batch.bytes(), 0, batch.size());
        while (applyReader.hasRecord()) {
            applier.apply(applyReader);
            applyReader.advance();
        }
    }

    private void checkpointLocked() {
        log.force();
        storage.checkpoint(this, log);
    }

    private void closeStorage() {
        RuntimeException failure = null;
        try {
            checkpointLocked();
        } catch (RuntimeException e) {
            failure = e;
        }
        failure = closeQuietly(log::close, failure);
        failure = closeQuietly(storage::close, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeQuietly(Runnable action, RuntimeException failure) {
        try {
            action.run();
            return failure;
        } catch (RuntimeException e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
            return failure;
        }
    }

    private long neighborAt(boolean outgoing, long node, int index) {
        NodeIds.checkValid(node);
        while (true) {
            long started = sequence.beginRead();
            EdgeTables tables = state.direct();
            int degree = outgoing ? tables.degree(node) : tables.inDegree(node);
            long key = NodeIds.NONE;
            if (index >= 0 && index < degree) {
                key = outgoing ? tables.outgoingKeyAt(node, index) : tables.incomingKeyAt(node, index);
            }
            if (sequence.validate(started)) {
                Objects.checkIndex(index, degree);
                return key;
            }
        }
    }

    private int commonNeighborsOnce(long u, long v, long[] out) {
        EdgeTables tables = state.direct();
        boolean uIsSmaller = tables.degree(u) <= tables.degree(v);
        long smaller = uIsSmaller ? u : v;
        long larger = uIsSmaller ? v : u;
        int smallerDegree = tables.degree(smaller);
        if (out.length < smallerDegree) {
            return KHopTraversal.OVERFLOW;
        }
        int count = 0;
        for (int i = 0; i < smallerDegree; i++) {
            long candidate = tables.outgoingKeyAt(smaller, i);
            if (tables.contains(larger, candidate)) {
                if (count == out.length) {
                    return KHopTraversal.OVERFLOW;
                }
                out[count++] = candidate;
            }
        }
        return count;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("graph is closed");
        }
        if (state.faulted()) {
            throw new IllegalStateException(FAULTED_MESSAGE);
        }
    }

    private void requireReplica(String action) {
        if (!isReplica()) {
            throw new IllegalStateException("only a follower can " + action);
        }
    }

    private void requireDurable(String operation) {
        if (storage == null) {
            throw new IllegalStateException("graph is in memory only and has nothing to " + operation);
        }
    }

    private static void validatePairs(long[] pairs, int pairCount) {
        if (pairCount < 0 || 2L * pairCount > pairs.length) {
            throw new IllegalArgumentException(
                    "pair count " + pairCount + " does not fit a buffer of " + pairs.length + " longs");
        }
        for (int i = 0; i < 2 * pairCount; i++) {
            NodeIds.checkValid(pairs[i]);
        }
    }
}
