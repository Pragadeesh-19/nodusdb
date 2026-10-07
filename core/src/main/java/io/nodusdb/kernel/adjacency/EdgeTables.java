package io.nodusdb.kernel.adjacency;

import io.nodusdb.kernel.memory.MemoryBudget;

import java.lang.foreign.Arena;

public final class EdgeTables {

    private final AdjacencyTable outgoing;
    private final AdjacencyTable incoming;

    public EdgeTables(Arena arena, MemoryBudget budget) {
        this.outgoing = new AdjacencyTable(arena, budget);
        this.incoming = new AdjacencyTable(arena, budget);
    }

    public int capacity() {
        return Math.min(outgoing.capacity(), incoming.capacity());
    }

    public long bytesToHold(int nodes) {
        return outgoing.bytesToHold(nodes) + incoming.bytesToHold(nodes);
    }

    public void ensureCapacity(int nodes) {
        outgoing.ensureCapacity(nodes);
        incoming.ensureCapacity(nodes);
    }

    public boolean contains(long object, long key) {
        return outgoing.contains(object, key);
    }

    public boolean containsIncoming(long subject, long key) {
        return incoming.contains(subject, key);
    }

    public int degree(long node) {
        return outgoing.degreeOf(node);
    }

    public int inDegree(long node) {
        return incoming.degreeOf(node);
    }

    public long outgoingKeyAt(long node, int index) {
        return outgoing.neighborAt(node, index);
    }

    public long incomingKeyAt(long node, int index) {
        return incoming.neighborAt(node, index);
    }

    public boolean isHighDegree(long node) {
        return outgoing.isHighDegree(node);
    }

    public boolean add(long object, long objectKey, long subject, long subjectKey) {
        boolean added = outgoing.add(object, objectKey);
        if (added) {
            boolean mirrored = incoming.add(subject, subjectKey);
            assert mirrored : "outgoing and incoming tables diverged on add";
        }
        return added;
    }

    public boolean remove(long object, long objectKey, long subject, long subjectKey) {
        boolean removed = outgoing.remove(object, objectKey);
        if (removed) {
            boolean mirrored = incoming.remove(subject, subjectKey);
            assert mirrored : "outgoing and incoming tables diverged on remove";
        }
        return removed;
    }

    public void accumulateOutgoing(long node, int additions, Headroom headroom) {
        outgoing.accumulateHeadroom(node, additions, headroom);
    }

    public void accumulateIncoming(long node, int additions, Headroom headroom) {
        incoming.accumulateHeadroom(node, additions, headroom);
    }

    public long bytesToReserve(Headroom outgoingHeadroom, Headroom incomingHeadroom) {
        return outgoing.bytesToReserve(outgoingHeadroom) + incoming.bytesToReserve(incomingHeadroom);
    }

    public void reserve(Headroom outgoingHeadroom, Headroom incomingHeadroom) {
        outgoing.reserve(outgoingHeadroom);
        incoming.reserve(incomingHeadroom);
    }

    public long[] outgoingKeysOf(long node, long[] scratch) {
        return outgoing.neighborsOf(node, scratch);
    }

    public AdjacencyTable outgoingTable() {
        return outgoing;
    }

    public void prepareBulkLoad(int[] forwardDegrees, int[] backwardDegrees) {
        outgoing.prepareBulkLoad(forwardDegrees);
        incoming.prepareBulkLoad(backwardDegrees);
    }

    public void fillBulkNode(boolean forward, int node, long[] keys, int degree) {
        (forward ? outgoing : incoming).fillBulkNode(node, keys, degree);
    }
}
