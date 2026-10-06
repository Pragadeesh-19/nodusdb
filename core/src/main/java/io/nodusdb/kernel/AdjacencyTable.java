package io.nodusdb.kernel;

import java.lang.foreign.Arena;

final class AdjacencyTable {

    static final int MAX_LOW_DEGREE = 15;
    static final int PROMOTED_CAPACITY = 16;
    static final int DEMOTION_DEGREE = 8;

    private static final int INITIAL_NODES = 16;
    private static final long[] NO_NEIGHBORS = new long[0];

    private final LowDegreeSlab slab;
    private final NativeBlockPool pool;
    private final NativeSparseSet sets;
    private final NodeTable nodes;

    AdjacencyTable(Arena arena, MemoryBudget budget) {
        this.slab = new LowDegreeSlab(arena, budget, INITIAL_NODES);
        this.pool = new NativeBlockPool(arena, budget);
        this.sets = new NativeSparseSet(pool);
        this.nodes = new NodeTable(arena, budget, INITIAL_NODES);
    }

    int capacity() {
        return nodes.capacity();
    }

    void ensureCapacity(int nodeCount) {
        nodes.ensureCapacity(nodeCount);
    }

    long bytesToHold(int nodeCount) {
        return nodes.bytesToHold(nodeCount);
    }

    /*
     * Reserves what the next add to node may allocate, so a budget refusal surfaces
     * before the graph changes. It mirrors the three places add allocates.
     */
    void reserveForAdd(long node) {
        long slot = nodes.read(slotOf(node));
        int degree = NodeTable.degreeOf(slot);
        if (NodeTable.isSet(slot)) {
            int handle = NodeTable.handleOf(slot);
            if (degree == sets.capacityOf(handle)) {
                sets.reserve(2 * degree);
            }
        } else if (degree == MAX_LOW_DEGREE) {
            sets.reserve(PROMOTED_CAPACITY);
        } else if (NodeTable.blockOf(slot) == NodeTable.NO_BLOCK) {
            slab.reserveBlock();
        }
    }

    int degreeOf(long node) {
        return NodeTable.degreeOf(nodes.read(slotOf(node)));
    }

    boolean isHighDegree(long node) {
        return NodeTable.isSet(nodes.read(slotOf(node)));
    }

    boolean contains(long node, long neighbor) {
        long slot = nodes.read(slotOf(node));
        int degree = NodeTable.degreeOf(slot);
        if (NodeTable.isSet(slot)) {
            return sets.contains(NodeTable.handleOf(slot), degree, neighbor);
        }
        return slab.containsValue(NodeTable.blockOf(slot), degree, neighbor);
    }

    long neighborAt(long node, int i) {
        long slot = nodes.read(slotOf(node));
        if (NodeTable.isSet(slot)) {
            return sets.peek(NodeTable.handleOf(slot), NodeTable.degreeOf(slot), i);
        }
        return slab.peek(NodeTable.blockOf(slot), i);
    }

    /*
     * Returns the neighbors of node in positions [0, degree). The scratch array must
     * hold at least that many longs; a shorter scratch yields NO_NEIGHBORS, which a
     * concurrent reader treats as a race and retries.
     */
    long[] neighborsOf(long node, long[] scratch) {
        long slot = nodes.read(slotOf(node));
        int degree = NodeTable.degreeOf(slot);
        if (degree == 0 || scratch.length < degree) {
            return NO_NEIGHBORS;
        }
        if (NodeTable.isSet(slot)) {
            sets.copyKeys(NodeTable.handleOf(slot), degree, scratch);
            return scratch;
        }
        int block = NodeTable.blockOf(slot);
        if (block == NodeTable.NO_BLOCK) {
            return NO_NEIGHBORS;
        }
        for (int i = 0; i < degree; i++) {
            scratch[i] = slab.peek(block, i);
        }
        return scratch;
    }

    boolean add(long node, long neighbor) {
        if (contains(node, neighbor)) {
            return false;
        }
        int n = (int) node;
        long slot = nodes.read(n);
        int degree = NodeTable.degreeOf(slot);
        if (NodeTable.isSet(slot)) {
            int handle = NodeTable.handleOf(slot);
            if (degree == sets.capacityOf(handle)) {
                handle = sets.grow(handle, degree);
            }
            sets.append(handle, degree, neighbor);
            nodes.writeSet(n, degree + 1, handle);
            return true;
        }
        int block = NodeTable.blockOf(slot);
        if (degree == MAX_LOW_DEGREE) {
            nodes.writeSet(n, degree + 1, promote(block, neighbor));
            return true;
        }
        if (block == NodeTable.NO_BLOCK) {
            block = slab.allocateBlock();
        }
        slab.set(block, degree, neighbor);
        nodes.write(n, degree + 1, block);
        return true;
    }

    boolean remove(long node, long neighbor) {
        int n = slotOf(node);
        long slot = nodes.read(n);
        int degree = NodeTable.degreeOf(slot);
        if (NodeTable.isSet(slot)) {
            int handle = NodeTable.handleOf(slot);
            if (!sets.contains(handle, degree, neighbor)) {
                return false;
            }
            sets.remove(handle, degree, neighbor);
            int remaining = degree - 1;
            if (remaining == DEMOTION_DEGREE && slab.canAllocateBlock()) {
                demote(n, handle, remaining);
            } else {
                nodes.writeSet(n, remaining, handle);
            }
            return true;
        }
        int block = NodeTable.blockOf(slot);
        for (int i = 0; i < degree; i++) {
            if (slab.get(block, i) == neighbor) {
                slab.set(block, i, slab.get(block, degree - 1));
                if (degree == 1) {
                    slab.freeBlock(block);
                    block = NodeTable.NO_BLOCK;
                }
                nodes.write(n, degree - 1, block);
                return true;
            }
        }
        return false;
    }

    void prepareBulkLoad(int[] nodeDegrees) {
        int lowNodes = 0;
        for (int degree : nodeDegrees) {
            if (degree > 0 && degree <= MAX_LOW_DEGREE) {
                lowNodes++;
            }
        }
        slab.reserve(lowNodes);
        for (int n = 0; n < nodeDegrees.length; n++) {
            int degree = nodeDegrees[n];
            if (degree > MAX_LOW_DEGREE) {
                nodes.writeSet(n, degree, sets.allocate(Integer.highestOneBit(degree - 1) << 1));
            } else {
                int block = degree > 0 ? slab.allocateBlock() : NodeTable.NO_BLOCK;
                nodes.write(n, degree, block);
            }
        }
    }

    void fillBulkNode(int n, long[] neighbors, int degree) {
        long slot = nodes.read(n);
        assert NodeTable.degreeOf(slot) == degree : "prepared degree differs for node " + n;
        if (degree <= MAX_LOW_DEGREE) {
            int block = NodeTable.blockOf(slot);
            for (int i = 0; i < degree; i++) {
                slab.set(block, i, neighbors[i]);
            }
            return;
        }
        sets.fill(NodeTable.handleOf(slot), neighbors, degree);
    }

    private int promote(int block, long neighbor) {
        int handle = sets.allocate(PROMOTED_CAPACITY);
        for (int i = 0; i < MAX_LOW_DEGREE; i++) {
            sets.append(handle, i, slab.get(block, i));
        }
        sets.append(handle, MAX_LOW_DEGREE, neighbor);
        slab.freeBlock(block);
        return handle;
    }

    private void demote(int n, int handle, int degree) {
        int block = slab.allocateBlock();
        for (int i = 0; i < DEMOTION_DEGREE; i++) {
            slab.set(block, i, sets.peek(handle, degree, i));
        }
        sets.release(handle);
        nodes.write(n, degree, block);
    }

    private static int slotOf(long node) {
        return node >= 0 && node < Integer.MAX_VALUE ? (int) node : -1;
    }
}
