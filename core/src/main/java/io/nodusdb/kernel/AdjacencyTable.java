package io.nodusdb.kernel;

import java.lang.foreign.Arena;
import java.util.Arrays;

final class AdjacencyTable {

    static final int MAX_LOW_DEGREE = 15;
    static final int PROMOTED_CAPACITY = 16;
    static final int DEMOTION_DEGREE = 8;

    private static final int INITIAL_NODES = 16;
    private static final long[] NO_NEIGHBORS = new long[0];

    private final LowDegreeSlab slab;
    private final NodeTable nodes;
    private IndexedSparseSet[] sets = new IndexedSparseSet[INITIAL_NODES];

    AdjacencyTable(Arena arena) {
        this.slab = new LowDegreeSlab(arena, INITIAL_NODES);
        this.nodes = new NodeTable(arena, INITIAL_NODES);
    }

    int capacity() {
        return nodes.capacity();
    }

    void ensureCapacity(int nodeCount) {
        if (nodeCount <= capacity()) {
            return;
        }
        nodes.ensureCapacity(nodeCount);
        sets = Arrays.copyOf(sets, nodes.capacity());
    }

    int degreeOf(long node) {
        return NodeTable.degreeOf(nodes.read(slotOf(node)));
    }

    boolean isHighDegree(long node) {
        int n = slotOf(node);
        IndexedSparseSet[] nodeSets = sets;
        return n >= 0 && n < nodeSets.length && nodeSets[n] != null;
    }

    boolean contains(long node, long neighbor) {
        int n = slotOf(node);
        IndexedSparseSet[] nodeSets = sets;
        if (n < 0 || n >= nodeSets.length) {
            return false;
        }
        IndexedSparseSet set = nodeSets[n];
        if (set != null) {
            return set.contains(neighbor);
        }
        long slot = nodes.read(n);
        return slab.containsValue(NodeTable.blockOf(slot), NodeTable.degreeOf(slot), neighbor);
    }

    long neighborAt(long node, int i) {
        int n = slotOf(node);
        IndexedSparseSet[] nodeSets = sets;
        if (n < 0 || n >= nodeSets.length) {
            return NodeIds.NONE;
        }
        IndexedSparseSet set = nodeSets[n];
        if (set != null) {
            return set.peek(i);
        }
        return slab.peek(NodeTable.blockOf(nodes.read(n)), i);
    }

    /*
     * Returns the neighbors of node in positions [0, degree). A high-degree node
     * returns its dense array. A low-degree node copies into scratch, which must
     * hold at least MAX_LOW_DEGREE longs, and returns scratch.
     */
    long[] neighborsOf(long node, long[] scratch) {
        int n = slotOf(node);
        IndexedSparseSet[] nodeSets = sets;
        if (n < 0 || n >= nodeSets.length) {
            return NO_NEIGHBORS;
        }
        IndexedSparseSet set = nodeSets[n];
        if (set != null) {
            return set.denseArray();
        }
        long slot = nodes.read(n);
        int block = NodeTable.blockOf(slot);
        if (block == NodeTable.NO_BLOCK) {
            return NO_NEIGHBORS;
        }
        int degree = NodeTable.degreeOf(slot);
        for (int i = 0; i < degree && i < MAX_LOW_DEGREE; i++) {
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
        int block = NodeTable.blockOf(slot);
        IndexedSparseSet set = sets[n];
        if (set != null) {
            set.appendAbsent(neighbor);
        } else if (degree == MAX_LOW_DEGREE) {
            promote(n, block, neighbor);
            block = NodeTable.NO_BLOCK;
        } else {
            if (block == NodeTable.NO_BLOCK) {
                block = slab.allocateBlock();
            }
            slab.set(block, degree, neighbor);
        }
        nodes.write(n, degree + 1, block);
        return true;
    }

    boolean remove(long node, long neighbor) {
        int n = slotOf(node);
        if (n < 0 || n >= sets.length) {
            return false;
        }
        long slot = nodes.read(n);
        int degree = NodeTable.degreeOf(slot);
        int block = NodeTable.blockOf(slot);
        IndexedSparseSet set = sets[n];
        if (set != null) {
            if (!set.remove(neighbor)) {
                return false;
            }
            int remaining = degree - 1;
            if (remaining == DEMOTION_DEGREE) {
                demote(n, remaining);
            } else {
                nodes.write(n, remaining, NodeTable.NO_BLOCK);
            }
            return true;
        }
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
            int block = degree > 0 && degree <= MAX_LOW_DEGREE ? slab.allocateBlock() : NodeTable.NO_BLOCK;
            nodes.write(n, degree, block);
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
        } else {
            IndexedSparseSet set = new IndexedSparseSet(Integer.highestOneBit(degree - 1) << 1);
            for (int i = 0; i < degree; i++) {
                set.appendAbsent(neighbors[i]);
            }
            sets[n] = set;
        }
    }

    private void promote(int n, int block, long neighbor) {
        IndexedSparseSet set = new IndexedSparseSet(PROMOTED_CAPACITY);
        for (int i = 0; i < MAX_LOW_DEGREE; i++) {
            set.appendAbsent(slab.get(block, i));
        }
        set.appendAbsent(neighbor);
        slab.freeBlock(block);
        sets[n] = set;
    }

    private void demote(int n, int degree) {
        IndexedSparseSet set = sets[n];
        int block = slab.allocateBlock();
        for (int i = 0; i < DEMOTION_DEGREE; i++) {
            slab.set(block, i, set.get(i));
        }
        sets[n] = null;
        nodes.write(n, degree, block);
    }

    private static int slotOf(long node) {
        return node >= 0 && node < Integer.MAX_VALUE ? (int) node : -1;
    }
}
