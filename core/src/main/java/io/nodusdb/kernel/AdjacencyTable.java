package io.nodusdb.kernel;

import java.util.Arrays;

final class AdjacencyTable {

    static final int MAX_LOW_DEGREE = 15;
    static final int PROMOTED_CAPACITY = 16;
    static final int DEMOTION_DEGREE = 8;

    private static final int NO_BLOCK = -1;
    private static final int INITIAL_NODES = 16;
    private static final long MAX_NODE_COUNT = NodeIds.MAX_NODE_ID + 1;
    private static final long[] NO_NEIGHBORS = new long[0];

    private final LowDegreeSlab slab = new LowDegreeSlab();
    private int[] degrees = new int[INITIAL_NODES];
    private int[] blocks = noBlocks(INITIAL_NODES);
    private IndexedSparseSet[] sets = new IndexedSparseSet[INITIAL_NODES];

    int capacity() {
        return degrees.length;
    }

    void ensureCapacity(int nodeCount) {
        int old = degrees.length;
        if (nodeCount <= old) {
            return;
        }
        int newCapacity = (int) Math.max(nodeCount, Math.min((long) old << 1, MAX_NODE_COUNT));
        degrees = Arrays.copyOf(degrees, newCapacity);
        blocks = Arrays.copyOf(blocks, newCapacity);
        Arrays.fill(blocks, old, newCapacity, NO_BLOCK);
        sets = Arrays.copyOf(sets, newCapacity);
    }

    int degreeOf(long node) {
        int[] nodeDegrees = degrees;
        return node < nodeDegrees.length ? nodeDegrees[(int) node] : 0;
    }

    boolean isHighDegree(long node) {
        IndexedSparseSet[] nodeSets = sets;
        return node < nodeSets.length && nodeSets[(int) node] != null;
    }

    boolean contains(long node, long neighbor) {
        int[] nodeDegrees = degrees;
        if (node >= nodeDegrees.length) {
            return false;
        }
        int n = (int) node;
        IndexedSparseSet[] nodeSets = sets;
        if (n >= nodeSets.length) {
            return false;
        }
        IndexedSparseSet set = nodeSets[n];
        if (set != null) {
            return set.contains(neighbor);
        }
        int[] nodeBlocks = blocks;
        return n < nodeBlocks.length && slab.containsValue(nodeBlocks[n], nodeDegrees[n], neighbor);
    }

    long neighborAt(long node, int i) {
        IndexedSparseSet[] nodeSets = sets;
        if (node >= nodeSets.length) {
            return NodeIds.NONE;
        }
        int n = (int) node;
        IndexedSparseSet set = nodeSets[n];
        if (set != null) {
            return set.peek(i);
        }
        int[] nodeBlocks = blocks;
        return n < nodeBlocks.length ? slab.peek(nodeBlocks[n], i) : NodeIds.NONE;
    }

    long[] neighborArray(long node) {
        IndexedSparseSet[] nodeSets = sets;
        if (node >= nodeSets.length) {
            return NO_NEIGHBORS;
        }
        IndexedSparseSet set = nodeSets[(int) node];
        return set != null ? set.denseArray() : slab.slots();
    }

    int neighborBase(long node) {
        IndexedSparseSet[] nodeSets = sets;
        int[] nodeBlocks = blocks;
        if (node >= nodeSets.length || node >= nodeBlocks.length) {
            return 0;
        }
        int n = (int) node;
        return nodeSets[n] == null && nodeBlocks[n] != NO_BLOCK ? slab.blockBase(nodeBlocks[n]) : 0;
    }

    boolean add(long node, long neighbor) {
        if (contains(node, neighbor)) {
            return false;
        }
        int n = (int) node;
        int degree = degrees[n];
        IndexedSparseSet set = sets[n];
        if (set != null) {
            set.appendAbsent(neighbor);
        } else if (degree == MAX_LOW_DEGREE) {
            promote(n, neighbor);
        } else {
            if (blocks[n] == NO_BLOCK) {
                blocks[n] = slab.allocateBlock();
            }
            slab.set(blocks[n], degree, neighbor);
        }
        degrees[n] = degree + 1;
        return true;
    }

    boolean remove(long node, long neighbor) {
        if (node >= degrees.length) {
            return false;
        }
        int n = (int) node;
        IndexedSparseSet set = sets[n];
        if (set != null) {
            if (!set.remove(neighbor)) {
                return false;
            }
            degrees[n]--;
            if (degrees[n] == DEMOTION_DEGREE) {
                demote(n);
            }
            return true;
        }
        int degree = degrees[n];
        int block = blocks[n];
        for (int i = 0; i < degree; i++) {
            if (slab.get(block, i) == neighbor) {
                slab.set(block, i, slab.get(block, degree - 1));
                degrees[n] = degree - 1;
                if (degree == 1) {
                    slab.freeBlock(block);
                    blocks[n] = NO_BLOCK;
                }
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
            degrees[n] = degree;
            if (degree > 0 && degree <= MAX_LOW_DEGREE) {
                blocks[n] = slab.allocateBlock();
            }
        }
    }

    void fillBulkNode(int n, long[] neighbors, int degree) {
        assert degrees[n] == degree : "prepared degree differs for node " + n;
        if (degree <= MAX_LOW_DEGREE) {
            int block = blocks[n];
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

    private void promote(int n, long neighbor) {
        IndexedSparseSet set = new IndexedSparseSet(PROMOTED_CAPACITY);
        int block = blocks[n];
        for (int i = 0; i < MAX_LOW_DEGREE; i++) {
            set.appendAbsent(slab.get(block, i));
        }
        set.appendAbsent(neighbor);
        slab.freeBlock(block);
        blocks[n] = NO_BLOCK;
        sets[n] = set;
    }

    private void demote(int n) {
        IndexedSparseSet set = sets[n];
        int block = slab.allocateBlock();
        for (int i = 0; i < DEMOTION_DEGREE; i++) {
            slab.set(block, i, set.get(i));
        }
        sets[n] = null;
        blocks[n] = block;
    }

    private static int[] noBlocks(int length) {
        int[] result = new int[length];
        Arrays.fill(result, NO_BLOCK);
        return result;
    }
}
