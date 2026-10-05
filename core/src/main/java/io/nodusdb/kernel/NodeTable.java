package io.nodusdb.kernel;

import java.lang.foreign.Arena;

/*
 * One 64-bit slot per node. The high 32 bits hold the degree. The low 32 bits hold
 * either a slab block id or, when the top bit is set, a pool handle for a
 * high-degree node. Zero degree with no block is the empty slot.
 */
final class NodeTable {

    static final int NO_BLOCK = -1;

    private static final int SET_FLAG = Integer.MIN_VALUE;
    private static final int NO_BLOCK_FIELD = Integer.MAX_VALUE;
    private static final long EMPTY = pack(0, NO_BLOCK_FIELD);

    private final NativeLongArray slots;

    NodeTable(Arena arena, int initialNodes) {
        this.slots = new NativeLongArray(arena, initialNodes, EMPTY);
    }

    int capacity() {
        return slots.capacity();
    }

    void ensureCapacity(int nodeCount) {
        slots.ensureCapacity(nodeCount);
    }

    long read(int node) {
        return slots.get(node);
    }

    void write(int node, int degree, int block) {
        slots.set(node, pack(degree, block == NO_BLOCK ? NO_BLOCK_FIELD : block));
    }

    void writeSet(int node, int degree, int handle) {
        slots.set(node, pack(degree, SET_FLAG | handle));
    }

    static int degreeOf(long slot) {
        return (int) (slot >>> Integer.SIZE);
    }

    static boolean isSet(long slot) {
        return ((int) slot & SET_FLAG) != 0;
    }

    static int blockOf(long slot) {
        int field = (int) slot;
        return field == NO_BLOCK_FIELD ? NO_BLOCK : field;
    }

    static int handleOf(long slot) {
        return (int) slot & NO_BLOCK_FIELD;
    }

    private static long pack(int degree, int field) {
        return ((long) degree << Integer.SIZE) | (field & 0xFFFF_FFFFL);
    }
}
