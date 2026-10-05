package io.nodusdb.kernel;

import java.lang.foreign.Arena;

/*
 * One 64-bit slot per node: the degree in the high 32 bits and the neighbor block
 * id in the low 32 bits. Reading both fields costs one native access.
 */
final class NodeTable {

    static final int NO_BLOCK = -1;

    private static final long EMPTY = pack(0, NO_BLOCK);

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
        slots.set(node, pack(degree, block));
    }

    static int degreeOf(long slot) {
        return (int) (slot >>> Integer.SIZE);
    }

    static int blockOf(long slot) {
        return (int) slot;
    }

    private static long pack(int degree, int block) {
        return ((long) degree << Integer.SIZE) | (block & 0xFFFF_FFFFL);
    }
}
