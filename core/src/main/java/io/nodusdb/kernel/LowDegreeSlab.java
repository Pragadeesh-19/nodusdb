package io.nodusdb.kernel;

import java.util.Arrays;

final class LowDegreeSlab {

    static final int BLOCK_SHIFT = 4;
    static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;

    private static final int INITIAL_BLOCKS = 16;
    private static final int MAX_BLOCKS = 1 << 26;

    private long[] slots;
    private int[] freeBlocks;
    private boolean[] allocated;
    private int blockCapacity;
    private int nextFresh;
    private int freeTop;

    LowDegreeSlab() {
        this(INITIAL_BLOCKS);
    }

    LowDegreeSlab(int initialBlocks) {
        this.blockCapacity = initialBlocks;
        this.slots = new long[initialBlocks << BLOCK_SHIFT];
        this.freeBlocks = new int[initialBlocks];
        this.allocated = new boolean[initialBlocks];
    }

    int allocateBlock() {
        int block;
        if (freeTop > 0) {
            block = freeBlocks[--freeTop];
        } else {
            if (nextFresh == blockCapacity) {
                grow();
            }
            block = nextFresh++;
        }
        allocated[block] = true;
        return block;
    }

    void freeBlock(int block) {
        if (block < 0 || block >= nextFresh || !allocated[block]) {
            throw new IllegalStateException("block is not allocated: " + block);
        }
        allocated[block] = false;
        freeBlocks[freeTop++] = block;
    }

    long get(int block, int offset) {
        assert offset >= 0 && offset < BLOCK_SIZE : "offset out of block: " + offset;
        return slots[(block << BLOCK_SHIFT) | offset];
    }

    void set(int block, int offset, long value) {
        assert offset >= 0 && offset < BLOCK_SIZE : "offset out of block: " + offset;
        slots[(block << BLOCK_SHIFT) | offset] = value;
    }

    long[] slots() {
        return slots;
    }

    boolean containsValue(int block, int count, long value) {
        if (block < 0) {
            return false;
        }
        long[] table = slots;
        int base = block << BLOCK_SHIFT;
        int limit = Math.min(count, BLOCK_SIZE);
        for (int i = 0; i < limit; i++) {
            int index = base + i;
            if (index >= table.length) {
                return false;
            }
            if (table[index] == value) {
                return true;
            }
        }
        return false;
    }

    long peek(int block, int offset) {
        if (block < 0 || offset < 0 || offset >= BLOCK_SIZE) {
            return NodeIds.NONE;
        }
        long[] table = slots;
        int index = (block << BLOCK_SHIFT) | offset;
        return index < table.length ? table[index] : NodeIds.NONE;
    }

    int blockBase(int block) {
        return block << BLOCK_SHIFT;
    }

    int allocatedBlocks() {
        return nextFresh - freeTop;
    }

    int blockCapacity() {
        return blockCapacity;
    }

    private void grow() {
        if (blockCapacity >= MAX_BLOCKS) {
            throw new IllegalStateException("slab block limit reached: " + MAX_BLOCKS);
        }
        int newCapacity = blockCapacity << 1;
        slots = Arrays.copyOf(slots, newCapacity << BLOCK_SHIFT);
        freeBlocks = Arrays.copyOf(freeBlocks, newCapacity);
        allocated = Arrays.copyOf(allocated, newCapacity);
        blockCapacity = newCapacity;
    }
}
