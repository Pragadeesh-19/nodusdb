package io.nodusdb.kernel;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/*
 * Fixed-size neighbor blocks in native memory. A block is 16 longs (128 bytes),
 * and every block starts on a 64-byte boundary.
 *
 * Blocks live in chunks. With first = the first chunk's size (a power of two):
 *
 *   chunk 0   blocks [0, first)               size first
 *   chunk 1   blocks [first, 2*first)         size first
 *   chunk 2   blocks [2*first, 4*first)       size 2*first
 *   chunk 3   blocks [4*first, 8*first)       size 4*first
 *
 * Growth appends a chunk and never moves an existing one, so a reader that holds
 * a segment stays valid until the arena closes. Free blocks form an intrusive
 * stack: a freed block's first long holds the next free block id plus one, and
 * zero marks the end. Allocation flags live in a parallel byte per block.
 */
final class LowDegreeSlab {

    static final int BLOCK_SHIFT = 4;
    static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;

    private static final int BLOCK_BYTES = BLOCK_SIZE * Long.BYTES;
    private static final int ALIGNMENT = 64;
    private static final int MAX_BLOCKS = 1 << 26;
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private final Arena arena;
    private final ChunkLayout layout;
    private MemorySegment[] words = new MemorySegment[0];
    private MemorySegment[] flags = new MemorySegment[0];
    private int chunkCount;
    private int capacity;
    private int nextFresh;
    private int freeHead;
    private int freeCount;

    LowDegreeSlab(Arena arena, int initialBlocks) {
        this.arena = arena;
        this.layout = new ChunkLayout(initialBlocks);
    }

    int allocateBlock() {
        int block;
        if (freeHead != 0) {
            block = freeHead - 1;
            freeHead = (int) get(block, 0);
            freeCount--;
        } else {
            block = nextFresh;
            ensureCapacity(block + 1);
            nextFresh++;
        }
        setFlag(block, (byte) 1);
        return block;
    }

    void freeBlock(int block) {
        if (block < 0 || block >= nextFresh || flag(block) != 1) {
            throw new IllegalStateException("block is not allocated: " + block);
        }
        setFlag(block, (byte) 0);
        set(block, 0, freeHead);
        freeHead = block + 1;
        freeCount++;
    }

    long get(int block, int offset) {
        assert offset >= 0 && offset < BLOCK_SIZE : "offset out of block: " + offset;
        int chunk = layout.chunkOf(block);
        return words[chunk].getAtIndex(LONG, longIndex(block, chunk, offset));
    }

    void set(int block, int offset, long value) {
        assert offset >= 0 && offset < BLOCK_SIZE : "offset out of block: " + offset;
        int chunk = layout.chunkOf(block);
        words[chunk].setAtIndex(LONG, longIndex(block, chunk, offset), value);
    }

    boolean containsValue(int block, int count, long value) {
        if (block < 0) {
            return false;
        }
        MemorySegment[] segments = words;
        int chunk = layout.chunkOf(block);
        if (chunk >= segments.length) {
            return false;
        }
        MemorySegment segment = segments[chunk];
        long base = longIndex(block, chunk, 0);
        int limit = Math.min(count, BLOCK_SIZE);
        for (int i = 0; i < limit; i++) {
            if (segment.getAtIndex(LONG, base + i) == value) {
                return true;
            }
        }
        return false;
    }

    long peek(int block, int offset) {
        if (block < 0 || offset < 0 || offset >= BLOCK_SIZE) {
            return NodeIds.NONE;
        }
        MemorySegment[] segments = words;
        int chunk = layout.chunkOf(block);
        return chunk < segments.length
                ? segments[chunk].getAtIndex(LONG, longIndex(block, chunk, offset))
                : NodeIds.NONE;
    }

    long addressOf(int block) {
        int chunk = layout.chunkOf(block);
        return words[chunk].address() + (long) layout.offsetOf(block, chunk) * BLOCK_BYTES;
    }

    void reserve(int blocks) {
        ensureCapacity(blocks);
    }

    int allocatedBlocks() {
        return nextFresh - freeCount;
    }

    int blockCapacity() {
        return capacity;
    }

    private void ensureCapacity(int blocks) {
        while (capacity < blocks) {
            appendChunk();
        }
    }

    private void appendChunk() {
        if (capacity >= MAX_BLOCKS) {
            throw new IllegalStateException("slab block limit reached: " + MAX_BLOCKS);
        }
        int blocks = layout.chunkSize(chunkCount);
        words = Arrays.copyOf(words, chunkCount + 1);
        flags = Arrays.copyOf(flags, chunkCount + 1);
        words[chunkCount] = arena.allocate((long) blocks * BLOCK_BYTES, ALIGNMENT);
        flags[chunkCount] = arena.allocate(blocks, 1);
        capacity += blocks;
        chunkCount++;
    }

    private long longIndex(int block, int chunk, int offset) {
        return (long) layout.offsetOf(block, chunk) * BLOCK_SIZE + offset;
    }

    private byte flag(int block) {
        int chunk = layout.chunkOf(block);
        return flags[chunk].get(BYTE, layout.offsetOf(block, chunk));
    }

    private void setFlag(int block, byte value) {
        int chunk = layout.chunkOf(block);
        flags[chunk].set(BYTE, layout.offsetOf(block, chunk), value);
    }
}
