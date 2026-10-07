package io.nodusdb.kernel.adjacency;

import io.nodusdb.kernel.NodeIds;
import io.nodusdb.kernel.memory.ChunkLayout;
import io.nodusdb.kernel.memory.MemoryBudget;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

public final class LowDegreeSlab {

    static final int BLOCK_SHIFT = 4;
    public static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;

    private static final int BLOCK_BYTES = BLOCK_SIZE * Long.BYTES;
    private static final int ALIGNMENT = 64;
    private static final int MAX_BLOCKS = 1 << 26;
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private final Arena arena;
    private final MemoryBudget budget;
    private final ChunkLayout layout;
    private MemorySegment[] words = new MemorySegment[0];
    private MemorySegment[] flags = new MemorySegment[0];
    private int chunkCount;
    private int capacity;
    private int nextFresh;
    private int freeHead;
    private int freeCount;

    public LowDegreeSlab(Arena arena, MemoryBudget budget, int initialBlocks) {
        this.arena = arena;
        this.budget = budget;
        this.layout = new ChunkLayout(initialBlocks);
    }

    public int allocateBlock() {
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

    public void freeBlock(int block) {
        if (block < 0 || block >= nextFresh || flag(block) != 1) {
            throw new IllegalStateException("block is not allocated: " + block);
        }
        setFlag(block, (byte) 0);
        set(block, 0, freeHead);
        freeHead = block + 1;
        freeCount++;
    }

    public long get(int block, int offset) {
        assert offset >= 0 && offset < BLOCK_SIZE : "offset out of block: " + offset;
        int chunk = layout.chunkOf(block);
        return words[chunk].getAtIndex(LONG, longIndex(block, chunk, offset));
    }

    public void set(int block, int offset, long value) {
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

    public void reserveBlock() {
        if (freeHead == 0) {
            ensureCapacity(nextFresh + 1);
        }
    }

    public boolean canAllocateBlock() {
        return freeHead != 0
                || nextFresh < capacity
                || (capacity < MAX_BLOCKS && budget.canCharge(bytesOfChunk(layout.chunkSize(chunkCount))));
    }

    public int allocatedBlocks() {
        return nextFresh - freeCount;
    }

    int blockCapacity() {
        return capacity;
    }

    private void ensureCapacity(int blocks) {
        if (capacity >= blocks) {
            return;
        }
        budget.charge(bytesToReach(blocks));
        while (capacity < blocks) {
            appendChunk();
        }
    }

    private long bytesToReach(int blocks) {
        long bytes = 0;
        long filled = capacity;
        for (int next = chunkCount; filled < blocks; next++) {
            checkBlockLimit(filled);
            int chunkBlocks = layout.chunkSize(next);
            bytes += bytesOfChunk(chunkBlocks);
            filled += chunkBlocks;
        }
        return bytes;
    }

    private static long bytesOfChunk(int chunkBlocks) {
        return (long) chunkBlocks * BLOCK_BYTES + chunkBlocks;
    }

    private static void checkBlockLimit(long filled) {
        if (filled >= MAX_BLOCKS) {
            throw new IllegalStateException("slab block limit reached: " + MAX_BLOCKS);
        }
    }

    private void appendChunk() {
        checkBlockLimit(capacity);
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
