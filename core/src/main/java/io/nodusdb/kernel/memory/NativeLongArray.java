package io.nodusdb.kernel.memory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;

public final class NativeLongArray {

    static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;

    private static final int ALIGNMENT = 64;

    private final Arena arena;
    private final MemoryBudget budget;
    private final ChunkLayout layout;
    private final long defaultValue;
    private Chunks chunks = Chunks.EMPTY;
    private int chunkCount;
    private int capacity;

    private record Chunks(MemorySegment[] segments, long[] addresses, long[] sizes) {

        static final Chunks EMPTY = new Chunks(new MemorySegment[0], new long[0], new long[0]);

        Chunks with(MemorySegment segment) {
            int count = segments.length;
            MemorySegment[] nextSegments = Arrays.copyOf(segments, count + 1);
            long[] nextAddresses = Arrays.copyOf(addresses, count + 1);
            long[] nextSizes = Arrays.copyOf(sizes, count + 1);
            nextSegments[count] = segment;
            nextAddresses[count] = NativeMemory.address(segment);
            nextSizes[count] = segment.byteSize();
            return new Chunks(nextSegments, nextAddresses, nextSizes);
        }
    }

    public NativeLongArray(Arena arena, MemoryBudget budget, int initialCapacity, long defaultValue) {
        this.arena = Objects.requireNonNull(arena, "arena");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.layout = new ChunkLayout(initialCapacity);
        this.defaultValue = defaultValue;
        growTo(1);
    }

    public int capacity() {
        return capacity;
    }

    public long get(int index) {
        if (index < 0) {
            return defaultValue;
        }
        Chunks current = chunks;
        int chunk = layout.chunkOf(index);
        if (chunk >= current.segments.length) {
            return defaultValue;
        }
        return NativeMemory.getLong(current.segments[chunk], current.addresses[chunk], current.sizes[chunk],
                (long) layout.offsetOf(index, chunk) << 3);
    }

    public void set(int index, long value) {
        Objects.checkIndex(index, capacity);
        Chunks current = chunks;
        int chunk = layout.chunkOf(index);
        NativeMemory.setLong(current.segments[chunk], current.addresses[chunk], current.sizes[chunk],
                (long) layout.offsetOf(index, chunk) << 3, value);
    }

    public void ensureCapacity(int size) {
        if (size > MAX_CAPACITY) {
            throw new IllegalArgumentException("native long array size " + size + " exceeds " + MAX_CAPACITY);
        }
        growTo(size);
    }

    public long bytesToReach(int size) {
        long bytes = 0;
        long filled = capacity;
        for (int next = chunkCount; filled < size; next++) {
            int elements = elementsOfChunk(next, filled);
            bytes += (long) elements * Long.BYTES;
            filled += elements;
        }
        return bytes;
    }

    private void growTo(int size) {
        if (capacity >= size) {
            return;
        }
        budget.charge(bytesToReach(size));
        while (capacity < size) {
            appendChunk();
        }
    }

    private void appendChunk() {
        int elements = elementsOfChunk(chunkCount, capacity);
        MemorySegment segment = arena.allocate((long) elements * Long.BYTES, ALIGNMENT);
        if (defaultValue != 0) {
            long address = NativeMemory.address(segment);
            long size = segment.byteSize();
            for (int i = 0; i < elements; i++) {
                NativeMemory.setLong(segment, address, size, (long) i << 3, defaultValue);
            }
        }
        chunks = chunks.with(segment);
        capacity += elements;
        chunkCount++;
    }

    private int elementsOfChunk(int chunk, long filled) {
        return (int) Math.min(layout.chunkSize(chunk), MAX_CAPACITY - filled);
    }
}
