package io.nodusdb.kernel;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Objects;

/*
 * A grow-only array of longs in native memory. Growth appends chunks and never
 * moves one, so a reader holding a segment stays valid while the arena is reachable.
 * Reads past the allocated range return the default value, so a reader racing a
 * writer never throws.
 */
final class NativeLongArray {

    static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final int ALIGNMENT = 64;

    private final Arena arena;
    private final ChunkLayout layout;
    private final long defaultValue;
    private MemorySegment[] chunks = new MemorySegment[0];
    private int chunkCount;
    private int capacity;

    NativeLongArray(Arena arena, int initialCapacity, long defaultValue) {
        this.arena = Objects.requireNonNull(arena, "arena");
        this.layout = new ChunkLayout(initialCapacity);
        this.defaultValue = defaultValue;
        appendChunk();
    }

    int capacity() {
        return capacity;
    }

    long get(int index) {
        if (index < 0) {
            return defaultValue;
        }
        MemorySegment[] segments = chunks;
        int chunk = layout.chunkOf(index);
        if (chunk >= segments.length) {
            return defaultValue;
        }
        return segments[chunk].getAtIndex(LONG, layout.offsetOf(index, chunk));
    }

    void set(int index, long value) {
        Objects.checkIndex(index, capacity);
        int chunk = layout.chunkOf(index);
        chunks[chunk].setAtIndex(LONG, layout.offsetOf(index, chunk), value);
    }

    void ensureCapacity(int size) {
        if (size > MAX_CAPACITY) {
            throw new IllegalArgumentException("native long array size " + size + " exceeds " + MAX_CAPACITY);
        }
        while (capacity < size) {
            appendChunk();
        }
    }

    private void appendChunk() {
        int elements = Math.min(layout.chunkSize(chunkCount), MAX_CAPACITY - capacity);
        MemorySegment segment = arena.allocate((long) elements * Long.BYTES, ALIGNMENT);
        if (defaultValue != 0) {
            for (int i = 0; i < elements; i++) {
                segment.setAtIndex(LONG, i, defaultValue);
            }
        }
        chunks = Arrays.copyOf(chunks, chunkCount + 1);
        chunks[chunkCount] = segment;
        capacity += elements;
        chunkCount++;
    }
}
