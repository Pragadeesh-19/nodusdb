package io.nodusdb.lake;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/*
 * A growable byte buffer in native memory. Capacity is a power of two and the first byte
 * sits on a 64-byte cache line. Growth copies the live contents into a new arena and closes
 * the old one, so a segment read before a growth must not be used after it.
 *
 *   segment   [ ... contents ... | zeroed spare ]   byteSize() == capacity()
 */
final class NativeColumn implements AutoCloseable {

    static final long ALIGNMENT = 64;
    static final long MAX_BYTES = 1L << 40;

    private Arena arena;
    private MemorySegment segment;

    NativeColumn(long bytes) {
        long capacity = powerOfTwoAtLeast(bytes);
        arena = Arena.ofShared();
        segment = arena.allocate(capacity, ALIGNMENT);
    }

    MemorySegment segment() {
        return segment;
    }

    long capacity() {
        return segment.byteSize();
    }

    void ensureCapacity(long bytes) {
        if (bytes > segment.byteSize()) {
            grow(bytes);
        }
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }

    private void grow(long bytes) {
        long target = Math.max(segment.byteSize() << 1, powerOfTwoAtLeast(bytes));
        Arena next = Arena.ofShared();
        MemorySegment grown = next.allocate(target, ALIGNMENT);
        MemorySegment.copy(segment, 0, grown, 0, segment.byteSize());
        arena.close();
        arena = next;
        segment = grown;
    }

    private static long powerOfTwoAtLeast(long bytes) {
        if (bytes < 1 || bytes > MAX_BYTES) {
            throw new IllegalArgumentException("native column size must be in [1, 2^40]: " + bytes);
        }
        long capacity = ALIGNMENT;
        while (capacity < bytes) {
            capacity <<= 1;
        }
        return capacity;
    }
}
