package io.nodusdb.lake;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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

    private static final ValueLayout.OfLong UNALIGNED_LONG = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final long LOOP_COPY_LIMIT = 64;

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

    static void copyBytes(MemorySegment source, long sourceOffset, MemorySegment destination, long destinationOffset,
                          long length) {
        if (length > LOOP_COPY_LIMIT) {
            MemorySegment.copy(source, sourceOffset, destination, destinationOffset, length);
            return;
        }
        long i = 0;
        for (; i + Long.BYTES <= length; i += Long.BYTES) {
            destination.set(UNALIGNED_LONG, destinationOffset + i, source.get(UNALIGNED_LONG, sourceOffset + i));
        }
        for (; i < length; i++) {
            destination.set(BYTE, destinationOffset + i, source.get(BYTE, sourceOffset + i));
        }
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
