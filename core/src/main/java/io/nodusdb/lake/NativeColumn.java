package io.nodusdb.lake;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/*
 * A growable byte buffer in native memory. Capacity is a power of two and the first byte
 * sits on a 64-byte cache line. Growth copies the live contents into a new buffer, so a
 * segment read before a growth must not be used after it.
 *
 * Memory comes from GC-managed arenas and is reclaimed once no segment refers to it. Shared
 * arenas cannot be used because GraalVM native image does not support closing them. close()
 * drops the segment, so any later access fails instead of reading freed memory.
 *
 *   segment   [ ... contents ... | zeroed spare ]   byteSize() == capacity()
 */
final class NativeColumn implements AutoCloseable {

    static final long ALIGNMENT = 64;
    static final long MAX_BYTES = 1L << 40;

    private static final ValueLayout.OfLong UNALIGNED_LONG = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final long LOOP_COPY_LIMIT = 64;
    private static final MemorySegment RELEASED = MemorySegment.ofArray(new byte[0]);

    private MemorySegment segment;
    private boolean closed;

    NativeColumn(long bytes) {
        segment = allocate(powerOfTwoAtLeast(bytes));
    }

    MemorySegment segment() {
        return segment;
    }

    long capacity() {
        return segment.byteSize();
    }

    void ensureCapacity(long bytes) {
        if (closed) {
            throw new IllegalStateException("native column is closed");
        }
        if (bytes > segment.byteSize()) {
            grow(bytes);
        }
    }

    @Override
    public void close() {
        segment = RELEASED;
        closed = true;
    }

    private void grow(long bytes) {
        long target = Math.max(segment.byteSize() << 1, powerOfTwoAtLeast(bytes));
        MemorySegment grown = allocate(target);
        MemorySegment.copy(segment, 0, grown, 0, segment.byteSize());
        segment = grown;
    }

    private static MemorySegment allocate(long capacity) {
        return Arena.ofAuto().allocate(capacity, ALIGNMENT);
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
