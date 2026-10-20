package io.nodusdb.lake.memory;

import io.nodusdb.kernel.memory.NativeMemory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public final class NativeColumn implements AutoCloseable {

    public static final long ALIGNMENT = 64;
    static final long MAX_BYTES = 1L << 40;

    private static final long LOOP_COPY_LIMIT = 64;
    private static final MemorySegment RELEASED = MemorySegment.ofArray(new byte[0]);

    private MemorySegment segment;
    private long address;
    private long size;
    private boolean closed;

    public NativeColumn(long bytes) {
        adopt(allocate(powerOfTwoAtLeast(bytes)));
    }

    public MemorySegment segment() {
        return segment;
    }

    public long capacity() {
        return size;
    }

    public void ensureCapacity(long bytes) {
        if (closed) {
            throw new IllegalStateException("native column is closed");
        }
        if (bytes > size) {
            grow(bytes);
        }
    }

    public long getLong(long offset) {
        return NativeMemory.getLong(segment, address, size, offset);
    }

    public void setLong(long offset, long value) {
        NativeMemory.setLong(segment, address, size, offset, value);
    }

    public long getLongAtIndex(int index) {
        return NativeMemory.getLong(segment, address, size, (long) index << 3);
    }

    public void setLongAtIndex(int index, long value) {
        NativeMemory.setLong(segment, address, size, (long) index << 3, value);
    }

    public int getIntAtIndex(int index) {
        return NativeMemory.getInt(segment, address, size, (long) index << 2);
    }

    public void setIntAtIndex(int index, int value) {
        NativeMemory.setInt(segment, address, size, (long) index << 2, value);
    }

    public void setIntLittleEndian(long offset, int value) {
        NativeMemory.setIntLittleEndian(segment, address, size, offset, value);
    }

    public void setLongLittleEndian(long offset, long value) {
        NativeMemory.setLongLittleEndian(segment, address, size, offset, value);
    }

    public byte getByte(long offset) {
        return NativeMemory.getByte(segment, address, size, offset);
    }

    public void setByte(long offset, byte value) {
        NativeMemory.setByte(segment, address, size, offset, value);
    }

    @Override
    public void close() {
        adopt(RELEASED);
        closed = true;
    }

    private void grow(long bytes) {
        long target = Math.max(size << 1, powerOfTwoAtLeast(bytes));
        MemorySegment grown = allocate(target);
        MemorySegment.copy(segment, 0, grown, 0, size);
        adopt(grown);
    }

    private void adopt(MemorySegment next) {
        segment = next;
        address = NativeMemory.address(next);
        size = next.byteSize();
    }

    private static MemorySegment allocate(long capacity) {
        return Arena.ofAuto().allocate(capacity, ALIGNMENT);
    }

    public static void copyBytes(MemorySegment source, long sourceOffset, MemorySegment destination, long destinationOffset,
                          long length) {
        if (length > LOOP_COPY_LIMIT || NativeMemory.usesPointers()) {
            MemorySegment.copy(source, sourceOffset, destination, destinationOffset, length);
            return;
        }
        long i = 0;
        for (; i + Long.BYTES <= length; i += Long.BYTES) {
            long value = NativeMemory.getLongUnaligned(source, sourceOffset + i);
            NativeMemory.setLongUnaligned(destination, destinationOffset + i, value);
        }
        for (; i < length; i++) {
            NativeMemory.setByte(destination, destinationOffset + i, NativeMemory.getByte(source, sourceOffset + i));
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
