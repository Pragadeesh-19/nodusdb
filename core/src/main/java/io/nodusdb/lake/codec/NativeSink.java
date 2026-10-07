package io.nodusdb.lake.codec;

import io.nodusdb.lake.memory.NativeColumn;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class NativeSink implements AutoCloseable {

    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final long INITIAL_BYTES = 1 << 12;

    private NativeColumn buffer;
    private long length;

    public NativeSink() {
        this.buffer = new NativeColumn(INITIAL_BYTES);
    }

    public void reset() {
        length = 0;
    }

    long length() {
        return length;
    }

    public MemorySegment segment() {
        return buffer.segment().asSlice(0, length);
    }

    public void put(int value) {
        reserve(1);
        buffer.segment().set(BYTE, length++, (byte) value);
    }

    public void putLittleEndian(long value, int count) {
        reserve(count);
        for (int i = 0; i < count; i++) {
            buffer.segment().set(BYTE, length++, (byte) (value >>> (Byte.SIZE * i)));
        }
    }

    public void putVarint(long value) {
        long remaining = value;
        while (remaining >= 0x80) {
            put((int) (remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        put((int) remaining);
    }

    public void write(MemorySegment source, long from, long count) {
        reserve(count);
        MemorySegment.copy(source, from, buffer.segment(), length, count);
        length += count;
    }

    @Override
    public void close() {
        buffer.close();
    }

    private void reserve(long extra) {
        buffer.ensureCapacity(length + extra);
    }
}
