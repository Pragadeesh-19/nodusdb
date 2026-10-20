package io.nodusdb.lake.codec;

import io.nodusdb.lake.memory.NativeColumn;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

public final class NativeSink implements AutoCloseable {

    private static final long INITIAL_BYTES = 1 << 12;
    private static final ValueLayout.OfInt INT_LITTLE_ENDIAN =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

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
        buffer.setByte(length++, (byte) value);
    }

    public void putLittleEndian(long value, int count) {
        reserve(count);
        if (count == Integer.BYTES) {
            buffer.setIntLittleEndian(length, (int) value);
            length += Integer.BYTES;
            return;
        }
        if (count == Long.BYTES) {
            buffer.setLongLittleEndian(length, value);
            length += Long.BYTES;
            return;
        }
        for (int i = 0; i < count; i++) {
            buffer.setByte(length++, (byte) (value >>> (Byte.SIZE * i)));
        }
    }

    public void putIntsLittleEndian(int[] values, int count) {
        long bytes = (long) count * Integer.BYTES;
        reserve(bytes);
        MemorySegment.copy(values, 0, buffer.segment(), INT_LITTLE_ENDIAN, length, count);
        length += bytes;
    }

    public void putBytes(byte[] values, int count) {
        reserve(count);
        MemorySegment.copy(values, 0, buffer.segment(), ValueLayout.JAVA_BYTE, length, count);
        length += count;
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
