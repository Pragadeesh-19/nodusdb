package io.nodusdb.lake;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/*
 * Append-only byte output in native memory. Encoders write their pages here, and the file
 * writer reads them back as a segment without copying. reset keeps the buffer for reuse, so
 * a segment returned by segment() is valid until the next reset or append that grows it.
 */
final class NativeSink implements AutoCloseable {

    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final long INITIAL_BYTES = 1 << 12;

    private NativeColumn buffer;
    private long length;

    NativeSink() {
        this.buffer = new NativeColumn(INITIAL_BYTES);
    }

    void reset() {
        length = 0;
    }

    long length() {
        return length;
    }

    MemorySegment segment() {
        return buffer.segment().asSlice(0, length);
    }

    void put(int value) {
        reserve(1);
        buffer.segment().set(BYTE, length++, (byte) value);
    }

    void putLittleEndian(long value, int count) {
        reserve(count);
        for (int i = 0; i < count; i++) {
            buffer.segment().set(BYTE, length++, (byte) (value >>> (Byte.SIZE * i)));
        }
    }

    void putVarint(long value) {
        long remaining = value;
        while (remaining >= 0x80) {
            put((int) (remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        put((int) remaining);
    }

    void write(MemorySegment source, long from, long count) {
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
