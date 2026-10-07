package io.nodusdb.avro;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class AvroEncoder {

    private byte[] buffer = new byte[256];
    private int length;

    public AvroEncoder writeBoolean(boolean value) {
        ensure(1);
        buffer[length++] = (byte) (value ? 1 : 0);
        return this;
    }

    public AvroEncoder writeInt(int value) {
        return writeLong(value);
    }

    public AvroEncoder writeLong(long value) {
        long remaining = (value << 1) ^ (value >> 63);
        ensure(Long.BYTES + 2);
        while ((remaining & ~0x7FL) != 0) {
            buffer[length++] = (byte) ((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        buffer[length++] = (byte) remaining;
        return this;
    }

    public AvroEncoder writeBytes(byte[] value) {
        writeLong(value.length);
        return writeRaw(value);
    }

    public AvroEncoder writeString(String value) {
        return writeBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    public AvroEncoder writeUnionIndex(int index) {
        return writeLong(index);
    }

    public AvroEncoder writeBlockCount(long count) {
        if (count <= 0) {
            throw new IllegalArgumentException("a block holds at least one item: " + count);
        }
        return writeLong(count);
    }

    public AvroEncoder writeEndOfBlocks() {
        return writeLong(0);
    }

    public AvroEncoder writeRaw(byte[] value) {
        ensure(value.length);
        System.arraycopy(value, 0, buffer, length, value.length);
        length += value.length;
        return this;
    }

    public int size() {
        return length;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buffer, length);
    }

    public void reset() {
        length = 0;
    }

    private void ensure(int extra) {
        if (length + extra > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(buffer.length * 2, length + extra));
        }
    }
}
