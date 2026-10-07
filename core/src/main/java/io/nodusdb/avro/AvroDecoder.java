package io.nodusdb.avro;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class AvroDecoder {

    private static final int MAX_VARINT_BYTES = 10;

    private final byte[] data;
    private final int end;
    private int position;

    public AvroDecoder(byte[] data) {
        this(data, 0, data.length);
    }

    public AvroDecoder(byte[] data, int from, int to) {
        if (from < 0 || to > data.length || from > to) {
            throw new IllegalArgumentException("the range " + from + ".." + to + " is outside the data");
        }
        this.data = data;
        this.position = from;
        this.end = to;
    }

    public boolean atEnd() {
        return position == end;
    }

    public int position() {
        return position;
    }

    public boolean readBoolean() {
        require(1);
        byte value = data[position++];
        if (value != 0 && value != 1) {
            throw new AvroFormatException("a boolean is 0 or 1, found " + value);
        }
        return value == 1;
    }

    public int readInt() {
        long value = readLong();
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new AvroFormatException("the value " + value + " does not fit an int");
        }
        return (int) value;
    }

    public long readLong() {
        long result = 0;
        int shift = 0;
        for (int i = 0; i < MAX_VARINT_BYTES; i++) {
            require(1);
            int b = data[position++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return (result >>> 1) ^ -(result & 1);
            }
            shift += 7;
        }
        throw new AvroFormatException("a variable-length integer is longer than " + MAX_VARINT_BYTES + " bytes");
    }

    public byte[] readBytes() {
        long length = readLong();
        if (length < 0 || length > end - position) {
            throw new AvroFormatException("a byte string of " + length + " bytes does not fit the data");
        }
        return readRaw((int) length);
    }

    public String readString() {
        return new String(readBytes(), StandardCharsets.UTF_8);
    }

    public int readUnionIndex() {
        long index = readLong();
        if (index < 0 || index > Integer.MAX_VALUE) {
            throw new AvroFormatException("a union branch index of " + index + " is not valid");
        }
        return (int) index;
    }

    public long readBlockCount() {
        long count = readLong();
        if (count < 0) {
            readLong();
            return -count;
        }
        return count;
    }

    public byte[] readRaw(int length) {
        require(length);
        byte[] bytes = Arrays.copyOfRange(data, position, position + length);
        position += length;
        return bytes;
    }

    private void require(int count) {
        if (count < 0 || count > end - position) {
            throw new AvroFormatException("the data ends at offset " + end + " but " + count + " more bytes were "
                    + "expected at offset " + position);
        }
    }
}
