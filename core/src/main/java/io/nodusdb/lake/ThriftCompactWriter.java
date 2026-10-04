package io.nodusdb.lake;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.Deque;

final class ThriftCompactWriter {

    static final int TYPE_I32 = 5;
    static final int TYPE_I64 = 6;
    static final int TYPE_BINARY = 8;
    static final int TYPE_LIST = 9;
    static final int TYPE_STRUCT = 12;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final Deque<Integer> savedFieldIds = new ArrayDeque<>();
    private int lastFieldId;

    void beginStruct() {
        savedFieldIds.push(lastFieldId);
        lastFieldId = 0;
    }

    void endStruct() {
        out.write(0);
        lastFieldId = savedFieldIds.pop();
    }

    void structField(int id) {
        header(id, TYPE_STRUCT);
        beginStruct();
    }

    void i32Field(int id, int value) {
        header(id, TYPE_I32);
        zigzagVarint(value);
    }

    void i64Field(int id, long value) {
        header(id, TYPE_I64);
        zigzagVarintLong(value);
    }

    void binaryField(int id, byte[] value) {
        header(id, TYPE_BINARY);
        binary(value);
    }

    void listField(int id, int elementType, int size) {
        header(id, TYPE_LIST);
        if (size < 15) {
            out.write((size << 4) | elementType);
        } else {
            out.write(0xF0 | elementType);
            varint(size);
        }
    }

    void i32Element(int value) {
        zigzagVarint(value);
    }

    void binaryElement(byte[] value) {
        binary(value);
    }

    byte[] toByteArray() {
        return out.toByteArray();
    }

    private void header(int id, int type) {
        int delta = id - lastFieldId;
        if (delta > 0 && delta <= 15) {
            out.write((delta << 4) | type);
        } else {
            out.write(type);
            zigzagVarint(id);
        }
        lastFieldId = id;
    }

    private void binary(byte[] value) {
        varint(value.length);
        out.write(value, 0, value.length);
    }

    private void zigzagVarint(int value) {
        varint(Integer.toUnsignedLong((value << 1) ^ (value >> 31)));
    }

    private void zigzagVarintLong(long value) {
        varint((value << 1) ^ (value >> 63));
    }

    private void varint(long value) {
        long remaining = value;
        while ((remaining & ~0x7FL) != 0) {
            out.write((int) ((remaining & 0x7F) | 0x80));
            remaining >>>= 7;
        }
        out.write((int) remaining);
    }
}
