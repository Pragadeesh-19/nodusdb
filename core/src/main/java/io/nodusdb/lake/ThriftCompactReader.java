package io.nodusdb.lake;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class ThriftCompactReader {

    private static final int TYPE_BOOL_TRUE = 1;
    private static final int TYPE_BOOL_FALSE = 2;
    private static final int TYPE_BYTE = 3;
    private static final int TYPE_I16 = 4;
    private static final int TYPE_I32 = 5;
    private static final int TYPE_I64 = 6;
    private static final int TYPE_BINARY = 8;
    private static final int TYPE_LIST = 9;
    private static final int TYPE_STRUCT = 12;

    private final byte[] data;
    private int position;

    ThriftCompactReader(byte[] data, int position) {
        this.data = data;
        this.position = position;
    }

    int position() {
        return position;
    }

    Map<Integer, Object> readStruct() {
        Map<Integer, Object> fields = new HashMap<>();
        int lastId = 0;
        while (true) {
            int header = readByte();
            if (header == 0) {
                return fields;
            }
            int type = header & 0x0F;
            int delta = (header >>> 4) & 0x0F;
            int id = delta == 0 ? (int) readZigzag() : lastId + delta;
            fields.put(id, readValue(type));
            lastId = id;
        }
    }

    private Object readValue(int type) {
        switch (type) {
            case TYPE_BOOL_TRUE:
                return Boolean.TRUE;
            case TYPE_BOOL_FALSE:
                return Boolean.FALSE;
            case TYPE_BYTE:
                return (long) readByte();
            case TYPE_I16:
            case TYPE_I32:
            case TYPE_I64:
                return readZigzag();
            case TYPE_BINARY:
                return readBinary();
            case TYPE_LIST:
                return readList();
            case TYPE_STRUCT:
                return readStruct();
            default:
                throw new IllegalStateException("unsupported thrift compact type " + type);
        }
    }

    private List<Object> readList() {
        int header = readByte();
        int size = (header >>> 4) & 0x0F;
        int elementType = header & 0x0F;
        if (size == 15) {
            size = (int) readVarint();
        }
        List<Object> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(readValue(elementType));
        }
        return items;
    }

    private byte[] readBinary() {
        int length = (int) readVarint();
        require(length);
        byte[] value = Arrays.copyOfRange(data, position, position + length);
        position += length;
        return value;
    }

    private long readZigzag() {
        long encoded = readVarint();
        return (encoded >>> 1) ^ -(encoded & 1);
    }

    private long readVarint() {
        long result = 0;
        int shift = 0;
        while (true) {
            int b = readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 63) {
                throw new IllegalStateException("varint too long");
            }
        }
    }

    private int readByte() {
        require(1);
        return data[position++] & 0xFF;
    }

    private void require(int count) {
        if (count < 0 || position + count > data.length) {
            throw new IllegalStateException("truncated thrift data at offset " + position);
        }
    }
}
