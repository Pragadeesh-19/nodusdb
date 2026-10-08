package io.nodusdb.iceberg;

final class LittleEndian {

    private LittleEndian() {
    }

    static byte[] ofInt(int value) {
        return new byte[]{(byte) value, (byte) (value >>> 8), (byte) (value >>> 16), (byte) (value >>> 24)};
    }

    static int toInt(byte[] bytes) {
        if (bytes.length != Integer.BYTES) {
            throw new IllegalArgumentException("an int bound holds " + Integer.BYTES + " bytes, got " + bytes.length);
        }
        return (bytes[0] & 0xFF) | (bytes[1] & 0xFF) << 8 | (bytes[2] & 0xFF) << 16 | (bytes[3] & 0xFF) << 24;
    }
}
