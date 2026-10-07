package io.nodusdb.lake.parquet;

import io.nodusdb.lake.parquet.ColumnChunkEncoder.Statistics;

import java.util.Arrays;

final class ColumnBounds {

    private final ColumnType type;
    private byte[] lower;
    private byte[] upper;
    private boolean complete = true;

    ColumnBounds(ColumnType type) {
        this.type = type;
    }

    void add(Statistics statistics) {
        if (!statistics.present()) {
            complete = false;
            return;
        }
        if (lower == null || compare(statistics.min(), lower) < 0) {
            lower = statistics.min();
        }
        if (upper == null || compare(statistics.max(), upper) > 0) {
            upper = statistics.max();
        }
    }

    byte[] lower() {
        return complete ? lower : null;
    }

    byte[] upper() {
        return complete ? upper : null;
    }

    private int compare(byte[] left, byte[] right) {
        return switch (type) {
            case STRING -> Arrays.compareUnsigned(left, right);
            case DOUBLE -> Double.compare(Double.longBitsToDouble(signedLittleEndian(left)),
                    Double.longBitsToDouble(signedLittleEndian(right)));
            case INT32, INT64, TIMESTAMP_MICROS -> Long.compare(signedLittleEndian(left), signedLittleEndian(right));
        };
    }

    private static long signedLittleEndian(byte[] bytes) {
        long value = 0;
        for (int i = bytes.length - 1; i >= 0; i--) {
            value = (value << 8) | (bytes[i] & 0xFFL);
        }
        int shift = 64 - 8 * bytes.length;
        return (value << shift) >> shift;
    }
}
