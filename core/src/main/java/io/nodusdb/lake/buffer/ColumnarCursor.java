package io.nodusdb.lake.buffer;

import io.nodusdb.kernel.memory.NativeMemory;

import java.lang.foreign.MemorySegment;
import java.util.List;

public final class ColumnarCursor {

    final int rowCount;
    final MemorySegment keys;
    final long keysAddress;
    final long keysSize;
    final MemorySegment[] longColumns;
    final long[] longAddresses;
    final long[] longSizes;
    final MemorySegment[] intColumns;
    final long[] intAddresses;
    final long[] intSizes;
    final MemorySegment[] offsetColumns;
    final long[] offsetAddresses;
    final long[] offsetSizes;
    final MemorySegment[] dataColumns;

    private ColumnarCursor(ColumnarRows rows) {
        rowCount = rows.rowCount();
        keys = rows.keys();
        keysAddress = NativeMemory.readableAddress(keys);
        keysSize = keys.byteSize();
        longColumns = rows.longColumns().toArray(MemorySegment[]::new);
        longAddresses = addresses(rows.longColumns());
        longSizes = sizes(rows.longColumns());
        intColumns = rows.intColumns().toArray(MemorySegment[]::new);
        intAddresses = addresses(rows.intColumns());
        intSizes = sizes(rows.intColumns());
        offsetColumns = rows.varCharOffsets().toArray(MemorySegment[]::new);
        offsetAddresses = addresses(rows.varCharOffsets());
        offsetSizes = sizes(rows.varCharOffsets());
        dataColumns = rows.varCharData().toArray(MemorySegment[]::new);
    }

    public static ColumnarCursor of(ColumnarRows rows) {
        return new ColumnarCursor(rows);
    }

    public int rowCount() {
        return rowCount;
    }

    public long keyAt(int row) {
        return NativeMemory.getLongUnaligned(keys, keysAddress, keysSize, (long) row << 3);
    }

    long longAt(int column, int row) {
        return NativeMemory.getLongUnaligned(longColumns[column], longAddresses[column], longSizes[column],
                (long) row << 3);
    }

    int intAt(int column, int row) {
        return NativeMemory.getIntUnaligned(intColumns[column], intAddresses[column], intSizes[column],
                (long) row << 2);
    }

    int offsetAt(int column, int row) {
        return NativeMemory.getIntUnaligned(offsetColumns[column], offsetAddresses[column], offsetSizes[column],
                (long) row << 2);
    }

    private static long[] addresses(List<MemorySegment> columns) {
        long[] result = new long[columns.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = NativeMemory.readableAddress(columns.get(i));
        }
        return result;
    }

    private static long[] sizes(List<MemorySegment> columns) {
        long[] result = new long[columns.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = columns.get(i).byteSize();
        }
        return result;
    }
}
