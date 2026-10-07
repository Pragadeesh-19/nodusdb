package io.nodusdb.lake.buffer;

import io.nodusdb.lake.memory.NativeColumn;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class RowBuffer implements AutoCloseable {

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfInt INT_UNALIGNED = ValueLayout.JAVA_INT_UNALIGNED;
    private static final ValueLayout.OfLong UNALIGNED_LONG = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final ValueLayout.OfInt UNALIGNED_INT = ValueLayout.JAVA_INT_UNALIGNED;
    private static final long LONG_BYTES = Long.BYTES;
    private static final long INT_BYTES = Integer.BYTES;

    private final DeltaMemTable.Schema shape;
    private final NativeColumn longs;
    private final NativeColumn ints;
    private final NativeColumn lengths;
    private final NativeColumn bytes;

    public RowBuffer(DeltaMemTable.Schema shape) {
        this.shape = shape;
        this.longs = new NativeColumn(Math.max(1, shape.longColumns() * LONG_BYTES));
        this.ints = new NativeColumn(Math.max(1, shape.intColumns() * INT_BYTES));
        this.lengths = new NativeColumn(Math.max(1, shape.varCharColumns() * INT_BYTES));
        this.bytes = new NativeColumn(1);
    }

    public void load(ColumnarRows rows, int row) {
        for (int c = 0; c < shape.longColumns(); c++) {
            longs.segment().setAtIndex(LONG, c, rows.longColumns().get(c).get(UNALIGNED_LONG, row * LONG_BYTES));
        }
        for (int c = 0; c < shape.intColumns(); c++) {
            ints.segment().setAtIndex(INT, c, rows.intColumns().get(c).get(UNALIGNED_INT, row * INT_BYTES));
        }
        long cursor = 0;
        for (int c = 0; c < shape.varCharColumns(); c++) {
            MemorySegment offsets = rows.varCharOffsets().get(c);
            int start = offsets.get(INT_UNALIGNED, row * INT_BYTES);
            int end = offsets.get(INT_UNALIGNED, (row + 1) * INT_BYTES);
            int length = end - start;
            lengths.segment().setAtIndex(INT, c, length);
            bytes.ensureCapacity(cursor + length);
            NativeColumn.copyBytes(rows.varCharData().get(c), start, bytes.segment(), cursor, length);
            cursor += length;
        }
    }

    public MemorySegment longs() {
        return longs.segment().asSlice(0, shape.longColumns() * LONG_BYTES);
    }

    public MemorySegment ints() {
        return ints.segment().asSlice(0, shape.intColumns() * INT_BYTES);
    }

    public MemorySegment lengths() {
        return lengths.segment().asSlice(0, shape.varCharColumns() * INT_BYTES);
    }

    public MemorySegment bytes() {
        return bytes.segment();
    }

    @Override
    public void close() {
        longs.close();
        ints.close();
        lengths.close();
        bytes.close();
    }
}
