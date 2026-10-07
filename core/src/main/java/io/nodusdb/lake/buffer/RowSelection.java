package io.nodusdb.lake.buffer;

import io.nodusdb.lake.memory.NativeColumn;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class RowSelection implements AutoCloseable {

    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;

    private final NativeColumn rows;
    private final int count;

    private RowSelection(NativeColumn rows, int count) {
        this.rows = rows;
        this.count = count;
    }

    public static RowSelection of(int... values) {
        NativeColumn rows = new NativeColumn(Math.max(1, (long) values.length * Integer.BYTES));
        MemorySegment segment = rows.segment();
        for (int i = 0; i < values.length; i++) {
            segment.setAtIndex(INT, i, values[i]);
        }
        return new RowSelection(rows, values.length);
    }

    public static RowSelection ofKind(DeltaMemTable table, byte kind) {
        int count = 0;
        for (int row = 0; row < table.size(); row++) {
            if (table.kindAt(row) == kind) {
                count++;
            }
        }
        NativeColumn rows = new NativeColumn(Math.max(1, (long) count * Integer.BYTES));
        MemorySegment segment = rows.segment();
        int next = 0;
        for (int row = 0; row < table.size(); row++) {
            if (table.kindAt(row) == kind) {
                segment.setAtIndex(INT, next++, row);
            }
        }
        return new RowSelection(rows, count);
    }

    public int size() {
        return count;
    }

    public int rowAt(int index) {
        return rows.segment().getAtIndex(INT, index);
    }

    @Override
    public void close() {
        rows.close();
    }
}
