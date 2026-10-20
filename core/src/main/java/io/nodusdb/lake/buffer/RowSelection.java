package io.nodusdb.lake.buffer;

import io.nodusdb.lake.memory.NativeColumn;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class RowSelection implements AutoCloseable {

    private static final int CHUNK = 4096;

    private final NativeColumn rows;
    private final int count;
    private final boolean identity;

    private RowSelection(NativeColumn rows, int count, boolean identity) {
        this.rows = rows;
        this.count = count;
        this.identity = identity;
    }

    public static RowSelection of(int... values) {
        NativeColumn rows = new NativeColumn(Math.max(1, (long) values.length * Integer.BYTES));
        MemorySegment.copy(values, 0, rows.segment(), ValueLayout.JAVA_INT, 0, values.length);
        boolean identity = true;
        for (int i = 0; i < values.length; i++) {
            identity &= values[i] == i;
        }
        return new RowSelection(rows, values.length, identity);
    }

    public static RowSelection ofKind(DeltaMemTable table, byte kind) {
        int size = table.size();
        MemorySegment kinds = table.kindColumn();
        byte[] chunk = new byte[CHUNK];
        int count = 0;
        for (int base = 0; base < size; base += CHUNK) {
            int length = Math.min(CHUNK, size - base);
            MemorySegment.copy(kinds, ValueLayout.JAVA_BYTE, base, chunk, 0, length);
            for (int i = 0; i < length; i++) {
                if (chunk[i] == kind) {
                    count++;
                }
            }
        }
        NativeColumn rows = new NativeColumn(Math.max(1, (long) count * Integer.BYTES));
        int[] selected = new int[CHUNK];
        int next = 0;
        int pending = 0;
        for (int base = 0; base < size; base += CHUNK) {
            int length = Math.min(CHUNK, size - base);
            MemorySegment.copy(kinds, ValueLayout.JAVA_BYTE, base, chunk, 0, length);
            for (int i = 0; i < length; i++) {
                if (chunk[i] == kind) {
                    selected[pending++] = base + i;
                    if (pending == CHUNK) {
                        MemorySegment.copy(selected, 0, rows.segment(), ValueLayout.JAVA_INT,
                                (long) next * Integer.BYTES, pending);
                        next += pending;
                        pending = 0;
                    }
                }
            }
        }
        MemorySegment.copy(selected, 0, rows.segment(), ValueLayout.JAVA_INT, (long) next * Integer.BYTES, pending);
        return new RowSelection(rows, count, count == size);
    }

    public int size() {
        return count;
    }

    public boolean identity() {
        return identity;
    }

    public int rowAt(int index) {
        return rows.getIntAtIndex(index);
    }

    public MemorySegment rows() {
        return rows.segment().asSlice(0, (long) count * Integer.BYTES);
    }

    @Override
    public void close() {
        rows.close();
    }
}
