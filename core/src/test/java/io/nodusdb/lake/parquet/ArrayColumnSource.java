package io.nodusdb.lake.parquet;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class ArrayColumnSource implements ColumnSource {

    private final List<ColumnSpec> specs = new ArrayList<>();
    private final List<Object> data = new ArrayList<>();
    private final int rows;

    private ArrayColumnSource(int rows) {
        this.rows = rows;
    }

    static ArrayColumnSource of(int rows) {
        return new ArrayColumnSource(rows);
    }

    ArrayColumnSource fixed(ColumnSpec spec, long... values) {
        if (values.length != rows) {
            throw new IllegalArgumentException("expected " + rows + " values");
        }
        specs.add(spec);
        data.add(values);
        return this;
    }

    ArrayColumnSource strings(ColumnSpec spec, String... values) {
        if (values.length != rows) {
            throw new IllegalArgumentException("expected " + rows + " values");
        }
        specs.add(spec);
        data.add(values);
        return this;
    }

    @Override
    public int rowCount() {
        return rows;
    }

    @Override
    public List<ColumnSpec> columns() {
        return specs;
    }

    @Override
    public MemorySegment fixed(int column, int from, int to) {
        long[] values = (long[]) data.get(column);
        return MemorySegment.ofArray(Arrays.copyOfRange(values, from, to));
    }

    @Override
    public Strings strings(int column, int from, int to) {
        String[] values = (String[]) data.get(column);
        int count = to - from;
        int[] offsets = new int[count + 1];
        byte[][] encoded = new byte[count][];
        int total = 0;
        for (int i = 0; i < count; i++) {
            encoded[i] = values[from + i].getBytes(StandardCharsets.UTF_8);
            total += encoded[i].length;
            offsets[i + 1] = total;
        }
        byte[] slab = new byte[Math.max(1, total)];
        int position = 0;
        for (byte[] value : encoded) {
            System.arraycopy(value, 0, slab, position, value.length);
            position += value.length;
        }
        return new Strings(MemorySegment.ofArray(slab).asSlice(0, total), MemorySegment.ofArray(offsets));
    }
}
