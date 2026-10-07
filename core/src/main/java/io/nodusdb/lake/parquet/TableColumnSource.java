package io.nodusdb.lake.parquet;

import io.nodusdb.lake.buffer.DeltaMemTable;
import io.nodusdb.lake.buffer.RowSelection;
import io.nodusdb.lake.memory.NativeColumn;
import io.nodusdb.lake.model.LakeSchema;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

final class TableColumnSource implements ColumnSource, AutoCloseable {

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;

    private enum Origin {
        KEY, LONG, INT, VAR
    }

    private record Binding(Origin origin, int slot) {
    }

    private final DeltaMemTable table;
    private final RowSelection rows;
    private final List<ColumnSpec> columns = new ArrayList<>();
    private final List<Binding> bindings = new ArrayList<>();
    private final NativeColumn longs = new NativeColumn(NativeColumn.ALIGNMENT);
    private final NativeColumn offsets = new NativeColumn(NativeColumn.ALIGNMENT);
    private final NativeColumn data = new NativeColumn(NativeColumn.ALIGNMENT);

    TableColumnSource(LakeSchema schema, DeltaMemTable table, RowSelection rows) {
        this.table = table;
        this.rows = rows;
        columns.add(new ColumnSpec(LakeSchema.KEY_COLUMN, ColumnType.INT64, ColumnSpec.NO_FIELD_ID, true));
        bindings.add(new Binding(Origin.KEY, 0));
        int[] slots = schema.slots();
        for (int i = 0; i < schema.fields().size(); i++) {
            LakeSchema.Field field = schema.fields().get(i);
            switch (field.type()) {
                case INT64 -> add(field.name(), ColumnType.INT64, Origin.LONG, slots[i]);
                case DOUBLE -> add(field.name(), ColumnType.DOUBLE, Origin.LONG, slots[i]);
                case INT32 -> add(field.name(), ColumnType.INT32, Origin.INT, slots[i]);
                case UTF8 -> add(field.name(), ColumnType.STRING, Origin.VAR, slots[i]);
            }
        }
    }

    private void add(String name, ColumnType type, Origin origin, int slot) {
        columns.add(ColumnSpec.of(name, type));
        bindings.add(new Binding(origin, slot));
    }

    @Override
    public int rowCount() {
        return rows.size();
    }

    @Override
    public List<ColumnSpec> columns() {
        return columns;
    }

    @Override
    public MemorySegment fixed(int column, int from, int to) {
        Binding binding = bindings.get(column);
        return switch (binding.origin()) {
            case KEY -> gatherLongs(table.keyHashColumn(), from, to);
            case LONG -> gatherLongs(table.longColumn(binding.slot()), from, to);
            case INT -> gatherInts(table.intColumn(binding.slot()), from, to);
            case VAR -> throw new IllegalArgumentException("column " + column + " is not fixed width");
        };
    }

    @Override
    public Strings strings(int column, int from, int to) {
        Binding binding = bindings.get(column);
        if (binding.origin() != Origin.VAR) {
            throw new IllegalArgumentException("column " + column + " is not a string column");
        }
        int count = to - from;
        offsets.ensureCapacity((count + 1L) * Integer.BYTES);
        MemorySegment lengths = table.varCharLengthColumn(binding.slot());
        MemorySegment starts = table.varCharOffsetColumn(binding.slot());
        MemorySegment slab = table.varCharSlab();
        offsets.segment().setAtIndex(INT, 0, 0);
        for (int i = 0; i < count; i++) {
            int row = rows.rowAt(from + i);
            offsets.segment().setAtIndex(INT, i + 1, offsets.segment().getAtIndex(INT, i)
                    + lengths.getAtIndex(INT, row));
        }
        long total = offsets.segment().getAtIndex(INT, count);
        data.ensureCapacity(Math.max(1, total));
        for (int i = 0; i < count; i++) {
            int row = rows.rowAt(from + i);
            MemorySegment.copy(slab, starts.getAtIndex(INT, row), data.segment(),
                    offsets.segment().getAtIndex(INT, i), lengths.getAtIndex(INT, row));
        }
        return new Strings(data.segment().asSlice(0, total),
                offsets.segment().asSlice(0, (count + 1L) * Integer.BYTES));
    }

    @Override
    public void close() {
        longs.close();
        offsets.close();
        data.close();
    }

    private MemorySegment gatherLongs(MemorySegment source, int from, int to) {
        int count = to - from;
        longs.ensureCapacity(count * (long) Long.BYTES);
        MemorySegment out = longs.segment();
        for (int i = 0; i < count; i++) {
            out.setAtIndex(LONG, i, source.getAtIndex(LONG, rows.rowAt(from + i)));
        }
        return out.asSlice(0, count * (long) Long.BYTES);
    }

    private MemorySegment gatherInts(MemorySegment source, int from, int to) {
        int count = to - from;
        longs.ensureCapacity(count * (long) Long.BYTES);
        MemorySegment out = longs.segment();
        for (int i = 0; i < count; i++) {
            out.setAtIndex(LONG, i, source.getAtIndex(INT, rows.rowAt(from + i)));
        }
        return out.asSlice(0, count * (long) Long.BYTES);
    }
}
