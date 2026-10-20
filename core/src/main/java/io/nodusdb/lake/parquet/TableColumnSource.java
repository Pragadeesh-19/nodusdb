package io.nodusdb.lake.parquet;

import io.nodusdb.kernel.memory.NativeMemory;
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
    private static final int CHUNK = 4096;

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
    private final int[] rowChunk = new int[CHUNK];
    private final int[] intChunk = new int[CHUNK];
    private final int[] lengthChunk = new int[CHUNK];
    private final long[] longChunk = new long[CHUNK];

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
        long total = 0;
        offsets.setIntAtIndex(0, 0);
        for (int base = 0; base < count; base += CHUNK) {
            int length = Math.min(CHUNK, count - base);
            selectInts(lengths, from + base, length, lengthChunk);
            for (int i = 0; i < length; i++) {
                total += lengthChunk[i];
                intChunk[i] = (int) total;
            }
            MemorySegment.copy(intChunk, 0, offsets.segment(), INT, (base + 1L) * Integer.BYTES, length);
        }
        data.ensureCapacity(Math.max(1, total));
        MemorySegment target = data.segment();
        long runSource = 0;
        long runTarget = 0;
        long runLength = 0;
        long written = 0;
        for (int base = 0; base < count; base += CHUNK) {
            int length = Math.min(CHUNK, count - base);
            selectInts(lengths, from + base, length, lengthChunk);
            selectInts(starts, from + base, length, intChunk);
            for (int i = 0; i < length; i++) {
                long start = intChunk[i];
                if (start != runSource + runLength) {
                    MemorySegment.copy(slab, runSource, target, runTarget, runLength);
                    runSource = start;
                    runTarget = written;
                    runLength = 0;
                }
                runLength += lengthChunk[i];
                written += lengthChunk[i];
            }
        }
        MemorySegment.copy(slab, runSource, target, runTarget, runLength);
        return new Strings(target.asSlice(0, total), offsets.segment().asSlice(0, (count + 1L) * Integer.BYTES));
    }

    @Override
    public void close() {
        longs.close();
        offsets.close();
        data.close();
    }

    private MemorySegment gatherLongs(MemorySegment source, int from, int to) {
        int count = to - from;
        if (rows.identity()) {
            return source.asSlice((long) from * Long.BYTES, (long) count * Long.BYTES);
        }
        longs.ensureCapacity(count * (long) Long.BYTES);
        MemorySegment out = longs.segment();
        long address = NativeMemory.readableAddress(source);
        long size = source.byteSize();
        for (int base = 0; base < count; base += CHUNK) {
            int length = Math.min(CHUNK, count - base);
            MemorySegment.copy(rows.rows(), INT, (long) (from + base) * Integer.BYTES, rowChunk, 0, length);
            for (int i = 0; i < length; i++) {
                longChunk[i] = NativeMemory.getLong(source, address, size, (long) rowChunk[i] << 3);
            }
            MemorySegment.copy(longChunk, 0, out, LONG, (long) base * Long.BYTES, length);
        }
        return out.asSlice(0, count * (long) Long.BYTES);
    }

    private MemorySegment gatherInts(MemorySegment source, int from, int to) {
        int count = to - from;
        longs.ensureCapacity(count * (long) Long.BYTES);
        MemorySegment out = longs.segment();
        for (int base = 0; base < count; base += CHUNK) {
            int length = Math.min(CHUNK, count - base);
            selectInts(source, from + base, length, intChunk);
            for (int i = 0; i < length; i++) {
                longChunk[i] = intChunk[i];
            }
            MemorySegment.copy(longChunk, 0, out, LONG, (long) base * Long.BYTES, length);
        }
        return out.asSlice(0, count * (long) Long.BYTES);
    }

    private void selectInts(MemorySegment source, int from, int length, int[] target) {
        if (rows.identity()) {
            MemorySegment.copy(source, INT, (long) from * Integer.BYTES, target, 0, length);
            return;
        }
        long address = NativeMemory.readableAddress(source);
        long size = source.byteSize();
        MemorySegment.copy(rows.rows(), INT, (long) from * Integer.BYTES, rowChunk, 0, length);
        for (int i = 0; i < length; i++) {
            target[i] = NativeMemory.getInt(source, address, size, (long) rowChunk[i] << 2);
        }
    }
}
