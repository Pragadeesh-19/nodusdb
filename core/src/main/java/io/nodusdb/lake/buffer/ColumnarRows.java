package io.nodusdb.lake.buffer;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Objects;

public record ColumnarRows(int rowCount, MemorySegment keys, List<MemorySegment> longColumns,
                           List<MemorySegment> intColumns, List<MemorySegment> varCharOffsets,
                           List<MemorySegment> varCharData) {

    public ColumnarRows {
        Objects.requireNonNull(keys, "keys");
        longColumns = List.copyOf(longColumns);
        intColumns = List.copyOf(intColumns);
        varCharOffsets = List.copyOf(varCharOffsets);
        varCharData = List.copyOf(varCharData);
        if (rowCount < 0) {
            throw new IllegalArgumentException("row count must be non-negative: " + rowCount);
        }
        requireBytes(keys, rowCount * (long) Long.BYTES, "keys");
        for (MemorySegment column : longColumns) {
            requireBytes(column, rowCount * (long) Long.BYTES, "long column");
        }
        for (MemorySegment column : intColumns) {
            requireBytes(column, rowCount * (long) Integer.BYTES, "int column");
        }
        if (varCharOffsets.size() != varCharData.size()) {
            throw new IllegalArgumentException("var-char offset and data columns differ in count");
        }
        for (MemorySegment offsets : varCharOffsets) {
            requireBytes(offsets, (rowCount + 1L) * Integer.BYTES, "var-char offsets");
        }
    }

    public int longColumnCount() {
        return longColumns.size();
    }

    public int intColumnCount() {
        return intColumns.size();
    }

    public int varCharColumnCount() {
        return varCharOffsets.size();
    }

    private static void requireBytes(MemorySegment segment, long bytes, String name) {
        if (segment.byteSize() < bytes) {
            throw new IllegalArgumentException(name + " spans " + segment.byteSize() + " bytes, needs " + bytes);
        }
    }
}
