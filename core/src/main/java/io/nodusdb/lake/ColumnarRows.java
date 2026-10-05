package io.nodusdb.lake;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Objects;

/*
 * Column-oriented input for LakeTable.upsertColumns, as segments the caller owns. Each
 * variable-width column follows Arrow's layout: value i of a column spans bytes
 * [offsets[i], offsets[i + 1]) of its data segment. Offsets are absolute positions in that
 * data, so a slice of a larger buffer needs no copy. Segments may be native memory, so the
 * table reads them in place and never builds a Java array from them.
 */
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

    int longColumnCount() {
        return longColumns.size();
    }

    int intColumnCount() {
        return intColumns.size();
    }

    int varCharColumnCount() {
        return varCharOffsets.size();
    }

    private static void requireBytes(MemorySegment segment, long bytes, String name) {
        if (segment.byteSize() < bytes) {
            throw new IllegalArgumentException(name + " spans " + segment.byteSize() + " bytes, needs " + bytes);
        }
    }
}
