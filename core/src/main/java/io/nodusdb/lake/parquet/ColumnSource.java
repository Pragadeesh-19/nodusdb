package io.nodusdb.lake.parquet;

import java.lang.foreign.MemorySegment;
import java.util.List;

public interface ColumnSource {

    record Strings(MemorySegment data, MemorySegment offsets) {
    }

    int rowCount();

    List<ColumnSpec> columns();

    MemorySegment fixed(int column, int from, int to);

    Strings strings(int column, int from, int to);
}
