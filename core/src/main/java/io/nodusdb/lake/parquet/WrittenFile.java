package io.nodusdb.lake.parquet;

import java.util.List;

public record WrittenFile(long rowCount, long fileBytes, List<ColumnMetrics> columns) {

    public WrittenFile {
        columns = List.copyOf(columns);
    }
}
