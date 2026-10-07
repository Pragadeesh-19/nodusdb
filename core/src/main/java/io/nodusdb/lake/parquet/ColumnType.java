package io.nodusdb.lake.parquet;

import io.nodusdb.lake.parquet.ColumnChunkEncoder.Physical;

public enum ColumnType {

    INT32(Physical.INT32),
    INT64(Physical.INT64),
    DOUBLE(Physical.DOUBLE),
    STRING(Physical.BYTE_ARRAY),
    TIMESTAMP_MICROS(Physical.INT64);

    final Physical physical;

    ColumnType(Physical physical) {
        this.physical = physical;
    }

    boolean variableWidth() {
        return this == STRING;
    }
}
