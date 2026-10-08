package io.nodusdb.iceberg;

import io.nodusdb.json.JsonWriter;
import io.nodusdb.lake.parquet.ColumnSpec;
import io.nodusdb.lake.parquet.ColumnType;

import java.util.ArrayList;
import java.util.List;

public final class NodusLogTable {

    public enum Column {
        LSN(1, "lsn", ColumnType.INT64, "long"),
        COMMIT_TS(2, "commit_ts", ColumnType.TIMESTAMP_MICROS, "timestamptz"),
        TXN_LSN(3, "txn_lsn", ColumnType.INT64, "long"),
        EPOCH(4, "epoch", ColumnType.INT64, "long"),
        EVENT(5, "event", ColumnType.STRING, "string"),
        OBJECT_TYPE(6, "object_type", ColumnType.STRING, "string"),
        OBJECT_ID(7, "object_id", ColumnType.STRING, "string"),
        RELATION(8, "relation", ColumnType.STRING, "string"),
        SUBJECT_TYPE(9, "subject_type", ColumnType.STRING, "string"),
        SUBJECT_ID(10, "subject_id", ColumnType.STRING, "string"),
        SUBJECT_RELATION(11, "subject_relation", ColumnType.STRING, "string"),
        SCHEMA_VERSION(12, "schema_version", ColumnType.INT32, "int"),
        DETAIL(13, "detail", ColumnType.STRING, "string");

        public final int fieldId;
        public final String columnName;
        public final ColumnType parquetType;
        public final String icebergType;

        Column(int fieldId, String columnName, ColumnType parquetType, String icebergType) {
            this.fieldId = fieldId;
            this.columnName = columnName;
            this.parquetType = parquetType;
            this.icebergType = icebergType;
        }
    }

    public static final String TABLE_NAME = "nodus_log";
    public static final int FORMAT_VERSION = 2;
    public static final int SCHEMA_ID = 0;
    public static final int SPEC_ID = 0;
    public static final int SORT_ORDER_ID = 1;
    public static final int PARTITION_FIELD_ID = 1000;
    public static final String PARTITION_FIELD_NAME = "commit_ts_day";
    public static final int LAST_COLUMN_ID = Column.DETAIL.fieldId;

    private static final long MICROS_PER_DAY = 86_400_000_000L;

    private NodusLogTable() {
    }

    public static int dayOf(long commitMicros) {
        return Math.toIntExact(Math.floorDiv(commitMicros, MICROS_PER_DAY));
    }

    public static List<ColumnSpec> columns() {
        List<ColumnSpec> specs = new ArrayList<>();
        for (Column column : Column.values()) {
            specs.add(new ColumnSpec(column.columnName, column.parquetType, column.fieldId, column == Column.LSN));
        }
        return specs;
    }

    public static void writeSchema(JsonWriter json) {
        json.beginObject().name("type").value("struct").name("schema-id").value(SCHEMA_ID).name("fields")
                .beginArray();
        for (Column column : Column.values()) {
            json.beginObject().name("id").value(column.fieldId).name("name").value(column.columnName)
                    .name("required").value(true).name("type").value(column.icebergType).endObject();
        }
        json.endArray().endObject();
    }

    public static String schemaText() {
        JsonWriter json = new JsonWriter();
        writeSchema(json);
        return json.toString();
    }

    public static void writePartitionFields(JsonWriter json) {
        json.beginArray().beginObject().name("name").value(PARTITION_FIELD_NAME).name("transform").value("day")
                .name("source-id").value(Column.COMMIT_TS.fieldId).name("field-id").value(PARTITION_FIELD_ID)
                .endObject().endArray();
    }

    public static String partitionFieldsText() {
        JsonWriter json = new JsonWriter();
        writePartitionFields(json);
        return json.toString();
    }

    public static void writePartitionSpecs(JsonWriter json) {
        json.beginArray().beginObject().name("spec-id").value(SPEC_ID).name("fields");
        writePartitionFields(json);
        json.endObject().endArray();
    }

    public static void writeSortOrders(JsonWriter json) {
        json.beginArray();
        json.beginObject().name("order-id").value(0).name("fields").beginArray().endArray().endObject();
        json.beginObject().name("order-id").value(SORT_ORDER_ID).name("fields").beginArray().beginObject()
                .name("transform").value("identity").name("source-id").value(Column.LSN.fieldId)
                .name("direction").value("asc").name("null-order").value("nulls-first").endObject().endArray()
                .endObject();
        json.endArray();
    }
}
