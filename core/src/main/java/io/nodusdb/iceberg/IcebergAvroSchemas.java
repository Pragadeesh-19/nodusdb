package io.nodusdb.iceberg;

final class IcebergAvroSchemas {

    static final String MANIFEST_LIST = manifestList();
    static final String MANIFEST_ENTRY = manifestEntry();

    private IcebergAvroSchemas() {
    }

    private static String manifestList() {
        return "{\"type\":\"record\",\"name\":\"manifest_file\",\"fields\":["
                + field("manifest_path", "\"string\"", 500)
                + "," + field("manifest_length", "\"long\"", 501)
                + "," + field("partition_spec_id", "\"int\"", 502)
                + "," + field("content", "\"int\"", 517)
                + "," + field("sequence_number", "\"long\"", 515)
                + "," + field("min_sequence_number", "\"long\"", 516)
                + "," + field("added_snapshot_id", "\"long\"", 503)
                + "," + field("added_files_count", "\"int\"", 504)
                + "," + field("existing_files_count", "\"int\"", 505)
                + "," + field("deleted_files_count", "\"int\"", 506)
                + "," + field("added_rows_count", "\"long\"", 512)
                + "," + field("existing_rows_count", "\"long\"", 513)
                + "," + field("deleted_rows_count", "\"long\"", 514)
                + "," + optional("partitions", partitionSummaries(), 507)
                + "," + optional("key_metadata", "\"bytes\"", 519)
                + "]}";
    }

    private static String partitionSummaries() {
        return "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"r508\",\"fields\":["
                + field("contains_null", "\"boolean\"", 509)
                + "," + optional("contains_nan", "\"boolean\"", 518)
                + "," + optional("lower_bound", "\"bytes\"", 510)
                + "," + optional("upper_bound", "\"bytes\"", 511)
                + "]},\"element-id\":508}";
    }

    private static String manifestEntry() {
        return "{\"type\":\"record\",\"name\":\"manifest_entry\",\"fields\":["
                + field("status", "\"int\"", 0)
                + "," + optional("snapshot_id", "\"long\"", 1)
                + "," + optional("sequence_number", "\"long\"", 3)
                + "," + optional("file_sequence_number", "\"long\"", 4)
                + "," + field("data_file", dataFile(), 2)
                + "]}";
    }

    private static String dataFile() {
        return "{\"type\":\"record\",\"name\":\"r2\",\"fields\":["
                + field("content", "\"int\"", 134)
                + "," + field("file_path", "\"string\"", 100)
                + "," + field("file_format", "\"string\"", 101)
                + "," + field("partition", partition(), 102)
                + "," + field("record_count", "\"long\"", 103)
                + "," + field("file_size_in_bytes", "\"long\"", 104)
                + "," + optional("column_sizes", map("\"long\"", 117, 118), 108)
                + "," + optional("value_counts", map("\"long\"", 119, 120), 109)
                + "," + optional("null_value_counts", map("\"long\"", 121, 122), 110)
                + "," + optional("nan_value_counts", map("\"long\"", 138, 139), 137)
                + "," + optional("lower_bounds", map("\"bytes\"", 126, 127), 125)
                + "," + optional("upper_bounds", map("\"bytes\"", 129, 130), 128)
                + "," + optional("key_metadata", "\"bytes\"", 131)
                + "," + optional("split_offsets", "{\"type\":\"array\",\"items\":\"long\",\"element-id\":133}", 132)
                + "," + optional("equality_ids", "{\"type\":\"array\",\"items\":\"int\",\"element-id\":136}", 135)
                + "," + optional("sort_order_id", "\"int\"", 140)
                + "]}";
    }

    private static String partition() {
        return "{\"type\":\"record\",\"name\":\"r102\",\"fields\":["
                + optional(NodusLogTable.PARTITION_FIELD_NAME, "{\"type\":\"int\",\"logicalType\":\"date\"}",
                NodusLogTable.PARTITION_FIELD_ID)
                + "]}";
    }

    private static String map(String valueType, int keyId, int valueId) {
        return "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"k" + keyId + "_v" + valueId
                + "\",\"fields\":[" + field("key", "\"int\"", keyId) + "," + field("value", valueType, valueId)
                + "]},\"logicalType\":\"map\"}";
    }

    private static String field(String name, String type, int fieldId) {
        return "{\"name\":\"" + name + "\",\"type\":" + type + ",\"field-id\":" + fieldId + "}";
    }

    private static String optional(String name, String type, int fieldId) {
        return "{\"name\":\"" + name + "\",\"type\":[\"null\"," + type + "],\"default\":null,\"field-id\":"
                + fieldId + "}";
    }
}
