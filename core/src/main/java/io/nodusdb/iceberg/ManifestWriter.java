package io.nodusdb.iceberg;

import io.nodusdb.avro.AvroContainerWriter;
import io.nodusdb.avro.AvroEncoder;
import io.nodusdb.lake.parquet.ColumnMetrics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ManifestWriter {

    public record Encoded(byte[] bytes, int files, long rows, int lowerDay, int upperDay) {
    }

    private static final int STATUS_ADDED = 1;
    private static final int CONTENT_DATA = 0;
    private static final String FORMAT_PARQUET = "PARQUET";

    private ManifestWriter() {
    }

    public static Encoded encode(List<DataFile> files, long snapshotId) {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("a manifest needs at least one data file");
        }
        AvroContainerWriter container = new AvroContainerWriter(IcebergAvroSchemas.MANIFEST_ENTRY, metadata());
        long rows = 0;
        int lowerDay = Integer.MAX_VALUE;
        int upperDay = Integer.MIN_VALUE;
        for (DataFile file : files) {
            container.append(entry(file, snapshotId));
            rows += file.recordCount();
            lowerDay = Math.min(lowerDay, file.partitionDay());
            upperDay = Math.max(upperDay, file.partitionDay());
        }
        return new Encoded(container.finish(), files.size(), rows, lowerDay, upperDay);
    }

    private static Map<String, String> metadata() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("schema", NodusLogTable.schemaText());
        metadata.put("schema-id", Integer.toString(NodusLogTable.SCHEMA_ID));
        metadata.put("partition-spec", NodusLogTable.partitionFieldsText());
        metadata.put("partition-spec-id", Integer.toString(NodusLogTable.SPEC_ID));
        metadata.put("format-version", Integer.toString(NodusLogTable.FORMAT_VERSION));
        metadata.put("content", "data");
        return metadata;
    }

    private static byte[] entry(DataFile file, long snapshotId) {
        AvroEncoder entry = new AvroEncoder();
        entry.writeInt(STATUS_ADDED);
        entry.writeUnionIndex(1).writeLong(snapshotId);
        entry.writeUnionIndex(0);
        entry.writeUnionIndex(0);
        entry.writeInt(CONTENT_DATA);
        entry.writeString(file.path());
        entry.writeString(FORMAT_PARQUET);
        entry.writeUnionIndex(1).writeInt(file.partitionDay());
        entry.writeLong(file.recordCount());
        entry.writeLong(file.fileSizeBytes());
        writeLongMap(entry, file.columns(), Metric.SIZE);
        writeLongMap(entry, file.columns(), Metric.VALUES);
        writeLongMap(entry, file.columns(), Metric.NULLS);
        entry.writeUnionIndex(0);
        writeBoundsMap(entry, file.columns(), true);
        writeBoundsMap(entry, file.columns(), false);
        entry.writeUnionIndex(0);
        entry.writeUnionIndex(0);
        entry.writeUnionIndex(0);
        entry.writeUnionIndex(1).writeInt(NodusLogTable.SORT_ORDER_ID);
        return entry.toByteArray();
    }

    private enum Metric {
        SIZE, VALUES, NULLS
    }

    private static void writeLongMap(AvroEncoder entry, List<ColumnMetrics> columns, Metric metric) {
        long present = columns.stream().filter(column -> column.fieldId() > 0).count();
        if (present == 0) {
            entry.writeUnionIndex(0);
            return;
        }
        entry.writeUnionIndex(1).writeBlockCount(present);
        for (ColumnMetrics column : columns) {
            if (column.fieldId() <= 0) {
                continue;
            }
            entry.writeInt(column.fieldId());
            entry.writeLong(switch (metric) {
                case SIZE -> column.compressedBytes();
                case VALUES -> column.valueCount();
                case NULLS -> column.nullCount();
            });
        }
        entry.writeEndOfBlocks();
    }

    private static void writeBoundsMap(AvroEncoder entry, List<ColumnMetrics> columns, boolean lower) {
        long present = columns.stream().filter(column -> column.fieldId() > 0 && column.hasBounds()).count();
        if (present == 0) {
            entry.writeUnionIndex(0);
            return;
        }
        entry.writeUnionIndex(1).writeBlockCount(present);
        for (ColumnMetrics column : columns) {
            if (column.fieldId() <= 0 || !column.hasBounds()) {
                continue;
            }
            entry.writeInt(column.fieldId());
            entry.writeBytes(lower ? column.lowerBound() : column.upperBound());
        }
        entry.writeEndOfBlocks();
    }
}
