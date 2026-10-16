package io.nodusdb.iceberg;

import io.nodusdb.avro.AvroContainerReader;
import io.nodusdb.avro.AvroDecoder;
import io.nodusdb.avro.AvroFormatException;
import io.nodusdb.iceberg.NodusLogTable.Column;
import io.nodusdb.lake.parquet.ColumnMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class ManifestReader {

    private static final int CONTENT_DATA = 0;

    private ManifestReader() {
    }

    public static List<ManifestEntry> read(byte[] bytes, ManifestSummary manifest) {
        AvroContainerReader.Container container = AvroContainerReader.read(bytes);
        List<ManifestEntry> entries = new ArrayList<>();
        for (AvroContainerReader.Block block : container.blocks()) {
            AvroDecoder records = new AvroDecoder(block.data());
            for (long i = 0; i < block.records(); i++) {
                entries.add(entry(records, manifest));
            }
            if (!records.atEnd()) {
                throw new AvroFormatException("a manifest block holds more data than its entries");
            }
        }
        return entries;
    }

    private static ManifestEntry entry(AvroDecoder in, ManifestSummary manifest) {
        ManifestEntry.Status status;
        try {
            status = ManifestEntry.Status.fromCode(in.readInt());
        } catch (IllegalArgumentException unknown) {
            throw new AvroFormatException(unknown.getMessage());
        }
        long snapshotId = in.readUnionIndex() == 0 ? manifest.addedSnapshotId() : in.readLong();
        long dataSequence = in.readUnionIndex() == 0 ? manifest.sequenceNumber() : in.readLong();
        long fileSequence = in.readUnionIndex() == 0 ? manifest.sequenceNumber() : in.readLong();
        if (in.readInt() != CONTENT_DATA) {
            throw new AvroFormatException("a manifest entry describes a delete file, which nodusdb never writes");
        }
        String path = in.readString();
        in.readString();
        if (in.readUnionIndex() == 0) {
            throw new AvroFormatException("a manifest entry has no partition day");
        }
        int partitionDay = in.readInt();
        long recordCount = in.readLong();
        long fileSize = in.readLong();
        Map<Integer, Long> sizes = longMap(in);
        Map<Integer, Long> values = longMap(in);
        Map<Integer, Long> nulls = longMap(in);
        longMap(in);
        Map<Integer, byte[]> lower = bytesMap(in);
        Map<Integer, byte[]> upper = bytesMap(in);
        skipOptionalBytes(in);
        skipOptionalArray(in);
        skipOptionalArray(in);
        if (in.readUnionIndex() != 0) {
            in.readInt();
        }
        return new ManifestEntry(status, snapshotId, dataSequence, fileSequence, path, partitionDay, recordCount,
                fileSize, metrics(sizes, values, nulls, lower, upper));
    }

    private static List<ColumnMetrics> metrics(Map<Integer, Long> sizes, Map<Integer, Long> values,
                                               Map<Integer, Long> nulls, Map<Integer, byte[]> lower,
                                               Map<Integer, byte[]> upper) {
        List<ColumnMetrics> metrics = new ArrayList<>();
        for (Map.Entry<Integer, Long> size : sizes.entrySet()) {
            int fieldId = size.getKey();
            metrics.add(new ColumnMetrics(fieldId, columnName(fieldId), values.getOrDefault(fieldId, 0L),
                    nulls.getOrDefault(fieldId, 0L), size.getValue(), lower.get(fieldId), upper.get(fieldId)));
        }
        return metrics;
    }

    private static String columnName(int fieldId) {
        for (Column column : Column.values()) {
            if (column.fieldId == fieldId) {
                return column.columnName;
            }
        }
        throw new AvroFormatException("a manifest entry has metrics for unknown field " + fieldId);
    }

    private static Map<Integer, Long> longMap(AvroDecoder in) {
        Map<Integer, Long> map = new TreeMap<>();
        if (in.readUnionIndex() == 0) {
            return map;
        }
        for (long count = in.readBlockCount(); count != 0; count = in.readBlockCount()) {
            for (long i = 0; i < count; i++) {
                map.put(in.readInt(), in.readLong());
            }
        }
        return map;
    }

    private static Map<Integer, byte[]> bytesMap(AvroDecoder in) {
        Map<Integer, byte[]> map = new TreeMap<>();
        if (in.readUnionIndex() == 0) {
            return map;
        }
        for (long count = in.readBlockCount(); count != 0; count = in.readBlockCount()) {
            for (long i = 0; i < count; i++) {
                map.put(in.readInt(), in.readBytes());
            }
        }
        return map;
    }

    private static void skipOptionalBytes(AvroDecoder in) {
        if (in.readUnionIndex() != 0) {
            in.readBytes();
        }
    }

    private static void skipOptionalArray(AvroDecoder in) {
        if (in.readUnionIndex() == 0) {
            return;
        }
        for (long count = in.readBlockCount(); count != 0; count = in.readBlockCount()) {
            for (long i = 0; i < count; i++) {
                in.readLong();
            }
        }
    }
}
