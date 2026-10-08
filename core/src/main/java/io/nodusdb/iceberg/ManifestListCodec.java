package io.nodusdb.iceberg;

import io.nodusdb.avro.AvroContainerReader;
import io.nodusdb.avro.AvroContainerWriter;
import io.nodusdb.avro.AvroDecoder;
import io.nodusdb.avro.AvroEncoder;
import io.nodusdb.avro.AvroFormatException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ManifestListCodec {

    private static final int CONTENT_DATA = 0;

    private ManifestListCodec() {
    }

    public static byte[] encode(List<ManifestSummary> manifests, long snapshotId, Long parentSnapshotId,
                                long sequenceNumber) {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("snapshot-id", Long.toString(snapshotId));
        metadata.put("parent-snapshot-id", parentSnapshotId == null ? "null" : Long.toString(parentSnapshotId));
        metadata.put("sequence-number", Long.toString(sequenceNumber));
        metadata.put("format-version", Integer.toString(NodusLogTable.FORMAT_VERSION));
        AvroContainerWriter container = new AvroContainerWriter(IcebergAvroSchemas.MANIFEST_LIST, metadata);
        for (ManifestSummary manifest : manifests) {
            container.append(record(manifest));
        }
        return container.finish();
    }

    public static List<ManifestSummary> decode(byte[] bytes) {
        AvroContainerReader.Container container = AvroContainerReader.read(bytes);
        List<ManifestSummary> manifests = new ArrayList<>();
        for (AvroContainerReader.Block block : container.blocks()) {
            AvroDecoder records = new AvroDecoder(block.data());
            for (long i = 0; i < block.records(); i++) {
                manifests.add(read(records));
            }
            if (!records.atEnd()) {
                throw new AvroFormatException("a manifest list block holds more data than its records");
            }
        }
        return manifests;
    }

    private static byte[] record(ManifestSummary manifest) {
        AvroEncoder record = new AvroEncoder();
        record.writeString(manifest.path());
        record.writeLong(manifest.length());
        record.writeInt(NodusLogTable.SPEC_ID);
        record.writeInt(CONTENT_DATA);
        record.writeLong(manifest.sequenceNumber());
        record.writeLong(manifest.minSequenceNumber());
        record.writeLong(manifest.addedSnapshotId());
        record.writeInt(manifest.addedFiles());
        record.writeInt(manifest.existingFiles());
        record.writeInt(manifest.deletedFiles());
        record.writeLong(manifest.addedRows());
        record.writeLong(manifest.existingRows());
        record.writeLong(manifest.deletedRows());
        record.writeUnionIndex(1).writeBlockCount(1);
        record.writeBoolean(false);
        record.writeUnionIndex(0);
        record.writeUnionIndex(1).writeBytes(LittleEndian.ofInt(manifest.lowerDay()));
        record.writeUnionIndex(1).writeBytes(LittleEndian.ofInt(manifest.upperDay()));
        record.writeEndOfBlocks();
        record.writeUnionIndex(0);
        return record.toByteArray();
    }

    private static ManifestSummary read(AvroDecoder in) {
        String path = in.readString();
        long length = in.readLong();
        in.readInt();
        in.readInt();
        long sequenceNumber = in.readLong();
        long minSequenceNumber = in.readLong();
        long addedSnapshotId = in.readLong();
        int addedFiles = in.readInt();
        int existingFiles = in.readInt();
        int deletedFiles = in.readInt();
        long addedRows = in.readLong();
        long existingRows = in.readLong();
        long deletedRows = in.readLong();
        int[] days = readPartitionDays(in);
        if (in.readUnionIndex() != 0) {
            in.readBytes();
        }
        return new ManifestSummary(path, length, sequenceNumber, minSequenceNumber, addedSnapshotId, addedFiles,
                existingFiles, deletedFiles, addedRows, existingRows, deletedRows, days[0], days[1]);
    }

    private static int[] readPartitionDays(AvroDecoder in) {
        if (in.readUnionIndex() == 0) {
            throw new AvroFormatException("a manifest list entry has no partition summary");
        }
        long summaries = in.readBlockCount();
        if (summaries != 1) {
            throw new AvroFormatException("expected one partition summary but found " + summaries);
        }
        in.readBoolean();
        if (in.readUnionIndex() != 0) {
            in.readBoolean();
        }
        int[] days = new int[2];
        days[0] = readBound(in);
        days[1] = readBound(in);
        if (in.readBlockCount() != 0) {
            throw new AvroFormatException("a manifest list entry has more than one partition summary");
        }
        return days;
    }

    private static int readBound(AvroDecoder in) {
        if (in.readUnionIndex() == 0) {
            throw new AvroFormatException("a partition summary has no bounds");
        }
        return LittleEndian.toInt(in.readBytes());
    }
}
