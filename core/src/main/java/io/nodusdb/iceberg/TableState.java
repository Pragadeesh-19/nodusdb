package io.nodusdb.iceberg;

import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.json.JsonString;
import io.nodusdb.json.JsonValue;
import io.nodusdb.json.JsonWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public record TableState(String tableUuid, String location, long lastSequenceNumber, long lastUpdatedMillis,
                         long currentSnapshotId, List<SnapshotEntry> snapshots, List<SnapshotLogEntry> snapshotLog,
                         List<MetadataLogEntry> metadataLog, Map<String, String> properties) {

    public static final long NO_SNAPSHOT = -1;

    public record SnapshotEntry(long snapshotId, Long parentSnapshotId, long sequenceNumber, long timestampMillis,
                                String manifestList, Map<String, String> summary) {

        public SnapshotEntry {
            summary = Map.copyOf(summary);
        }
    }

    public record SnapshotLogEntry(long timestampMillis, long snapshotId) {
    }

    public record MetadataLogEntry(long timestampMillis, String file) {
    }

    public TableState {
        snapshots = List.copyOf(snapshots);
        snapshotLog = List.copyOf(snapshotLog);
        metadataLog = List.copyOf(metadataLog);
        properties = Map.copyOf(properties);
    }

    public static TableState empty(String tableUuid, String location, long nowMillis,
                                   Map<String, String> properties) {
        return new TableState(tableUuid, location, 0, nowMillis, NO_SNAPSHOT, List.of(), List.of(), List.of(),
                properties);
    }

    public Optional<SnapshotEntry> current() {
        return snapshots.stream().filter(snapshot -> snapshot.snapshotId() == currentSnapshotId).findFirst();
    }

    public TableState withSnapshot(SnapshotEntry snapshot, Optional<MetadataLogEntry> previous) {
        List<SnapshotEntry> all = new ArrayList<>(snapshots);
        all.add(snapshot);
        List<SnapshotLogEntry> log = new ArrayList<>(snapshotLog);
        log.add(new SnapshotLogEntry(snapshot.timestampMillis(), snapshot.snapshotId()));
        return new TableState(tableUuid, location, snapshot.sequenceNumber(), snapshot.timestampMillis(),
                snapshot.snapshotId(), all, log, withMetadataLog(previous), properties);
    }

    public TableState withoutSnapshots(Set<Long> expired, MetadataLogEntry previous,
                                       long nowMillis) {
        List<SnapshotEntry> kept = snapshots.stream().filter(s -> !expired.contains(s.snapshotId())).toList();
        List<SnapshotLogEntry> log = snapshotLog.stream().filter(s -> !expired.contains(s.snapshotId())).toList();
        return new TableState(tableUuid, location, lastSequenceNumber, nowMillis, currentSnapshotId, kept, log,
                withMetadataLog(Optional.of(previous)), properties);
    }

    private List<MetadataLogEntry> withMetadataLog(Optional<MetadataLogEntry> previous) {
        List<MetadataLogEntry> log = new ArrayList<>(metadataLog);
        previous.ifPresent(log::add);
        return log;
    }

    public byte[] toJson() {
        JsonWriter json = new JsonWriter();
        json.beginObject();
        json.name("format-version").value(NodusLogTable.FORMAT_VERSION);
        json.name("table-uuid").value(tableUuid);
        json.name("location").value(location);
        json.name("last-sequence-number").value(lastSequenceNumber);
        json.name("last-updated-ms").value(lastUpdatedMillis);
        json.name("last-column-id").value(NodusLogTable.LAST_COLUMN_ID);
        json.name("current-schema-id").value(NodusLogTable.SCHEMA_ID);
        json.name("schemas").beginArray();
        NodusLogTable.writeSchema(json);
        json.endArray();
        json.name("default-spec-id").value(NodusLogTable.SPEC_ID);
        json.name("partition-specs");
        NodusLogTable.writePartitionSpecs(json);
        json.name("last-partition-id").value(NodusLogTable.PARTITION_FIELD_ID);
        json.name("default-sort-order-id").value(NodusLogTable.SORT_ORDER_ID);
        json.name("sort-orders");
        NodusLogTable.writeSortOrders(json);
        writeStringMap(json, "properties", properties);
        json.name("current-snapshot-id").value(currentSnapshotId);
        if (currentSnapshotId != NO_SNAPSHOT) {
            json.name("refs").beginObject().name("main").beginObject().name("snapshot-id").value(currentSnapshotId)
                    .name("type").value("branch").endObject().endObject();
        }
        json.name("snapshots").beginArray();
        for (SnapshotEntry snapshot : snapshots) {
            writeSnapshot(json, snapshot);
        }
        json.endArray();
        json.name("statistics").beginArray().endArray();
        json.name("snapshot-log").beginArray();
        for (SnapshotLogEntry entry : snapshotLog) {
            json.beginObject().name("timestamp-ms").value(entry.timestampMillis()).name("snapshot-id")
                    .value(entry.snapshotId()).endObject();
        }
        json.endArray();
        json.name("metadata-log").beginArray();
        for (MetadataLogEntry entry : metadataLog) {
            json.beginObject().name("timestamp-ms").value(entry.timestampMillis()).name("metadata-file")
                    .value(entry.file()).endObject();
        }
        json.endArray();
        json.endObject();
        return json.toBytes();
    }

    private static void writeSnapshot(JsonWriter json, SnapshotEntry snapshot) {
        json.beginObject().name("snapshot-id").value(snapshot.snapshotId());
        if (snapshot.parentSnapshotId() != null) {
            json.name("parent-snapshot-id").value(snapshot.parentSnapshotId());
        }
        json.name("sequence-number").value(snapshot.sequenceNumber());
        json.name("timestamp-ms").value(snapshot.timestampMillis());
        writeStringMap(json, "summary", snapshot.summary());
        json.name("manifest-list").value(snapshot.manifestList());
        json.name("schema-id").value(NodusLogTable.SCHEMA_ID);
        json.endObject();
    }

    private static void writeStringMap(JsonWriter json, String name, Map<String, String> map) {
        json.name(name).beginObject();
        new TreeMap<>(map).forEach((key, value) -> json.name(key).value(value));
        json.endObject();
    }

    public static TableState parse(byte[] bytes) {
        JsonObject root = JsonParser.parseObject(bytes);
        if (root.requireLong("format-version") != NodusLogTable.FORMAT_VERSION
                || root.requireLong("last-column-id") != NodusLogTable.LAST_COLUMN_ID) {
            throw new IllegalStateException("the table is not a format-version 2 nodus_log table");
        }
        List<SnapshotEntry> snapshots = new ArrayList<>();
        for (JsonValue item : root.requireArray("snapshots").items()) {
            snapshots.add(parseSnapshot((JsonObject) item));
        }
        List<SnapshotLogEntry> snapshotLog = new ArrayList<>();
        for (JsonValue item : root.requireArray("snapshot-log").items()) {
            JsonObject entry = (JsonObject) item;
            snapshotLog.add(new SnapshotLogEntry(entry.requireLong("timestamp-ms"), entry.requireLong("snapshot-id")));
        }
        List<MetadataLogEntry> metadataLog = new ArrayList<>();
        for (JsonValue item : root.requireArray("metadata-log").items()) {
            JsonObject entry = (JsonObject) item;
            metadataLog.add(new MetadataLogEntry(entry.requireLong("timestamp-ms"),
                    entry.requireString("metadata-file")));
        }
        return new TableState(root.requireString("table-uuid"), root.requireString("location"),
                root.requireLong("last-sequence-number"), root.requireLong("last-updated-ms"),
                root.longOr("current-snapshot-id", NO_SNAPSHOT), snapshots, snapshotLog, metadataLog,
                stringMap(root.requireObject("properties")));
    }

    private static SnapshotEntry parseSnapshot(JsonObject snapshot) {
        Long parent = snapshot.has("parent-snapshot-id") ? snapshot.requireLong("parent-snapshot-id") : null;
        return new SnapshotEntry(snapshot.requireLong("snapshot-id"), parent, snapshot.requireLong("sequence-number"),
                snapshot.requireLong("timestamp-ms"), snapshot.requireString("manifest-list"),
                stringMap(snapshot.requireObject("summary")));
    }

    private static Map<String, String> stringMap(JsonObject object) {
        Map<String, String> map = new LinkedHashMap<>();
        for (Map.Entry<String, JsonValue> entry : object.members().entrySet()) {
            if (!(entry.getValue() instanceof JsonString text)) {
                throw new IllegalStateException("the value of '" + entry.getKey() + "' is not text");
            }
            map.put(entry.getKey(), text.value());
        }
        return map;
    }
}
