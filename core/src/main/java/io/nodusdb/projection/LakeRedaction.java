package io.nodusdb.projection;

import io.nodusdb.chain.SignedRecord;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.iceberg.DataFile;
import io.nodusdb.iceberg.DataFileReader;
import io.nodusdb.iceberg.DataFileWriter;
import io.nodusdb.iceberg.EdgeLogRow;
import io.nodusdb.iceberg.IcebergTable;
import io.nodusdb.iceberg.ManifestEntry;
import io.nodusdb.iceberg.ManifestSummary;
import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.json.JsonString;
import io.nodusdb.json.JsonValue;
import io.nodusdb.json.JsonWriter;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ShipState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

public final class LakeRedaction {

    public static final String COVERAGE_PROPERTY = "nodus.erasure.v1";
    public static final String RECORD_PROPERTY = "nodus.redaction.v1";
    public static final String RECORD_DOMAIN = "nodus.redaction.v1";

    private static final String INTENT_PREFIX = "redaction-";
    private static final String INTENT_SUFFIX = ".json";
    private static final char COVERAGE_SEPARATOR = '/';
    private static final int PAGE_KEYS = 1000;

    private record Plan(int partitionDay, List<EdgeLogRow> rows, String key) {
    }

    private record Intent(String key, List<String> added, List<String> removed, OptionalLong snapshotId,
                          List<String> manifests) {
    }

    private final ObjectStore store;
    private final IcebergTable table;
    private final DataFileReader reader;
    private final DataFileWriter writer;
    private final ErasedNames names;
    private final SigningKey key;

    public LakeRedaction(ObjectStore store, IcebergTable table, DataFileWriter writer, ErasedNames names,
                         SigningKey key) {
        this.store = store;
        this.table = table;
        this.reader = new DataFileReader(store, table);
        this.writer = writer;
        this.names = names;
        this.key = key;
    }

    public static ShipState.LakeCoverage coverageOf(Optional<IcebergTable.Head> head) {
        String text = head.flatMap(found -> found.state().current())
                .map(snapshot -> snapshot.summary().get(COVERAGE_PROPERTY)).orElse(null);
        if (text == null) {
            return ShipState.LakeCoverage.NONE;
        }
        int separator = text.indexOf(COVERAGE_SEPARATOR);
        try {
            return new ShipState.LakeCoverage(Integer.parseInt(text.substring(0, separator)),
                    Long.parseLong(text.substring(separator + 1)));
        } catch (NumberFormatException | IndexOutOfBoundsException malformed) {
            throw new IllegalStateException("the table's " + COVERAGE_PROPERTY + " is not valid: " + text);
        }
    }

    public static String coverageText(ShipState.LakeCoverage coverage) {
        return coverage.erasedCount() + String.valueOf(COVERAGE_SEPARATOR) + coverage.eraseLsn();
    }

    public Optional<IcebergTable.Head> recover(Optional<IcebergTable.Head> head) {
        Optional<IcebergTable.Head> current = head;
        for (Intent intent : intents()) {
            boolean published = intent.snapshotId().isPresent() && current.isPresent()
                    && current.get().state().snapshots().stream()
                    .anyMatch(snapshot -> snapshot.snapshotId() == intent.snapshotId().getAsLong());
            if (published) {
                current = Optional.of(finish(current.get(), intent));
            } else {
                table.deleteAll(intent.added());
                store.delete(intent.key());
            }
        }
        return current;
    }

    public IcebergTable.Head run(IcebergTable.Head head, ShipState.LakeRequest request,
                                 Map<String, String> carried) throws IOException {
        Map<String, Plan> plans = plan(head, request.eraseLsn());
        Map<String, String> properties = new LinkedHashMap<>(carried);
        properties.put(COVERAGE_PROPERTY, coverageText(new ShipState.LakeCoverage(request.erasedCount(),
                request.eraseLsn())));
        if (plans.isEmpty()) {
            properties.put(RECORD_PROPERTY, record(request, Map.of()));
            return table.publish(table.prepareOverwrite(head, Map.of(), properties).prepared());
        }
        String intentKey = table.metadataKeyOf(INTENT_PREFIX + UUID.randomUUID() + INTENT_SUFFIX);
        List<String> added = plans.values().stream().map(Plan::key).toList();
        List<String> removed = plans.keySet().stream().map(table::keyOf).toList();
        writeIntent(new Intent(intentKey, added, removed, OptionalLong.empty(), List.of()));
        Map<String, DataFile> replacements = new LinkedHashMap<>();
        for (Map.Entry<String, Plan> plan : plans.entrySet()) {
            Plan rewrite = plan.getValue();
            replacements.put(plan.getKey(), writer.write(DataFileReader.rowsOf(rewrite.rows()),
                    rewrite.partitionDay(), rewrite.key()));
        }
        properties.put(RECORD_PROPERTY, record(request, replacements));
        IcebergTable.Overwrite overwrite = table.prepareOverwrite(head, replacements, properties);
        Intent intent = new Intent(intentKey, added, removed,
                OptionalLong.of(overwrite.prepared().state().currentSnapshotId()), overwrite.replacedManifests());
        writeIntent(intent);
        return finish(table.publish(overwrite.prepared()), intent);
    }

    private Map<String, Plan> plan(IcebergTable.Head head, long eraseLsn) throws IOException {
        ErasedNames.Lookup lookup = names.current();
        Map<String, Optional<ErasedNames.Erased>> seen = new HashMap<>();
        Map<String, Plan> plans = new LinkedHashMap<>();
        for (ManifestSummary manifest : table.manifests(head)) {
            for (ManifestEntry entry : table.entries(manifest)) {
                OptionalLong firstLsn = table.firstLsnOf(entry.path());
                if (!entry.live() || firstLsn.isPresent() && firstLsn.getAsLong() > eraseLsn) {
                    continue;
                }
                List<EdgeLogRow> rows = reader.read(entry.path());
                List<EdgeLogRow> rewritten = new ArrayList<>(rows.size());
                boolean changed = false;
                for (EdgeLogRow row : rows) {
                    EdgeLogRow scrubbed = scrub(row, lookup, seen);
                    changed |= scrubbed != row;
                    rewritten.add(scrubbed);
                }
                if (changed) {
                    plans.put(entry.path(), new Plan(entry.partitionDay(), rewritten,
                            table.newDataKey(rows.get(0).lsn(), rows.get(rows.size() - 1).lsn())));
                }
            }
        }
        return plans;
    }

    private static EdgeLogRow scrub(EdgeLogRow row, ErasedNames.Lookup lookup,
                                    Map<String, Optional<ErasedNames.Erased>> seen) {
        EdgeLogRow scrubbed = row;
        Optional<ErasedNames.Erased> object = erased(row.objectType(), row.objectId(), lookup, seen);
        if (object.isPresent() && row.lsn() <= object.get().eraseLsn()) {
            String[] parts = EdgeLogDecoder.split(object.get().pseudonym());
            scrubbed = scrubbed.withObject(parts[0], parts[1]);
        }
        Optional<ErasedNames.Erased> subject = erased(row.subjectType(), row.subjectId(), lookup, seen);
        if (subject.isPresent() && row.lsn() <= subject.get().eraseLsn()) {
            String[] parts = EdgeLogDecoder.split(subject.get().pseudonym());
            scrubbed = scrubbed.withSubject(parts[0], parts[1]);
        }
        return scrubbed;
    }

    private static Optional<ErasedNames.Erased> erased(String type, String id, ErasedNames.Lookup lookup,
                                                       Map<String, Optional<ErasedNames.Erased>> seen) {
        if (type.isEmpty() && id.isEmpty()) {
            return Optional.empty();
        }
        String name = type.isEmpty() ? id : type + ":" + id;
        return seen.computeIfAbsent(name, candidate -> Optional.ofNullable(lookup.find(candidate)));
    }

    private IcebergTable.Head finish(IcebergTable.Head head, Intent intent) {
        IcebergTable.Head current = head;
        IcebergTable.Expiry expiry = table.prepareExpiry(current, Long.MAX_VALUE);
        if (expiry.prepared().isPresent()) {
            current = table.publish(expiry.prepared().get());
            table.deleteAll(expiry.deletable());
        }
        table.deleteAll(intent.removed());
        table.deleteAll(intent.manifests());
        store.delete(intent.key());
        return current;
    }

    private String record(ShipState.LakeRequest request, Map<String, DataFile> replacements) {
        JsonWriter json = new JsonWriter();
        json.beginObject();
        json.name("v").value(1);
        json.name("erase_lsn").value(request.eraseLsn());
        json.name("erased_count").value(request.erasedCount());
        json.name("removed").beginArray();
        for (String path : replacements.keySet()) {
            json.value(path);
        }
        json.endArray();
        json.name("added").beginArray();
        for (DataFile file : replacements.values()) {
            json.beginObject().name("path").value(file.path()).name("sha256").value(file.sha256().hex()).endObject();
        }
        json.endArray();
        json.endObject();
        return SignedRecord.seal(key, RECORD_DOMAIN, json.toBytes());
    }

    private void writeIntent(Intent intent) {
        JsonWriter json = new JsonWriter();
        json.beginObject();
        strings(json, "added", intent.added());
        strings(json, "removed", intent.removed());
        if (intent.snapshotId().isPresent()) {
            json.name("snapshot_id").value(intent.snapshotId().getAsLong());
        }
        strings(json, "manifests", intent.manifests());
        json.endObject();
        store.put(intent.key(), json.toBytes());
    }

    private static void strings(JsonWriter json, String name, List<String> values) {
        json.name(name).beginArray();
        for (String value : values) {
            json.value(value);
        }
        json.endArray();
    }

    private List<Intent> intents() {
        List<Intent> found = new ArrayList<>();
        String prefix = table.metadataKeyOf(INTENT_PREFIX);
        String after = "";
        while (true) {
            ListPage page = store.list(prefix, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                if (entry.key().endsWith(INTENT_SUFFIX)) {
                    store.get(entry.key()).ifPresent(bytes -> found.add(parseIntent(entry.key(), bytes)));
                }
            }
            if (!page.truncated()) {
                return found;
            }
            after = page.lastKey();
        }
    }

    private static Intent parseIntent(String key, byte[] bytes) {
        JsonObject json = JsonParser.parseObject(bytes);
        OptionalLong snapshotId = json.has("snapshot_id") ? OptionalLong.of(json.requireLong("snapshot_id"))
                : OptionalLong.empty();
        return new Intent(key, strings(json.requireArray("added")), strings(json.requireArray("removed")),
                snapshotId, strings(json.requireArray("manifests")));
    }

    private static List<String> strings(JsonArray array) {
        List<String> values = new ArrayList<>(array.size());
        for (JsonValue value : array.items()) {
            values.add(((JsonString) value).value());
        }
        return values;
    }
}
