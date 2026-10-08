package io.nodusdb.iceberg;

import io.nodusdb.error.WriterFencedException;
import io.nodusdb.iceberg.TableState.MetadataLogEntry;
import io.nodusdb.iceberg.TableState.SnapshotEntry;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.PutResult;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class IcebergTable {

    public record Head(long version, TableState state, byte[] bytes) {
    }

    public record Prepared(long version, String metadataKey, byte[] metadata, TableState state) {
    }

    public record Expiry(Optional<Prepared> prepared, List<String> deletable) {
    }

    public static final String DATA_FOLDER = "data/";
    public static final String METADATA_FOLDER = "metadata/";
    public static final String VERSION_HINT = "version-hint.text";

    private static final Pattern METADATA_FILE = Pattern.compile("v(\\d+)\\.metadata\\.json");
    private static final Pattern DATA_FILE = Pattern.compile("(\\d{20})-\\d{20}-[0-9a-f-]+\\.parquet");
    private static final int PAGE_KEYS = 1000;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final ObjectStore store;
    private final String locationUri;
    private final String keyPrefix;
    private final LongSupplier clockMillis;
    private final Supplier<UUID> uuids;

    public IcebergTable(ObjectStore store, String locationUri, String keyPrefix, LongSupplier clockMillis) {
        this(store, locationUri, keyPrefix, clockMillis, UUID::randomUUID);
    }

    IcebergTable(ObjectStore store, String locationUri, String keyPrefix, LongSupplier clockMillis,
                 Supplier<UUID> uuids) {
        if (locationUri.endsWith("/") || !keyPrefix.endsWith("/")) {
            throw new IllegalArgumentException("the location must not end with '/' and the key prefix must");
        }
        this.store = store;
        this.locationUri = locationUri;
        this.keyPrefix = keyPrefix;
        this.clockMillis = clockMillis;
        this.uuids = uuids;
    }

    public String uriOf(String key) {
        if (!key.startsWith(keyPrefix)) {
            throw new IllegalArgumentException("'" + key + "' is outside the table");
        }
        return locationUri + "/" + key.substring(keyPrefix.length());
    }

    public String keyOf(String uri) {
        if (!uri.startsWith(locationUri + "/")) {
            throw new IllegalArgumentException("'" + uri + "' is outside the table");
        }
        return keyPrefix + uri.substring(locationUri.length() + 1);
    }

    public String newDataKey(long lsnFirst, long lsnLast) {
        return keyPrefix + DATA_FOLDER + String.format("%020d-%020d-%s.parquet", lsnFirst, lsnLast, uuids.get());
    }

    public Optional<Head> load() {
        long version = 0;
        String after = "";
        while (true) {
            ListPage page = store.list(keyPrefix + METADATA_FOLDER, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                version = Math.max(version, metadataVersion(entry.key()));
            }
            if (!page.truncated()) {
                break;
            }
            after = page.lastKey();
        }
        if (version == 0) {
            return Optional.empty();
        }
        long newest = version;
        byte[] bytes = store.get(metadataKey(newest)).orElseThrow(() -> new IllegalStateException(
                "the metadata file of version " + newest + " was listed but cannot be read"));
        return Optional.of(new Head(newest, TableState.parse(bytes), bytes));
    }

    public Prepared prepareAppend(Optional<Head> base, List<DataFile> files, Map<String, String> extraSummary) {
        long now = clockMillis.getAsLong();
        long snapshotId = newSnapshotId();
        long sequence = base.map(head -> head.state().lastSequenceNumber()).orElse(0L) + 1;
        TableState state = base.map(Head::state).orElseGet(() -> TableState.empty(uuids.get().toString(),
                locationUri, now, tableProperties()));
        Long parent = state.current().map(SnapshotEntry::snapshotId).orElse(null);
        List<ManifestSummary> manifests = new ArrayList<>(carriedManifests(state));
        ManifestWriter.Encoded manifest = ManifestWriter.encode(files, snapshotId);
        String manifestKey = keyPrefix + METADATA_FOLDER + uuids.get() + "-m0.avro";
        store.put(manifestKey, manifest.bytes());
        manifests.add(new ManifestSummary(uriOf(manifestKey), manifest.bytes().length, sequence, sequence,
                snapshotId, manifest.files(), 0, 0, manifest.rows(), 0, 0, manifest.lowerDay(), manifest.upperDay()));
        String listKey = keyPrefix + METADATA_FOLDER + "snap-" + snapshotId + "-0-" + uuids.get() + ".avro";
        store.put(listKey, ManifestListCodec.encode(manifests, snapshotId, parent, sequence));
        SnapshotEntry snapshot = new SnapshotEntry(snapshotId, parent, sequence, now, uriOf(listKey),
                summary(state, files, extraSummary));
        long version = base.map(Head::version).orElse(0L) + 1;
        TableState next = state.withSnapshot(snapshot, previousMetadata(base));
        return new Prepared(version, metadataKey(version), next.toJson(), next);
    }

    public Head publish(Prepared prepared) {
        PutResult result = store.putIfAbsent(prepared.metadataKey(), prepared.metadata());
        if (result != PutResult.CREATED) {
            Optional<byte[]> existing = store.get(prepared.metadataKey());
            if (existing.isEmpty() || !Arrays.equals(existing.get(), prepared.metadata())) {
                throw new WriterFencedException("table metadata version " + prepared.version()
                        + " was written by another writer");
            }
        }
        store.put(keyPrefix + METADATA_FOLDER + VERSION_HINT,
                Long.toString(prepared.version()).getBytes(StandardCharsets.UTF_8));
        return new Head(prepared.version(), prepared.state(), prepared.metadata());
    }

    public Expiry prepareExpiry(Head base, long olderThanMillis) {
        TableState state = base.state();
        Set<Long> expired = new HashSet<>();
        List<String> deletable = new ArrayList<>();
        for (SnapshotEntry snapshot : state.snapshots()) {
            if (snapshot.snapshotId() != state.currentSnapshotId() && snapshot.timestampMillis() < olderThanMillis) {
                expired.add(snapshot.snapshotId());
                deletable.add(keyOf(snapshot.manifestList()));
            }
        }
        for (MetadataLogEntry entry : state.metadataLog()) {
            if (entry.timestampMillis() < olderThanMillis) {
                deletable.add(keyOf(entry.file()));
            }
        }
        if (expired.isEmpty()) {
            return new Expiry(Optional.empty(), List.of());
        }
        long version = base.version() + 1;
        long now = clockMillis.getAsLong();
        MetadataLogEntry previous = new MetadataLogEntry(state.lastUpdatedMillis(), uriOf(metadataKey(base.version())));
        TableState next = state.withoutSnapshots(expired, previous, now);
        return new Expiry(Optional.of(new Prepared(version, metadataKey(version), next.toJson(), next)), deletable);
    }

    public int removeOrphans(Head head, long committedLsn, long olderThanMillis) {
        List<String> doomed = new ArrayList<>();
        collectUnreferencedData(committedLsn, doomed);
        collectUnreferencedMetadata(head, olderThanMillis, doomed);
        return delete(doomed);
    }

    public int removeOrphanData(long committedLsn) {
        List<String> doomed = new ArrayList<>();
        collectUnreferencedData(committedLsn, doomed);
        return delete(doomed);
    }

    private int delete(List<String> doomed) {
        if (!doomed.isEmpty()) {
            store.deleteAll(doomed);
        }
        return doomed.size();
    }

    public void deleteAll(List<String> keys) {
        if (!keys.isEmpty()) {
            store.deleteAll(keys);
        }
    }

    private void collectUnreferencedData(long committedLsn, List<String> doomed) {
        String after = "";
        while (true) {
            ListPage page = store.list(keyPrefix + DATA_FOLDER, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                Matcher matcher = DATA_FILE.matcher(entry.key().substring((keyPrefix + DATA_FOLDER).length()));
                if (matcher.matches() && Long.parseLong(matcher.group(1)) > committedLsn) {
                    doomed.add(entry.key());
                }
            }
            if (!page.truncated()) {
                return;
            }
            after = page.lastKey();
        }
    }

    private void collectUnreferencedMetadata(Head head, long olderThanMillis, List<String> doomed) {
        Set<String> referenced = referencedMetadataKeys(head);
        String after = "";
        while (true) {
            ListPage page = store.list(keyPrefix + METADATA_FOLDER, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                String name = entry.key().substring((keyPrefix + METADATA_FOLDER).length());
                boolean avro = name.endsWith(".avro");
                if (avro && !referenced.contains(entry.key()) && entry.lastModifiedMillis() < olderThanMillis) {
                    doomed.add(entry.key());
                }
            }
            if (!page.truncated()) {
                return;
            }
            after = page.lastKey();
        }
    }

    private Set<String> referencedMetadataKeys(Head head) {
        Set<String> referenced = new HashSet<>();
        for (SnapshotEntry snapshot : head.state().snapshots()) {
            referenced.add(keyOf(snapshot.manifestList()));
        }
        for (ManifestSummary manifest : carriedManifests(head.state())) {
            referenced.add(keyOf(manifest.path()));
        }
        return referenced;
    }

    private List<ManifestSummary> carriedManifests(TableState state) {
        Optional<SnapshotEntry> current = state.current();
        if (current.isEmpty()) {
            return List.of();
        }
        String key = keyOf(current.get().manifestList());
        byte[] list = store.get(key).orElseThrow(() -> new IllegalStateException(
                "the manifest list " + key + " of the current snapshot cannot be read"));
        return ManifestListCodec.decode(list);
    }

    private Optional<MetadataLogEntry> previousMetadata(Optional<Head> base) {
        return base.map(head -> new MetadataLogEntry(head.state().lastUpdatedMillis(),
                uriOf(metadataKey(head.version()))));
    }

    private Map<String, String> summary(TableState state, List<DataFile> files, Map<String, String> extra) {
        long addedRecords = files.stream().mapToLong(DataFile::recordCount).sum();
        long addedSize = files.stream().mapToLong(DataFile::fileSizeBytes).sum();
        Map<String, String> previous = state.current().map(SnapshotEntry::summary).orElse(Map.of());
        Map<String, String> summary = new LinkedHashMap<>();
        summary.put("operation", "append");
        summary.put("added-data-files", Integer.toString(files.size()));
        summary.put("added-records", Long.toString(addedRecords));
        summary.put("added-files-size", Long.toString(addedSize));
        summary.put("total-data-files", Long.toString(total(previous, "total-data-files") + files.size()));
        summary.put("total-records", Long.toString(total(previous, "total-records") + addedRecords));
        summary.put("total-files-size", Long.toString(total(previous, "total-files-size") + addedSize));
        summary.putAll(extra);
        return summary;
    }

    private static long total(Map<String, String> summary, String key) {
        String value = summary.get(key);
        return value == null ? 0 : Long.parseLong(value);
    }

    private static Map<String, String> tableProperties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("write.format.default", "parquet");
        properties.put("nodus.table", NodusLogTable.TABLE_NAME);
        return properties;
    }

    private String metadataKey(long version) {
        return keyPrefix + METADATA_FOLDER + "v" + version + ".metadata.json";
    }

    private long metadataVersion(String key) {
        String name = key.substring((keyPrefix + METADATA_FOLDER).length());
        Matcher matcher = METADATA_FILE.matcher(name);
        return matcher.matches() ? Long.parseLong(matcher.group(1)) : 0;
    }

    private static long newSnapshotId() {
        long id = RANDOM.nextLong() & Long.MAX_VALUE;
        return id == 0 ? 1 : id;
    }
}
