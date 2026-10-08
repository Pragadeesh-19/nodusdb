package io.nodusdb.iceberg;

import io.nodusdb.iceberg.IcebergTable.Head;
import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonNumber;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonString;
import io.nodusdb.verify.PythonVerifier;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergPyIcebergTest {

    @TempDir
    Path root;

    private IcebergFixture fixture;

    private JsonObject scan(long version, String... filters) {
        List<String> arguments = new ArrayList<>();
        arguments.add(fixture.metadataUri(version));
        arguments.addAll(List.of(filters));
        Optional<JsonObject> report = PythonVerifier.run(root, "iceberg_scan.py", arguments.toArray(String[]::new));
        Assumptions.assumeTrue(report.isPresent(), "pyiceberg is not available");
        return report.get();
    }

    private static List<Long> longs(JsonArray array) {
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            values.add(((JsonNumber) array.get(i)).asLong());
        }
        return values;
    }

    private static List<String> strings(JsonArray array) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            values.add(((JsonString) array.get(i)).value());
        }
        return values;
    }

    @Test
    void aRealIcebergReaderScansTheFirstCommitRowForRow() throws IOException {
        fixture = new IcebergFixture(root);
        DataFile file = fixture.dataFile(1, 50, IcebergFixture.START_MICROS + 5_000_000L, 3);
        fixture.commit(Optional.empty(), Map.of(), file);

        JsonObject report = scan(1);

        assertEquals(2, report.requireLong("format_version"));
        assertEquals(50, report.requireLong("rows"));
        JsonObject columns = report.requireObject("columns");
        List<Long> lsn = longs(columns.requireArray("lsn"));
        assertEquals(50, lsn.size());
        for (long expected = 1; expected <= 50; expected++) {
            assertTrue(lsn.contains(expected), "lsn " + expected);
        }
        int position = lsn.indexOf(7L);
        assertEquals("add", strings(columns.requireArray("event")).get(lsn.indexOf(1L)));
        assertEquals("remove", strings(columns.requireArray("event")).get(lsn.indexOf(2L)));
        assertEquals("d6", strings(columns.requireArray("object_id")).get(position));
        assertEquals("u6", strings(columns.requireArray("subject_id")).get(position));
        assertEquals(1_700_000_000_000_000L - 1_700_000_000_000_000L % IcebergFixture.DAY_MICROS + 5_000_000L + 6_000_000L,
                longs(columns.requireArray("commit_ts")).get(position));
        assertEquals(3, longs(columns.requireArray("schema_version")).get(position));
    }

    @Test
    void theSchemaPartitionSpecAndSnapshotAreWhatTheWedgeDesignSays() throws IOException {
        fixture = new IcebergFixture(root);
        fixture.commit(Optional.empty(), Map.of("nodus.projected.lsn", "50"),
                fixture.dataFile(1, 50, IcebergFixture.START_MICROS, 1));

        JsonObject report = scan(1);

        JsonArray schema = report.requireArray("schema");
        String[] names = {"lsn", "commit_ts", "txn_lsn", "epoch", "event", "object_type", "object_id", "relation",
                "subject_type", "subject_id", "subject_relation", "schema_version", "detail"};
        String[] types = {"long", "timestamptz", "long", "long", "string", "string", "string", "string", "string",
                "string", "string", "int", "string"};
        assertEquals(13, schema.size());
        for (int i = 0; i < 13; i++) {
            JsonObject field = (JsonObject) schema.get(i);
            assertEquals(i + 1, field.requireLong("id"));
            assertEquals(names[i], field.requireString("name"));
            assertEquals(types[i], field.requireString("type"));
            assertTrue(field.boolOr("required", false), names[i] + " must be required");
        }
        JsonObject partition = (JsonObject) report.requireArray("spec").get(0);
        assertEquals("commit_ts_day", partition.requireString("name"));
        assertEquals("day", partition.requireString("transform").replaceAll("\\(.*", ""));
        assertEquals(2, partition.requireLong("source_id"));
        assertEquals(1000, partition.requireLong("field_id"));
        JsonObject snapshot = (JsonObject) report.requireArray("snapshots").get(0);
        JsonObject summary = snapshot.requireObject("summary");
        assertEquals("append", summary.requireString("operation"));
        assertEquals("50", summary.requireString("added-records"));
        assertEquals("1", summary.requireString("total-data-files"));
        assertEquals("50", summary.requireString("nodus.projected.lsn"));
    }

    @Test
    void theDataFileCarriesPartitionValuesMetricsAndTheSortOrder() throws IOException {
        fixture = new IcebergFixture(root);
        fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 50, IcebergFixture.START_MICROS, 1));

        JsonObject report = scan(1);

        JsonObject file = (JsonObject) report.requireArray("files").get(0);
        assertEquals(50, file.requireLong("records"));
        assertTrue(file.requireLong("size") > 0);
        assertEquals(1, file.requireLong("sort_order_id"));
        JsonObject counts = file.requireObject("value_counts");
        assertEquals(50, counts.requireLong("1"));
        assertEquals(50, counts.requireLong("13"));
        assertEquals("0100000000000000", file.requireObject("lower").requireObject("1").requireString("hex"));
        assertEquals("3200000000000000", file.requireObject("upper").requireObject("1").requireString("hex"));
        assertEquals("01000000", file.requireObject("upper").requireObject("12").requireString("hex"));
        assertEquals("01000000", file.requireObject("lower").requireObject("12").requireString("hex"));
    }

    @Test
    void aSecondCommitAddsASnapshotAndKeepsTheEarlierManifest() throws IOException {
        fixture = new IcebergFixture(root);
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 30, IcebergFixture.START_MICROS, 1));
        Head second = fixture.commit(Optional.of(first), Map.of(),
                fixture.dataFile(31, 20, IcebergFixture.START_MICROS + 60_000_000L, 1));

        JsonObject report = scan(second.version());

        assertEquals(2, second.version());
        assertEquals(50, report.requireLong("rows"));
        assertEquals(2, report.requireArray("files").size());
        assertEquals(2, report.requireArray("snapshots").size());
        JsonObject newest = (JsonObject) report.requireArray("snapshots").get(1);
        assertEquals(report.requireLong("current_snapshot_id"), newest.requireLong("id"));
        assertEquals(((JsonObject) report.requireArray("snapshots").get(0)).requireLong("id"),
                newest.requireLong("parent"));
        assertEquals(2, newest.requireLong("sequence"));
        assertEquals("2", newest.requireObject("summary").requireString("total-data-files"));
        assertEquals("50", newest.requireObject("summary").requireString("total-records"));
    }

    @Test
    void everyEarlierMetadataVersionStillScansToWhatItHeld() throws IOException {
        fixture = new IcebergFixture(root);
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 30, IcebergFixture.START_MICROS, 1));
        fixture.commit(Optional.of(first), Map.of(), fixture.dataFile(31, 20, IcebergFixture.START_MICROS, 1));

        assertEquals(30, scan(1).requireLong("rows"));
        assertEquals(50, scan(2).requireLong("rows"));
    }

    @Test
    void afterExpiringOldSnapshotsEveryRowIsStillScannedFromTheRemainingOne() throws IOException {
        fixture = new IcebergFixture(root);
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, IcebergFixture.START_MICROS, 1));
        fixture.clock.addAndGet(10_000);
        Head second = fixture.commit(Optional.of(first), Map.of(),
                fixture.dataFile(11, 10, IcebergFixture.START_MICROS + 60_000_000L, 1));
        fixture.clock.addAndGet(10_000);
        Head third = fixture.commit(Optional.of(second), Map.of(),
                fixture.dataFile(21, 10, IcebergFixture.START_MICROS + 120_000_000L, 1));
        IcebergTable.Expiry expiry = fixture.table.prepareExpiry(third,
                third.state().current().orElseThrow().timestampMillis() - 1);
        Head after = fixture.table.publish(expiry.prepared().orElseThrow());
        fixture.table.deleteAll(expiry.deletable());

        JsonObject report = scan(after.version());

        assertEquals(30, report.requireLong("rows"));
        assertEquals(1, report.requireArray("snapshots").size());
        assertEquals(3, report.requireArray("files").size());
    }

    @Test
    void theManifestAndTheManifestListAreValidAvroWithTheRequiredMetadataAndFieldIds() throws IOException {
        fixture = new IcebergFixture(root);
        Head head = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, IcebergFixture.START_MICROS, 1));
        TableState.SnapshotEntry snapshot = head.state().current().orElseThrow();
        Path list = fixture.bucket.resolve(fixture.table.keyOf(snapshot.manifestList()));
        ManifestSummary manifestSummary = ManifestListCodec.decode(java.nio.file.Files.readAllBytes(list)).get(0);
        Path manifest = fixture.bucket.resolve(fixture.table.keyOf(manifestSummary.path()));

        Optional<JsonObject> listDump = PythonVerifier.run(root, "avro_dump.py", list.toString());
        Optional<JsonObject> manifestDump = PythonVerifier.run(root, "avro_dump.py", manifest.toString());
        Assumptions.assumeTrue(listDump.isPresent() && manifestDump.isPresent(), "fastavro is not available");

        JsonObject listMetadata = listDump.get().requireObject("metadata");
        assertEquals(Long.toString(snapshot.snapshotId()), listMetadata.requireString("snapshot-id"));
        assertEquals("null", listMetadata.requireString("parent-snapshot-id"));
        assertEquals("1", listMetadata.requireString("sequence-number"));
        assertEquals("2", listMetadata.requireString("format-version"));
        JsonObject entry = (JsonObject) listDump.get().requireArray("records").get(0);
        assertEquals(fixture.table.uriOf(fixture.table.keyOf(manifestSummary.path())), entry.requireString("manifest_path"));
        assertEquals(manifestSummary.length(), entry.requireLong("manifest_length"));
        assertEquals(10, entry.requireLong("added_rows_count"));
        assertEquals(1, entry.requireLong("added_files_count"));
        JsonObject manifestMetadata = manifestDump.get().requireObject("metadata");
        for (String key : new String[]{"schema", "schema-id", "partition-spec", "partition-spec-id",
                "format-version", "content"}) {
            assertTrue(manifestMetadata.has(key), "manifest metadata lacks " + key);
        }
        assertEquals("data", manifestMetadata.requireString("content"));
        assertTrue(manifestMetadata.requireString("schema").contains("\"commit_ts\""));
        assertTrue(manifestMetadata.requireString("partition-spec").contains("\"day\""));
        JsonObject manifestEntry = (JsonObject) manifestDump.get().requireArray("records").get(0);
        assertEquals(1, manifestEntry.requireLong("status"));
        assertEquals(snapshot.snapshotId(), manifestEntry.requireLong("snapshot_id"));
        JsonObject dataFile = manifestEntry.requireObject("data_file");
        assertEquals("PARQUET", dataFile.requireString("file_format"));
        assertEquals(10, dataFile.requireLong("record_count"));
        assertEquals(13, dataFile.requireArray("column_sizes").size());
        assertEquals(13, dataFile.requireArray("value_counts").size());
    }

    @Test
    void filesOnDifferentDaysCarryTheirOwnPartitionValue() throws IOException {
        fixture = new IcebergFixture(root);
        DataFile dayOne = fixture.dataFile(1, 10, IcebergFixture.START_MICROS + 1_000_000L, 1);
        DataFile dayThree = fixture.dataFile(11, 10, IcebergFixture.START_MICROS + 2 * IcebergFixture.DAY_MICROS, 1);
        fixture.commit(Optional.empty(), Map.of(), dayOne, dayThree);

        JsonObject report = scan(1);

        assertEquals(20, report.requireLong("rows"));
        assertEquals(2, report.requireArray("files").size());
        int firstDay = NodusLogTable.dayOf(IcebergFixture.START_MICROS);
        assertEquals(firstDay, dayOne.partitionDay());
        assertEquals(firstDay + 2, dayThree.partitionDay());
    }

    @Test
    void filtersPruneFilesUsingTheBoundsWeWrite() throws IOException {
        fixture = new IcebergFixture(root);
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, IcebergFixture.START_MICROS, 1));
        Head second = fixture.commit(Optional.of(first), Map.of(),
                fixture.dataFile(11, 10, IcebergFixture.START_MICROS + IcebergFixture.DAY_MICROS, 1));

        JsonObject report = scan(second.version(), "lsn > 15", "lsn > 1000", "lsn <= 3");

        JsonObject filters = report.requireObject("filters");
        assertEquals(1, filters.requireObject("lsn > 15").requireLong("files"));
        assertEquals(5, filters.requireObject("lsn > 15").requireLong("rows"));
        assertEquals(0, filters.requireObject("lsn > 1000").requireLong("files"));
        assertEquals(0, filters.requireObject("lsn > 1000").requireLong("rows"));
        assertEquals(1, filters.requireObject("lsn <= 3").requireLong("files"));
        assertEquals(3, filters.requireObject("lsn <= 3").requireLong("rows"));
    }
}
