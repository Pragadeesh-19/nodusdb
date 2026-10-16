package io.nodusdb.iceberg;

import io.nodusdb.iceberg.IcebergTable.Head;
import io.nodusdb.lake.parquet.ColumnMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergOverwriteTest {

    @TempDir
    Path root;

    private IcebergFixture fixture;

    private static long micros(int dayOffset, int second) {
        return IcebergFixture.START_MICROS + dayOffset * IcebergFixture.DAY_MICROS + second * 1_000_000L;
    }

    private DataFile rewritten(DataFile original) throws IOException {
        List<EdgeLogRow> rows = new ArrayList<>();
        for (EdgeLogRow row : new DataFileReader(fixture.store, fixture.table).read(original.path())) {
            rows.add(row.withSubject("erased", "0123456789abcdef"));
        }
        return fixture.writer.write(DataFileReader.rowsOf(rows), original.partitionDay());
    }

    private Set<String> livePaths(Head head) {
        Set<String> paths = new HashSet<>();
        for (ManifestSummary manifest : fixture.table.manifests(head)) {
            for (ManifestEntry entry : fixture.table.entries(manifest)) {
                if (entry.live()) {
                    paths.add(entry.path());
                }
            }
        }
        return paths;
    }

    private static void assertSameMetrics(List<ColumnMetrics> expected, List<ColumnMetrics> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            ColumnMetrics left = expected.get(i);
            ColumnMetrics right = actual.get(i);
            assertEquals(left.fieldId(), right.fieldId());
            assertEquals(left.name(), right.name());
            assertEquals(left.valueCount(), right.valueCount());
            assertEquals(left.nullCount(), right.nullCount());
            assertEquals(left.compressedBytes(), right.compressedBytes());
            assertArrayEquals(left.lowerBound(), right.lowerBound());
            assertArrayEquals(left.upperBound(), right.upperBound());
        }
    }

    @Test
    void anAppendedManifestReadsBackEntryForEntry() throws IOException {
        fixture = new IcebergFixture(root);
        DataFile first = fixture.dataFile(1, 10, micros(0, 0), 1);
        DataFile second = fixture.dataFile(11, 10, micros(1, 0), 1);
        Head head = fixture.commit(Optional.empty(), Map.of(), first, second);

        List<ManifestEntry> entries = fixture.table.entries(fixture.table.manifests(head).get(0));

        assertEquals(2, entries.size());
        for (int i = 0; i < 2; i++) {
            DataFile written = i == 0 ? first : second;
            ManifestEntry entry = entries.get(i);
            assertEquals(ManifestEntry.Status.ADDED, entry.status());
            assertEquals(head.state().currentSnapshotId(), entry.snapshotId());
            assertEquals(1, entry.dataSequenceNumber());
            assertEquals(1, entry.fileSequenceNumber());
            assertEquals(written.path(), entry.path());
            assertEquals(written.partitionDay(), entry.partitionDay());
            assertEquals(written.recordCount(), entry.recordCount());
            assertEquals(written.fileSizeBytes(), entry.fileSizeBytes());
            assertSameMetrics(written.columns(), entry.columns());
        }
    }

    @Test
    void anOverwriteReplacesOneFileAndCarriesTheOtherManifestsUnchanged() throws IOException {
        fixture = new IcebergFixture(root);
        DataFile a = fixture.dataFile(1, 10, micros(0, 0), 1);
        DataFile b = fixture.dataFile(11, 10, micros(0, 20), 1);
        DataFile c = fixture.dataFile(21, 10, micros(0, 40), 1);
        Head first = fixture.commit(Optional.empty(), Map.of(), a);
        Head second = fixture.commit(Optional.of(first), Map.of(), b);
        Head third = fixture.commit(Optional.of(second), Map.of("nodus.projected.lsn", "30"), c);
        List<ManifestSummary> before = fixture.table.manifests(third);
        DataFile replacement = rewritten(b);

        IcebergTable.Overwrite overwrite = fixture.table.prepareOverwrite(third, Map.of(b.path(), replacement),
                Map.of("nodus.projected.lsn", "30"));
        Head after = fixture.table.publish(overwrite.prepared());

        assertEquals(List.of(fixture.table.keyOf(before.get(1).path())), overwrite.replacedManifests());
        assertEquals(Set.of(a.path(), replacement.path(), c.path()), livePaths(after));
        List<ManifestSummary> manifests = fixture.table.manifests(after);
        assertEquals(before.get(0), manifests.get(0));
        assertEquals(before.get(2), manifests.get(1));
        ManifestSummary written = manifests.get(2);
        assertEquals(1, written.addedFiles());
        assertEquals(1, written.deletedFiles());
        assertEquals(0, written.existingFiles());
        Map<String, String> summary = after.state().current().orElseThrow().summary();
        assertEquals("overwrite", summary.get("operation"));
        assertEquals("3", summary.get("total-data-files"));
        assertEquals("30", summary.get("total-records"));
        assertEquals("30", summary.get("nodus.projected.lsn"));
        assertEquals(4, after.state().lastSequenceNumber());
        List<EdgeLogRow> rows = new DataFileReader(fixture.store, fixture.table).read(replacement.path());
        assertEquals(10, rows.size());
        assertTrue(rows.stream().allMatch(row -> row.subjectType().equals("erased")));
    }

    @Test
    void theOtherFilesOfAReplacedManifestStayAsExistingWithTheirOriginalSequence() throws IOException {
        fixture = new IcebergFixture(root);
        DataFile a = fixture.dataFile(1, 10, micros(0, 0), 1);
        DataFile b = fixture.dataFile(11, 10, micros(1, 0), 1);
        Head first = fixture.commit(Optional.empty(), Map.of(), a, b);
        Head second = fixture.commit(Optional.of(first), Map.of(), fixture.dataFile(21, 5, micros(1, 30), 1));
        DataFile replacement = rewritten(b);

        Head after = fixture.table.publish(fixture.table.prepareOverwrite(second, Map.of(b.path(), replacement),
                Map.of()).prepared());

        ManifestSummary written = fixture.table.manifests(after).get(1);
        List<ManifestEntry> entries = fixture.table.entries(written);
        ManifestEntry kept = entries.stream().filter(entry -> entry.path().equals(a.path())).findFirst()
                .orElseThrow();
        assertEquals(ManifestEntry.Status.EXISTING, kept.status());
        assertEquals(first.state().currentSnapshotId(), kept.snapshotId());
        assertEquals(1, kept.dataSequenceNumber());
        assertEquals(1, written.minSequenceNumber());
        assertEquals(3, written.sequenceNumber());
        ManifestEntry removed = entries.stream().filter(entry -> entry.path().equals(b.path())).findFirst()
                .orElseThrow();
        assertEquals(ManifestEntry.Status.DELETED, removed.status());
        assertEquals(List.of(), removed.columns());
        assertEquals(after.state().currentSnapshotId(), removed.snapshotId());
        ManifestEntry added = entries.stream().filter(entry -> entry.path().equals(replacement.path())).findFirst()
                .orElseThrow();
        assertEquals(ManifestEntry.Status.ADDED, added.status());
        assertEquals(3, added.dataSequenceNumber());
    }

    @Test
    void replacingAFileTheTableDoesNotHoldIsRefused() throws IOException {
        fixture = new IcebergFixture(root);
        Head head = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));
        DataFile stranger = fixture.dataFile(11, 10, micros(0, 20), 1);

        assertThrows(IllegalStateException.class, () -> fixture.table.prepareOverwrite(head,
                Map.of(stranger.path(), stranger), Map.of()));
    }
}
