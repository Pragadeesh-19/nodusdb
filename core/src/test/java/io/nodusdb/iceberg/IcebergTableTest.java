package io.nodusdb.iceberg;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.iceberg.IcebergTable.Expiry;
import io.nodusdb.iceberg.IcebergTable.Head;
import io.nodusdb.iceberg.IcebergTable.Prepared;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergTableTest {

    @TempDir
    Path root;

    private IcebergFixture fixture() {
        return new IcebergFixture(root);
    }

    private static long micros(int dayOffset, int second) {
        return IcebergFixture.START_MICROS + dayOffset * IcebergFixture.DAY_MICROS + second * 1_000_000L;
    }

    @Test
    void anEmptyBucketHasNoTable() {
        assertEquals(Optional.empty(), fixture().table.load());
    }

    @Test
    void theFirstCommitCreatesVersionOneAndAHint() throws IOException {
        IcebergFixture fixture = fixture();

        Head head = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));

        assertEquals(1, head.version());
        assertEquals(1, head.state().snapshots().size());
        assertEquals(1, head.state().lastSequenceNumber());
        assertEquals(head.state().snapshots().get(0).snapshotId(), head.state().currentSnapshotId());
        assertTrue(fixture.store.exists("iceberg/metadata/v1.metadata.json"));
        assertEquals("1", new String(fixture.store.get("iceberg/metadata/version-hint.text").orElseThrow(),
                StandardCharsets.UTF_8));
        assertEquals(0, head.state().metadataLog().size());
    }

    @Test
    void loadFindsTheHighestVersionWithoutTheHint() throws IOException {
        IcebergFixture fixture = fixture();
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));
        Head second = fixture.commit(Optional.of(first), Map.of(), fixture.dataFile(11, 10, micros(0, 20), 1));
        fixture.store.delete("iceberg/metadata/version-hint.text");

        Head loaded = fixture.table.load().orElseThrow();

        assertEquals(2, loaded.version());
        assertArrayEquals(second.bytes(), loaded.bytes());
        assertEquals(second.state(), loaded.state());
    }

    @Test
    void loadOrdersVersionsNumericallyNotAsText() throws IOException {
        IcebergFixture fixture = fixture();
        Optional<Head> head = Optional.empty();
        for (int i = 0; i < 11; i++) {
            head = Optional.of(fixture.commit(head, Map.of(), fixture.dataFile(1 + i * 5L, 5, micros(0, i * 10), 1)));
        }

        assertEquals(11, fixture.table.load().orElseThrow().version());
    }

    @Test
    void eachCommitKeepsEverySnapshotAndLogsThePreviousMetadataFile() throws IOException {
        IcebergFixture fixture = fixture();
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));
        Head second = fixture.commit(Optional.of(first), Map.of(), fixture.dataFile(11, 10, micros(0, 20), 1));
        Head third = fixture.commit(Optional.of(second), Map.of(), fixture.dataFile(21, 10, micros(0, 40), 1));

        TableState state = third.state();

        assertEquals(3, state.snapshots().size());
        assertEquals(3, state.snapshotLog().size());
        assertEquals(List.of(fixture.metadataUri(1), fixture.metadataUri(2)),
                state.metadataLog().stream().map(TableState.MetadataLogEntry::file).toList());
        assertEquals(state.snapshots().get(1).snapshotId(), state.snapshots().get(2).parentSnapshotId());
        assertEquals(null, state.snapshots().get(0).parentSnapshotId());
        assertEquals(3, state.snapshots().get(2).sequenceNumber());
        assertEquals("30", state.snapshots().get(2).summary().get("total-records"));
        assertEquals("3", state.snapshots().get(2).summary().get("total-data-files"));
    }

    @Test
    void theManifestListOfEachSnapshotCarriesEveryManifestSoFar() throws IOException {
        IcebergFixture fixture = fixture();
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));
        Head second = fixture.commit(Optional.of(first), Map.of(), fixture.dataFile(11, 10, micros(1, 0), 1));

        List<ManifestSummary> list = ManifestListCodec.decode(fixture.store.get(
                fixture.table.keyOf(second.state().current().orElseThrow().manifestList())).orElseThrow());

        assertEquals(2, list.size());
        assertEquals(1, list.get(0).sequenceNumber());
        assertEquals(2, list.get(1).sequenceNumber());
        assertEquals(10, list.get(0).addedRows());
        assertEquals(first.state().current().orElseThrow().snapshotId(), list.get(0).addedSnapshotId());
        assertEquals(second.state().current().orElseThrow().snapshotId(), list.get(1).addedSnapshotId());
        assertEquals(NodusLogTable.dayOf(micros(1, 0)), list.get(1).lowerDay());
    }

    @Test
    void extraSummaryPropertiesAreStoredOnTheSnapshot() throws IOException {
        IcebergFixture fixture = fixture();

        Head head = fixture.commit(Optional.empty(), Map.of("nodus.projected.lsn", "10", "x", "y"),
                fixture.dataFile(1, 10, micros(0, 0), 1));

        Map<String, String> summary = head.state().current().orElseThrow().summary();
        assertEquals("10", summary.get("nodus.projected.lsn"));
        assertEquals("y", summary.get("x"));
        assertEquals("append", summary.get("operation"));
    }

    @Test
    void publishingTheSamePreparedCommitTwiceIsHarmless() throws IOException {
        IcebergFixture fixture = fixture();
        Prepared prepared = fixture.table.prepareAppend(Optional.empty(), List.of(fixture.dataFile(1, 10, micros(0, 0), 1)),
                Map.of());

        Head once = fixture.table.publish(prepared);
        Head twice = fixture.table.publish(prepared);

        assertEquals(once.version(), twice.version());
        assertArrayEquals(once.bytes(), twice.bytes());
    }

    @Test
    void aCommitThatLandedBeforeTheErrorIsAdoptedOnRetry() throws IOException {
        IcebergFixture fixture = fixture();
        FaultyObjectStore faulty = new FaultyObjectStore(fixture.store);
        IcebergTable table = new IcebergTable(faulty, fixture.table.uriOf("iceberg/x").replace("/x", ""),
                IcebergFixture.KEY_PREFIX, fixture.clock::get);
        Prepared prepared = table.prepareAppend(Optional.empty(), List.of(fixture.dataFile(1, 10, micros(0, 0), 1)),
                Map.of());
        faulty.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_AFTER);

        assertThrows(TransientStoreException.class, () -> table.publish(prepared));
        Head head = table.publish(prepared);

        assertEquals(1, head.version());
        assertEquals(1, table.load().orElseThrow().version());
    }

    @Test
    void aVersionWrittenByAnotherWriterFencesThisOne() throws IOException {
        IcebergFixture fixture = fixture();
        DataFile mine = fixture.dataFile(1, 10, micros(0, 0), 1);
        Prepared first = fixture.table.prepareAppend(Optional.empty(), List.of(mine), Map.of());
        Prepared theirs = fixture.table.prepareAppend(Optional.empty(), List.of(mine), Map.of());
        fixture.table.publish(theirs);

        WriterFencedException refused = assertThrows(WriterFencedException.class, () -> fixture.table.publish(first));

        assertTrue(refused.getMessage().contains("version 1"), refused.getMessage());
        assertArrayEquals(theirs.metadata(), fixture.store.get("iceberg/metadata/v1.metadata.json").orElseThrow());
    }

    @Test
    void aFailureToWriteTheHintDoesNotLoseTheCommit() throws IOException {
        IcebergFixture fixture = fixture();
        FaultyObjectStore faulty = new FaultyObjectStore(fixture.store);
        IcebergTable table = new IcebergTable(faulty, fixture.table.uriOf("iceberg/x").replace("/x", ""),
                IcebergFixture.KEY_PREFIX, fixture.clock::get);
        Prepared prepared = table.prepareAppend(Optional.empty(), List.of(fixture.dataFile(1, 10, micros(0, 0), 1)),
                Map.of());
        faulty.failWhen(call -> call.key().endsWith("version-hint.text"), 1, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> table.publish(prepared));

        assertEquals(1, table.load().orElseThrow().version());
        assertEquals(1, table.publish(prepared).version());
    }

    @Test
    void theStateRoundTripsThroughItsJson() throws IOException {
        IcebergFixture fixture = fixture();
        Head first = fixture.commit(Optional.empty(), Map.of("a", "b"), fixture.dataFile(1, 10, micros(0, 0), 1));
        Head second = fixture.commit(Optional.of(first), Map.of(), fixture.dataFile(11, 10, micros(0, 20), 1));

        assertEquals(second.state(), TableState.parse(second.state().toJson()));
        assertEquals(first.state(), TableState.parse(first.state().toJson()));
    }

    @Test
    void metadataOfAnotherKindOfTableIsRefused() {
        byte[] foreign = ("{\"format-version\":1,\"last-column-id\":13}").getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalStateException.class, () -> TableState.parse(foreign));
        assertThrows(IllegalStateException.class, () -> TableState.parse(
                "{\"format-version\":2,\"last-column-id\":3}".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void uriAndKeyConvertBothWaysAndRefuseForeignOnes() {
        IcebergFixture fixture = fixture();

        String uri = fixture.table.uriOf("iceberg/data/a.parquet");

        assertTrue(uri.endsWith("/iceberg/data/a.parquet"));
        assertEquals("iceberg/data/a.parquet", fixture.table.keyOf(uri));
        assertThrows(IllegalArgumentException.class, () -> fixture.table.uriOf("other/data/a.parquet"));
        assertThrows(IllegalArgumentException.class, () -> fixture.table.keyOf("/elsewhere/a.parquet"));
    }

    @Test
    void theLocationAndPrefixMustBeWellFormed() {
        IcebergFixture fixture = fixture();

        assertThrows(IllegalArgumentException.class, () -> new IcebergTable(fixture.store, "/a/b/", "iceberg/",
                fixture.clock::get));
        assertThrows(IllegalArgumentException.class, () -> new IcebergTable(fixture.store, "/a/b", "iceberg",
                fixture.clock::get));
    }

    @Test
    void expiryRemovesOldSnapshotsAndTheirManifestListsButKeepsTheData() throws IOException {
        IcebergFixture fixture = fixture();
        Head first = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));
        fixture.clock.addAndGet(10_000);
        Head second = fixture.commit(Optional.of(first), Map.of(), fixture.dataFile(11, 10, micros(0, 20), 1));
        fixture.clock.addAndGet(10_000);
        Head third = fixture.commit(Optional.of(second), Map.of(), fixture.dataFile(21, 10, micros(0, 40), 1));
        long cutoff = third.state().snapshots().get(2).timestampMillis() - 1;

        Expiry expiry = fixture.table.prepareExpiry(third, cutoff);
        Head after = fixture.table.publish(expiry.prepared().orElseThrow());
        fixture.table.deleteAll(expiry.deletable());

        assertEquals(4, after.version());
        assertEquals(1, after.state().snapshots().size());
        assertEquals(third.state().currentSnapshotId(), after.state().snapshots().get(0).snapshotId());
        assertEquals(1, after.state().snapshotLog().size());
        assertFalse(fixture.store.exists(fixture.table.keyOf(first.state().snapshots().get(0).manifestList())));
        assertFalse(fixture.store.exists(fixture.table.keyOf(second.state().snapshots().get(1).manifestList())));
        assertTrue(fixture.store.exists(fixture.table.keyOf(third.state().current().orElseThrow().manifestList())));
        List<ManifestSummary> list = ManifestListCodec.decode(fixture.store.get(
                fixture.table.keyOf(after.state().current().orElseThrow().manifestList())).orElseThrow());
        assertEquals(3, list.size());
        for (ManifestSummary manifest : list) {
            assertTrue(fixture.store.exists(fixture.table.keyOf(manifest.path())));
        }
    }

    @Test
    void expiryNeverRemovesTheCurrentSnapshotAndDoesNothingWhenNothingIsOld() throws IOException {
        IcebergFixture fixture = fixture();
        Head only = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));

        Expiry tooLate = fixture.table.prepareExpiry(only, Long.MAX_VALUE);
        Expiry tooEarly = fixture.table.prepareExpiry(only, 0);

        assertTrue(tooLate.prepared().isEmpty());
        assertTrue(tooEarly.prepared().isEmpty());
        assertTrue(tooLate.deletable().isEmpty());
    }

    @Test
    void orphanDataFilesBeyondTheCommittedLsnAreRemovedAndCommittedOnesKept() throws IOException {
        IcebergFixture fixture = fixture();
        DataFile committed = fixture.dataFile(1, 10, micros(0, 0), 1);
        Head head = fixture.commit(Optional.empty(), Map.of(), committed);
        DataFile crashed = fixture.dataFile(11, 10, micros(0, 20), 1);

        int removed = fixture.table.removeOrphans(head, 10, 0);

        assertEquals(1, removed);
        assertTrue(fixture.store.exists(fixture.table.keyOf(committed.path())));
        assertFalse(fixture.store.exists(fixture.table.keyOf(crashed.path())));
    }

    @Test
    void aCommittedFileThatStartsAtTheCommittedLsnIsNotAnOrphan() throws IOException {
        IcebergFixture fixture = fixture();
        DataFile single = fixture.dataFile(10, 1, micros(0, 0), 1);
        Head head = fixture.commit(Optional.empty(), Map.of(), single);

        int removed = fixture.table.removeOrphans(head, 10, 0);

        assertEquals(0, removed);
        assertTrue(fixture.store.exists(fixture.table.keyOf(single.path())));
    }

    @Test
    void unreferencedManifestsAreRemovedOnlyAfterTheGracePeriodAndReferencedOnesStay() throws IOException {
        IcebergFixture fixture = fixture();
        Head head = fixture.commit(Optional.empty(), Map.of(), fixture.dataFile(1, 10, micros(0, 0), 1));
        Prepared abandoned = fixture.table.prepareAppend(Optional.of(head), List.of(fixture.dataFile(11, 10,
                micros(0, 20), 1)), Map.of());
        long afterEverything = System.currentTimeMillis() + 60_000;

        int early = fixture.table.removeOrphans(head, 20, 0);
        int late = fixture.table.removeOrphans(head, 20, afterEverything);

        assertEquals(0, early);
        assertEquals(2, late);
        assertTrue(fixture.store.exists(fixture.table.keyOf(head.state().current().orElseThrow().manifestList())));
        assertEquals(2, abandoned.version());
    }

    @Test
    void aNewDataKeySortsByLsnAndIsUnique() {
        IcebergFixture fixture = fixture();

        String a = fixture.table.newDataKey(5, 9);
        String b = fixture.table.newDataKey(5, 9);

        assertTrue(a.startsWith("iceberg/data/00000000000000000005-00000000000000000009-"));
        assertTrue(a.endsWith(".parquet"));
        assertFalse(a.equals(b));
    }

    @Test
    void aDataFileValidatesItsFields() {
        ChainHash hash = ChainHash.sha256(new byte[0]);

        assertThrows(NullPointerException.class, () -> new DataFile(null, 1, 1, 0, List.of(), hash));
        assertThrows(NullPointerException.class, () -> new DataFile("p", 1, 1, 0, List.of(), null));
        assertThrows(IllegalArgumentException.class, () -> new DataFile("p", -1, 1, 0, List.of(), hash));
        assertThrows(IllegalArgumentException.class, () -> new DataFile("p", 1, -1, 0, List.of(), hash));
    }

    @Test
    void aManifestNeedsAtLeastOneFile() {
        assertThrows(IllegalArgumentException.class, () -> ManifestWriter.encode(List.of(), 1));
    }
}
