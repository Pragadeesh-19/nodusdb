package io.nodusdb.projection;

import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.SignedRecord;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.iceberg.EdgeLogRows;
import io.nodusdb.iceberg.IcebergTable;
import io.nodusdb.iceberg.TableState;
import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.FaultyObjectStore.SimulatedCrash;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.projection.EdgeLogProjector.Cadence;
import io.nodusdb.projection.ProjectionRig.Row;
import io.nodusdb.projection.StreamScript.Chunk;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ChainRing;
import io.nodusdb.ship.ShipState;
import io.nodusdb.ship.ShipState.ProjectionPhase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeLogProjectorRecoveryTest {

    private static final long T0 = ProjectionRig.DAY_ZERO + 3_600_000_000L;
    private static final String METADATA_V1 = "iceberg/metadata/v1.metadata.json";

    @TempDir
    Path root;

    private ProjectionRig rig;
    private EdgeLogProjector projector;

    @BeforeEach
    void setUp() {
        open(ProjectionSettings.defaults());
    }

    private void open(ProjectionSettings settings) {
        rig = new ProjectionRig(root, settings);
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").relation(1, "viewer");
        projector = rig.projector();
    }

    private Chunk add(long micros) {
        return rig.script.transaction(micros, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));
    }

    private void commit(Chunk chunk) {
        rig.ship(chunk);
        assertTrue(projector.drain());
    }

    private long metadataFiles(String suffix) throws IOException {
        try (Stream<Path> files = Files.list(rig.bucket.resolve("iceberg/metadata"))) {
            return files.filter(file -> file.getFileName().toString().endsWith(suffix)).count();
        }
    }

    @Test
    void aRestartedProjectorResumesAfterTheLastCommitWithoutDuplicates() {
        commit(add(T0));
        projector = rig.restart();

        commit(add(T0 + 1));

        assertEquals(List.of(11L, 13L), rig.rows().stream().map(Row::lsn).toList());
        assertEquals(2, rig.table.load().orElseThrow().version());
        assertEquals(2, rig.table.load().orElseThrow().state().snapshots().size());
    }

    @Test
    void aRestartTakesTheTenureAndSchemaVersionFromTheTableNotFromTheKernel() {
        commit(rig.script.transaction(T0, b -> b.epoch(7, 3, 20)));
        rig.names.epoch = 99;
        rig.names.version = 50;
        projector = rig.restart();

        commit(add(T0 + 1));

        List<Row> rows = rig.rows();
        assertEquals(7, rows.get(1).epoch());
        assertEquals(1, rows.get(1).schemaVersion());
    }

    @Test
    void aRestartWithNothingNewCommitsNothing() {
        commit(add(T0));
        projector = rig.restart();

        assertTrue(projector.drain());

        assertEquals(1, rig.table.load().orElseThrow().version());
    }

    @Test
    void commitRecordsAreSignedLinkedAndDescribeTheirFiles() throws IOException {
        commit(add(T0));
        commit(add(T0 + 1));
        commit(add(T0 + 2));
        TableState state = rig.table.load().orElseThrow().state();
        ChainHash previous = null;
        long expectedFirst = 11;

        for (TableState.SnapshotEntry snapshot : state.snapshots()) {
            SignedRecord.Opened opened = SignedRecord.open(ChainBuilder.keyring(), CommitRecord.DOMAIN,
                    snapshot.summary().get(CommitRecord.PROPERTY));
            JsonObject payload = JsonParser.parseObject(opened.payload());

            assertEquals(ChainBuilder.KEY_ID, opened.keyId());
            assertEquals(1, payload.requireLong("v"));
            assertEquals(previous == null ? "" : previous.hex(), payload.requireString("prev"));
            assertEquals(expectedFirst, payload.requireLong("lsn_first"));
            assertEquals(Long.parseLong(snapshot.summary().get(EdgeLogProjector.PROJECTED_LSN)),
                    payload.requireLong("lsn_last"));
            assertEquals(Long.parseLong(snapshot.summary().get(EdgeLogProjector.PROJECTED_CHAIN_SEQ)),
                    payload.requireLong("chain_seq"));
            JsonArray files = payload.requireArray("files");
            assertEquals(1, files.size());
            JsonObject file = (JsonObject) files.get(0);
            Path local = rig.bucket.resolve(rig.table.keyOf(file.requireString("path")));
            assertEquals(ChainHash.sha256(Files.readAllBytes(local)).hex(), file.requireString("sha256"));
            previous = ChainHash.sha256(opened.payload());
            expectedFirst = payload.requireLong("lsn_last") + 1;
        }
        assertEquals(3, state.snapshots().size());
    }

    @Test
    void aTamperedCommitRecordIsRejected() {
        commit(add(T0));
        String sealed = rig.table.load().orElseThrow().state().current().orElseThrow().summary()
                .get(CommitRecord.PROPERTY);
        String[] parts = sealed.split("\\.");
        String forged = (parts[0].charAt(0) == 'A' ? "B" : "A") + parts[0].substring(1) + "." + parts[1] + "."
                + parts[2];

        assertThrows(RuntimeException.class, () -> SignedRecord.open(ChainBuilder.keyring(), CommitRecord.DOMAIN,
                forged));
    }

    @Test
    void aTransientPublishFailureIsRetriedWithTheSamePreparedCommit() throws IOException {
        rig.ship(add(T0));
        rig.run(projector);
        rig.faulty.failWhen(call -> call.operation() == Operation.PUT_IF_ABSENT && call.key().equals(METADATA_V1), 2,
                Fault.FAIL_BEFORE);
        rig.advance(61);

        assertEquals(Cadence.BACKOFF, projector.step().cadence());
        assertEquals(Cadence.BACKOFF, projector.step().cadence());
        assertEquals(ProjectionPhase.RETRYING, rig.state.snapshot().projection().phase());
        rig.run(projector);

        assertEquals(1, rig.table.load().orElseThrow().version());
        assertEquals(1, rig.rows().size());
        assertEquals(1, rig.dataFiles());
        assertEquals(2, metadataFiles(".avro"));
        assertEquals(ProjectionPhase.ACTIVE, rig.state.snapshot().projection().phase());
    }

    @Test
    void aCommitThatLandedBeforeTheErrorIsAdopted() {
        rig.ship(add(T0));
        rig.run(projector);
        rig.faulty.failWhen(call -> call.operation() == Operation.PUT_IF_ABSENT && call.key().equals(METADATA_V1), 1,
                Fault.FAIL_AFTER);
        rig.advance(61);

        assertEquals(Cadence.BACKOFF, projector.step().cadence());
        rig.run(projector);

        assertEquals(1, rig.table.load().orElseThrow().version());
        assertEquals(1, rig.rows().size());
        assertEquals(1, rig.state.snapshot().projection().commits());
    }

    @Test
    void aCrashBeforeTheFirstMetadataLeavesOrphanFilesThatTheNextStartRemoves() {
        rig.ship(add(T0));
        rig.run(projector);
        rig.faulty.failWhen(call -> call.operation() == Operation.PUT_IF_ABSENT && call.key().equals(METADATA_V1), 1,
                Fault.CRASH_BEFORE);
        rig.advance(61);

        assertThrows(SimulatedCrash.class, () -> rig.run(projector));
        assertEquals(1, rig.dataFiles());
        assertFalse(rig.table.load().isPresent());
        projector = rig.restart();
        assertTrue(projector.drain());

        assertEquals(1, rig.dataFiles());
        assertEquals(1, rig.rows().size());
        assertEquals(1, rig.table.load().orElseThrow().version());
    }

    @Test
    void aCrashAfterALaterCommitLeavesOnlyThatBatchesFilesAsOrphans() {
        commit(add(T0));
        rig.ship(add(T0 + 1));
        rig.faulty.failWhen(call -> call.operation() == Operation.PUT_IF_ABSENT
                && call.key().equals("iceberg/metadata/v2.metadata.json"), 1, Fault.CRASH_BEFORE);
        rig.advance(61);

        assertThrows(SimulatedCrash.class, () -> rig.run(projector));
        assertEquals(2, rig.dataFiles());
        projector = rig.restart();
        assertTrue(projector.drain());

        assertEquals(2, rig.dataFiles());
        assertEquals(List.of(11L, 13L), rig.rows().stream().map(Row::lsn).toList());
    }

    @Test
    void aStoreOutageWhileUploadingADataFileKeepsTheBufferAndRetries() {
        rig.ship(add(T0));
        rig.run(projector);
        rig.faulty.failNext(Operation.PUT_FILE, 2, Fault.FAIL_BEFORE);
        rig.advance(61);

        assertEquals(Cadence.BACKOFF, projector.step().cadence());
        assertEquals(Cadence.BACKOFF, projector.step().cadence());
        rig.run(projector);

        assertEquals(1, rig.rows().size());
        assertEquals(1, rig.dataFiles());
        assertEquals(1, rig.table.load().orElseThrow().version());
    }

    @Test
    void aSecondWriterThatCommittedFirstFencesTheProjector() {
        EdgeLogProjector other = rig.projector();
        assertEquals(Cadence.IDLE, rig.run(projector).cadence());
        rig.ship(add(T0));
        assertTrue(other.drain());

        rig.advance(61);
        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.STOP, next.cadence());
        assertEquals(ProjectionPhase.FENCED, rig.state.snapshot().projection().phase());
        assertEquals(Cadence.STOP, projector.step().cadence());
        assertEquals(1, rig.table.load().orElseThrow().version());
    }

    @Test
    void anObjectThatDoesNotLinkToTheChainFailsTheProjector() {
        commit(add(T0));
        ChainBuilder foreign = new ChainBuilder(new MemoryObjectStore(), 9).epoch(1);
        foreign.snapshotRef(10, 1);
        foreign.records(add(T0).records(), 11, 12);
        ChainObject forged = foreign.records(rig.script.transaction(T0 + 1, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1,
                0, 1)).records(), 13, 14);
        rig.state.ring().put(forged);
        rig.state.shipped(14, forged.seq(), 1, 0);

        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(ProjectionPhase.FAILED, rig.state.snapshot().projection().phase());
        assertTrue(rig.state.snapshot().projection().lastError().contains("does not extend"),
                rig.state.snapshot().projection().lastError());
    }

    @Test
    void anObjectThatIsShippedButCannotBeReadIsRetriedAndThenProcessed() {
        Chunk chunk = add(T0);
        ChainObject object = rig.chain.records(chunk.records(), chunk.lsnFirst(), chunk.lsnLast());
        rig.store.delete(ChainLayout.chainKey(object.seq()));
        rig.state.shipped(chunk.lsnLast(), object.seq(), object.encoded().length, 0);
        rig.advance(61);

        EdgeLogProjector.Next missing = rig.run(projector);

        assertEquals(Cadence.BACKOFF, missing.cadence());
        assertEquals(ProjectionPhase.RETRYING, rig.state.snapshot().projection().phase());
        rig.store.put(ChainLayout.chainKey(object.seq()), object.encoded());
        rig.advance(61);
        assertTrue(projector.drain());
        assertEquals(1, rig.rows().size());
    }

    @Test
    void theRingIsReadBeforeTheStoreAndTheStoreAfterARestart() {
        commit(add(T0));
        long withRing = rig.faulty.calls().stream().filter(call -> call.operation() == Operation.GET
                && call.key().startsWith(ChainLayout.CHAIN_PREFIX)).count();
        assertEquals(0, withRing);

        projector = rig.restart();
        commit(add(T0 + 1));

        long afterRestart = rig.faulty.calls().stream().filter(call -> call.operation() == Operation.GET
                && call.key().startsWith(ChainLayout.CHAIN_PREFIX)).count();
        assertEquals(1, afterRestart, "only the last projected object is read from the store");
    }

    @Test
    void aStoredObjectWithAFlippedByteIsRefused() {
        commit(add(T0));
        Chunk chunk = add(T0 + 1);
        ChainObject object = rig.ship(chunk);
        byte[] bytes = rig.store.get(ChainLayout.chainKey(object.seq())).orElseThrow();
        bytes[bytes.length / 2] ^= 0x01;
        rig.store.put(ChainLayout.chainKey(object.seq()), bytes);
        projector = rig.restart();

        rig.advance(61);
        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(ProjectionPhase.FAILED, rig.state.snapshot().projection().phase());
    }

    @Test
    void aStoredObjectKeptUnderTheWrongSequenceNumberIsRefused() {
        commit(add(T0));
        rig.ship(add(T0 + 1));
        byte[] previous = rig.store.get(ChainLayout.chainKey(2)).orElseThrow();
        rig.store.put(ChainLayout.chainKey(3), previous);
        projector = rig.restart();
        rig.advance(61);

        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(ProjectionPhase.FAILED, rig.state.snapshot().projection().phase());
        assertTrue(rig.state.snapshot().projection().lastError().contains("carries sequence number 2"),
                rig.state.snapshot().projection().lastError());
    }

    @Test
    void aStoredObjectSignedByAnotherKeyUnderTheTrustedIdIsRefused() {
        commit(add(T0));
        Chunk chunk = add(T0 + 1);
        ChainObject genuine = rig.chain.records(chunk.records(), chunk.lsnFirst(), chunk.lsnLast());
        SigningKey impostor = new SigningKey(ChainBuilder.KEY_ID, KeyFiles.generate().getPrivate());
        ChainObject forged = ChainCodec.seal(genuine.header(), genuine.body(), impostor);
        rig.store.put(ChainLayout.chainKey(genuine.seq()), forged.encoded());
        rig.state.shipped(chunk.lsnLast(), genuine.seq(), 1, 0);
        projector = rig.restart();

        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertTrue(rig.state.snapshot().projection().lastError().contains("does not verify"),
                rig.state.snapshot().projection().lastError());
    }

    @Test
    void oldSnapshotsExpireOnceTheyAreOlderThanTheRetention() {
        open(ProjectionSettings.defaults().withSnapshotRetention(Duration.ofHours(1)));
        commit(add(T0));
        rig.advance(2 * 3_600);

        commit(add(T0 + 1));

        TableState state = rig.table.load().orElseThrow().state();
        assertEquals(1, state.snapshots().size());
        assertEquals(3, rig.table.load().orElseThrow().version());
        assertEquals(2, rig.rows().size());
        assertEquals(2, rig.dataFiles());
    }

    @Test
    void snapshotsWithinTheRetentionAreKept() {
        commit(add(T0));
        rig.advance(3_600);

        commit(add(T0 + 1));

        assertEquals(2, rig.table.load().orElseThrow().state().snapshots().size());
    }

    @Test
    void aFailureToDeleteExpiredFilesIsRetriedWithoutCommittingAgain() {
        open(ProjectionSettings.defaults().withSnapshotRetention(Duration.ofHours(1)));
        commit(add(T0));
        rig.advance(2 * 3_600);
        rig.ship(add(T0 + 1));
        rig.faulty.failNext(Operation.DELETE, Fault.FAIL_BEFORE);

        rig.run(projector);
        assertTrue(projector.drain());

        assertEquals(3, rig.table.load().orElseThrow().version());
        assertEquals(1, rig.table.load().orElseThrow().state().snapshots().size());
    }

    @Test
    void orphanDataFilesBeyondTheCommittedLsnAreRemovedWhenTheProjectorStarts() throws IOException {
        commit(add(T0));
        String orphan = rig.table.newDataKey(500, 600);
        rig.store.put(orphan, new byte[]{1, 2, 3});
        int before = rig.dataFiles();
        projector = rig.restart();

        assertTrue(projector.drain());

        assertFalse(rig.store.exists(orphan));
        assertEquals(before - 1, rig.dataFiles());
        assertEquals(1, rig.rows().size());
    }

    @Test
    void aPeriodicSweepNeverRemovesTheFilesOfABatchThatIsNotYetCommitted() {
        open(ProjectionSettings.defaults().withFlushRows(1).withOrphans(Duration.ofSeconds(5), Duration.ofSeconds(1)));
        commit(add(T0));
        rig.ship(add(T0 + 1));
        rig.run(projector);
        assertEquals(2, rig.dataFiles());
        assertEquals(1, rig.state.snapshot().projection().commits());
        rig.advance(10);

        rig.run(projector);

        assertEquals(2, rig.dataFiles());
        rig.advance(61);
        assertTrue(projector.drain());
        assertEquals(2, rig.rows().size());
        assertEquals(2, rig.dataFiles());
    }

    @Test
    void aProjectorStartedAfterTheEarlyObjectsWereRetainedAwayAdoptsTheOldestOne() {
        rig.ship(add(T0));
        rig.ship(add(T0 + 1));
        rig.ship(add(T0 + 2));
        rig.store.delete(ChainLayout.chainKey(1));
        rig.store.delete(ChainLayout.chainKey(2));
        EdgeLogProjector late = rig.projector(new StoreChainSource(new ChainRing(1), rig.faulty,
                ChainBuilder.keyring()));

        assertTrue(late.drain());

        assertEquals(List.of(13L, 15L), rig.rows().stream().map(Row::lsn).toList());
    }

    @Test
    void aTableThatNodusdbDidNotWriteIsRefused() {
        rig.store.put(METADATA_V1, "{\"format-version\":2,\"last-column-id\":13}".getBytes(StandardCharsets.UTF_8));

        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(ProjectionPhase.FAILED, rig.state.snapshot().projection().phase());
    }

    @Test
    void aTableWithoutOurPositionPropertiesIsRefused() throws IOException {
        EdgeLogRows rows = new EdgeLogRows();
        rows.add(1, T0, 1, 1, "add", "a", "b", "c", "d", "e", "", 1, "");
        IcebergTable.Prepared foreign = rig.table.prepareAppend(Optional.empty(), List.of(rig.writer.write(rows, 1)),
                Map.of());
        rig.table.publish(foreign);

        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertTrue(rig.state.snapshot().projection().lastError().contains(EdgeLogProjector.PROJECTED_LSN),
                rig.state.snapshot().projection().lastError());
    }

    @Test
    void theProjectionStatusFollowsTheLifecycle() {
        assertEquals(ProjectionPhase.DISABLED, rig.state.snapshot().projection().phase());

        rig.run(projector);
        assertEquals(ProjectionPhase.ACTIVE, rig.state.snapshot().projection().phase());

        commit(add(T0));
        ShipState.ProjectionStatus status = rig.state.snapshot().projection();
        assertEquals(1, status.commits());
        assertEquals(0, status.failures());
        assertEquals("", status.lastError());
        assertEquals(2, status.projectedSeq());
        assertEquals(2, rig.state.projectedSeq());
    }
}
