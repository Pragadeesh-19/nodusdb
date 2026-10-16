package io.nodusdb.projection;

import io.nodusdb.iceberg.IcebergTable;
import io.nodusdb.iceberg.TableState;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.projection.ProjectionRig.Row;
import io.nodusdb.projection.StreamScript.Chunk;
import io.nodusdb.ship.ShipState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeLogProjectorErasureTest {

    private static final long T0 = ProjectionRig.DAY_ZERO + 3_600_000_000L;
    private static final String PSEUDONYM = "erased:0123456789abcdef";
    private static final byte[] ALICE = "alice".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path root;

    private ProjectionRig rig;
    private EdgeLogProjector projector;
    private long eraseLsn;

    @BeforeEach
    void open() {
        rig = new ProjectionRig(root, ProjectionSettings.defaults());
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").symbol(2, "user:bob").relation(1, "viewer");
        projector = rig.projector();
    }

    private void project(Chunk chunk) {
        rig.ship(chunk);
        projector.drain();
    }

    private void grantThenErase() {
        project(rig.script.transaction(T0, b -> {
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1);
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 2);
        }));
        Chunk erase = rig.script.transaction(T0 + 1, b -> {
            b.tuple(RecordType.TUPLE_REMOVE, 0, 1, 0, 1);
            b.erase(1, RecordFixtures.pseudonym());
        });
        project(erase);
        eraseLsn = erase.lsnLast();
        rig.erased.put("user:alice", new ErasedNames.Erased(PSEUDONYM, eraseLsn));
    }

    private void requestErasure(int erasedCount) {
        rig.state.requestLakeErasure(new ShipState.LakeRequest(eraseLsn, erasedCount));
        projector.drain();
    }

    private List<Path> filesHoldingAlice() throws IOException {
        List<Path> holding = new ArrayList<>();
        try (Stream<Path> files = Files.walk(rig.bucket.resolve("iceberg"))) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                byte[] content = Files.readAllBytes(file);
                for (int start = 0; start + ALICE.length <= content.length; start++) {
                    if (Arrays.equals(content, start, start + ALICE.length, ALICE, 0, ALICE.length)) {
                        holding.add(file);
                        break;
                    }
                }
            }
        }
        return holding;
    }

    private TableState.SnapshotEntry current() {
        return rig.table.load().orElseThrow().state().current().orElseThrow();
    }

    private int intents() {
        ListPage page = rig.store.list("iceberg/metadata/redaction-", "", 100);
        return page.entries().size();
    }

    @Test
    void theRowsThatNamedTheErasedSubjectAreRewrittenAndEveryOlderCopyIsGone() throws IOException {
        grantThenErase();
        assertFalse(filesHoldingAlice().isEmpty());

        requestErasure(1);

        List<Row> rows = rig.rows();
        assertTrue(rows.stream().noneMatch(row -> row.subjectId().equals("alice")));
        assertEquals(2, rows.stream().filter(row -> row.subjectType().equals("erased")
                && row.subjectId().equals("0123456789abcdef")).count());
        assertTrue(rows.stream().anyMatch(row -> row.subjectId().equals("bob")));
        assertEquals(List.of(), filesHoldingAlice());
        Map<String, String> summary = current().summary();
        assertEquals("overwrite", summary.get("operation"));
        assertEquals("1/" + eraseLsn, summary.get(LakeRedaction.COVERAGE_PROPERTY));
        assertTrue(summary.containsKey(LakeRedaction.RECORD_PROPERTY));
        assertEquals(1, rig.table.load().orElseThrow().state().snapshots().size());
        assertEquals(new ShipState.LakeCoverage(1, eraseLsn), rig.state.lakeCoverage());
        assertEquals(0, intents());
    }

    @Test
    void theCoverageSurvivesLaterCommitsAndARestart() {
        grantThenErase();
        requestErasure(1);
        project(rig.script.transaction(T0 + 2, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 2)));
        rig.advance(120);
        projector.drain();

        assertEquals("1/" + eraseLsn, current().summary().get(LakeRedaction.COVERAGE_PROPERTY));
        projector = rig.restart();
        projector.drain();
        assertEquals(new ShipState.LakeCoverage(1, eraseLsn), rig.state.lakeCoverage());
    }

    @Test
    void aNameWrittenAgainAfterItsErasureKeepsItsLaterRows() {
        grantThenErase();
        requestErasure(1);
        rig.names.symbol(3, "user:alice");
        project(rig.script.transaction(T0 + 2, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 3)));
        long reborn = rig.script.nextLsn() - 1;
        rig.erased.put("user:bob", new ErasedNames.Erased("erased:fedcba9876543210", reborn));
        eraseLsn = reborn;

        requestErasure(2);

        List<Row> rows = rig.rows();
        assertTrue(rows.stream().anyMatch(row -> row.subjectId().equals("alice") && row.lsn() > reborn - 2));
        assertTrue(rows.stream().noneMatch(row -> row.subjectId().equals("bob")));
    }

    @Test
    void aCrashBeforeTheOverwriteIsPublishedLeavesNothingBehindAndTheRetryFinishes() throws IOException {
        grantThenErase();
        rig.faulty.failWhen(call -> call.operation() == Operation.PUT_IF_ABSENT
                && call.key().endsWith(".metadata.json"), 1, Fault.CRASH_BEFORE);

        rig.state.requestLakeErasure(new ShipState.LakeRequest(eraseLsn, 1));
        assertThrows(FaultyObjectStore.SimulatedCrash.class, () -> projector.drain());
        assertEquals(1, intents());
        projector = rig.restart();
        requestErasure(1);

        assertEquals(List.of(), filesHoldingAlice());
        assertEquals(0, intents());
        assertEquals(rig.rows().size(), rig.rows().stream().map(Row::lsn).distinct().count());
    }

    @Test
    void aCrashWhileDeletingTheReplacedFilesIsFinishedWhenTheProjectorStartsAgain() throws IOException {
        grantThenErase();
        rig.faulty.failWhen(call -> call.operation() == Operation.DELETE && call.key().contains("/data/"), 1,
                Fault.CRASH_BEFORE);

        rig.state.requestLakeErasure(new ShipState.LakeRequest(eraseLsn, 1));
        assertThrows(FaultyObjectStore.SimulatedCrash.class, () -> projector.drain());
        assertFalse(filesHoldingAlice().isEmpty());
        projector = rig.restart();
        projector.drain();

        assertEquals(List.of(), filesHoldingAlice());
        assertEquals(0, intents());
        assertEquals(new ShipState.LakeCoverage(1, eraseLsn), rig.state.lakeCoverage());
    }

    @Test
    void anErasureWhoseNameNeverReachedTheLakeIsRecordedWithoutReplacingFiles() {
        grantThenErase();
        rig.erased.clear();
        int files = rig.dataFiles();
        int snapshots = rig.table.load().orElseThrow().state().snapshots().size();

        requestErasure(1);

        assertEquals(files, rig.dataFiles());
        assertEquals(snapshots + 1, rig.table.load().orElseThrow().state().snapshots().size());
        assertEquals(new ShipState.LakeCoverage(1, eraseLsn), rig.state.lakeCoverage());
    }

    @Test
    void theRecordOfTheOverwriteIsSignedAndNamesTheReplacedFiles() {
        grantThenErase();
        requestErasure(1);

        String record = current().summary().get(LakeRedaction.RECORD_PROPERTY);
        String payload = new String(io.nodusdb.chain.SignedRecord.open(io.nodusdb.ship.ChainBuilder.keyring(),
                LakeRedaction.RECORD_DOMAIN, record).payload(), StandardCharsets.UTF_8);
        assertTrue(payload.contains("\"erase_lsn\":" + eraseLsn));
        assertTrue(payload.contains("\"removed\":[\""));
        assertFalse(payload.contains("alice"));
        IcebergTable.Head head = rig.table.load().orElseThrow();
        assertEquals(1, head.state().snapshots().size());
    }
}
