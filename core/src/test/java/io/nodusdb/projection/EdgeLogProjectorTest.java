package io.nodusdb.projection;

import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.projection.EdgeLogProjector.Cadence;
import io.nodusdb.projection.ProjectionRig.Row;
import io.nodusdb.projection.StreamScript.Chunk;
import io.nodusdb.ship.ShipState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeLogProjectorTest {

    private static final long T0 = ProjectionRig.DAY_ZERO + 3_600_000_000L;

    @TempDir
    Path root;

    private ProjectionRig rig;
    private EdgeLogProjector projector;

    @BeforeEach
    void setUp() {
        open(ProjectionSettings.defaults());
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").symbol(2, "group:eng")
                .symbol(3, "document:plan").relation(1, "viewer").relation(2, "member").relation(3, "parent");
    }

    private void open(ProjectionSettings settings) {
        rig = new ProjectionRig(root, settings);
        projector = rig.projector();
    }

    private List<Row> project(Chunk... chunks) {
        for (Chunk chunk : chunks) {
            rig.ship(chunk);
        }
        assertTrue(projector.drain());
        return rig.rows();
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void everyTupleBecomesARowWithSplitNamesAndTheTransactionsCommitFields() {
        Chunk first = rig.script.transaction(T0, b -> {
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1);
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 2, 2);
        });
        Chunk second = rig.script.transaction(T0 + 5_000_000L, b -> b.tuple(RecordType.TUPLE_REMOVE, 0, 1, 0, 1));

        List<Row> rows = project(first, second);

        assertEquals(List.of(
                new Row(11, T0, 13, 1, "add", "document", "readme", "viewer", "user", "alice", "", 1, ""),
                new Row(12, T0, 13, 1, "add", "document", "readme", "viewer", "group", "eng", "member", 1, ""),
                new Row(14, T0 + 5_000_000L, 15, 1, "remove", "document", "readme", "viewer", "user", "alice", "", 1,
                        "")), rows);
    }

    @Test
    void anAutocommitTupleIsItsOwnTransactionWithItsOwnCommitTime() {
        Chunk single = rig.script.autocommit(T0 + 7, RecordType.TUPLE_ADD, 3, 3, 0, 0);

        List<Row> rows = project(single);

        assertEquals(1, rows.size());
        assertEquals(new Row(11, T0 + 7, 11, 1, "add", "document", "plan", "parent", "document", "readme", "", 1, ""),
                rows.get(0));
    }

    @Test
    void symbolsDefinedInTheStreamResolveBeforeTheKernelKnowsThem() {
        Chunk chunk = rig.script.transaction(T0, b -> {
            StreamScript.symbol(b, 9, "folder:root");
            StreamScript.symbol(b, 10, "user:zed");
            b.tuple(RecordType.TUPLE_ADD, 9, 1, 0, 10);
        });

        List<Row> rows = project(chunk);

        assertEquals(1, rows.size());
        assertEquals("folder", rows.get(0).objectType());
        assertEquals("root", rows.get(0).objectId());
        assertEquals("zed", rows.get(0).subjectId());
    }

    @Test
    void symbolsDefinedInAnEarlierObjectStillResolveUntilTheNextTableCommit() {
        Chunk defines = rig.script.transaction(T0, b -> StreamScript.symbol(b, 9, "folder:root"));
        Chunk uses = rig.script.transaction(T0 + 1, b -> b.tuple(RecordType.TUPLE_ADD, 9, 1, 0, 1));
        rig.ship(defines);
        rig.run(projector);

        List<Row> rows = project(uses);

        assertEquals("root", rows.get(0).objectId());
    }

    @Test
    void unknownSymbolsAndRelationsAreMarkedWithTheirIds() {
        Chunk chunk = rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 77, 55, 66, 88));

        List<Row> rows = project(chunk);

        assertEquals(new Row(11, T0, 12, 1, "add", "", "#77", "#55", "", "#88", "#66", 1, ""), rows.get(0));
    }

    @Test
    void aNameWithoutATypeSeparatorHasAnEmptyType() {
        rig.names.symbol(4, "orphan");
        Chunk chunk = rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 4, 1, 0, 1));

        List<Row> rows = project(chunk);

        assertEquals("", rows.get(0).objectType());
        assertEquals("orphan", rows.get(0).objectId());
    }

    @Test
    void onlyTheFirstSeparatorSplitsATypeFromItsId() {
        rig.names.symbol(5, "document:a:b:c");
        Chunk chunk = rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 5, 1, 0, 1));

        List<Row> rows = project(chunk);

        assertEquals("document", rows.get(0).objectType());
        assertEquals("a:b:c", rows.get(0).objectId());
    }

    @Test
    void aSchemaRecordIsAnEventWithTheDocumentAndChangesTheVersionOfLaterRows() {
        Chunk schema = rig.script.transaction(T0, b -> {
            StreamScript.symbol(b, 20, "viewer");
            StreamScript.symbol(b, 21, "document");
            b.schema(2, RecordFixtures.digest(), new int[]{5}, new int[]{21}, new int[]{20}, new int[]{1},
                    utf8("type document { relation viewer: user }"));
        });
        Chunk tuple = rig.script.transaction(T0 + 10, b -> b.tuple(RecordType.TUPLE_ADD, 0, 5, 0, 1));

        List<Row> rows = project(schema, tuple);

        assertEquals(2, rows.size());
        assertEquals(new Row(13, T0, 14, 1, "schema", "", "", "", "", "", "", 2,
                "type document { relation viewer: user }"), rows.get(0));
        assertEquals("viewer", rows.get(1).relation());
        assertEquals(2, rows.get(1).schemaVersion());
    }

    @Test
    void rowsBeforeASchemaRecordKeepTheVersionTheyWereWrittenUnder() {
        Chunk before = rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));
        Chunk schema = rig.script.transaction(T0 + 1, b -> b.schema(2, RecordFixtures.digest(), new int[0],
                new int[0], new int[0], new int[0], utf8("v2")));
        Chunk after = rig.script.transaction(T0 + 2, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));

        List<Row> rows = project(before, schema, after);

        assertEquals(List.of(1L, 2L, 2L), rows.stream().map(Row::schemaVersion).toList());
    }

    @Test
    void anEpochRecordIsAnEventAndChangesTheTenureOfLaterRows() {
        Chunk epoch = rig.script.transaction(T0, b -> b.epoch(7, 3, 20));
        Chunk tuple = rig.script.transaction(T0 + 1, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));
        Chunk earlier = rig.script.transaction(T0 + 2, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));

        List<Row> rows = project(epoch, tuple, earlier);

        assertEquals("epoch", rows.get(0).event());
        assertEquals(7, rows.get(0).epoch());
        assertEquals(List.of(7L, 7L, 7L), rows.stream().map(Row::epoch).toList());
    }

    @Test
    void theTenureEpochStartsFromWhatTheKernelSaysAboutTheFirstLsn() {
        rig.names.epoch = 4;
        projector = rig.projector();
        Chunk tuple = rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));

        List<Row> rows = project(tuple);

        assertEquals(4, rows.get(0).epoch());
    }

    @Test
    void anEraseRecordIsAnEventWithThePseudonymAndLaterRowsShowItInsteadOfTheName() {
        byte[] pseudonym = RecordFixtures.pseudonym();
        Chunk erase = rig.script.transaction(T0, b -> b.erase(1, pseudonym));
        Chunk later = rig.script.transaction(T0 + 1, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));

        List<Row> rows = project(erase, later);

        String hex = HexFormat.of().formatHex(pseudonym);
        assertEquals("erase", rows.get(0).event());
        assertEquals(hex, rows.get(0).detail());
        assertEquals("", rows.get(0).objectId());
        assertEquals("", rows.get(1).subjectType());
        assertEquals(hex, rows.get(1).subjectId());
    }

    @Test
    void symbolConfigAndCommitRecordsProduceNoRows() {
        Chunk chunk = rig.script.transaction(T0, b -> {
            b.graphConfig(2);
            StreamScript.symbol(b, 9, "folder:root");
        });

        List<Row> rows = project(chunk);

        assertEquals(List.of(), rows);
        assertEquals(0, rig.dataFiles());
        assertFalse(rig.table.load().isPresent());
    }

    @Test
    void rowsAreGroupedIntoFilesByTheDayOfTheirCommit() {
        Chunk dayZero = rig.script.transaction(ProjectionRig.DAY_ZERO + 10, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));
        Chunk dayOne = rig.script.transaction(ProjectionRig.DAY_ZERO + ProjectionRig.DAY + 5,
                b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));
        Chunk backToDayZero = rig.script.transaction(ProjectionRig.DAY_ZERO + 20,
                b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));

        List<Row> rows = project(dayZero, dayOne, backToDayZero);

        assertEquals(3, rows.size());
        assertEquals(2, rig.dataFiles());
    }

    @Test
    void nothingIsCommittedBeforeTheCommitIntervalAndTheNextStepAfterItCommits() {
        rig.ship(rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)));

        EdgeLogProjector.Next waiting = rig.run(projector);
        assertEquals(Cadence.IDLE, waiting.cadence());
        assertEquals(0, rig.dataFiles());
        assertEquals(0, rig.state.snapshot().projection().commits());

        rig.advance(61);
        rig.run(projector);

        assertEquals(1, rig.state.snapshot().projection().commits());
        assertEquals(1, rig.dataFiles());
        assertEquals(1, rig.table.load().orElseThrow().version());
    }

    @Test
    void anIdleProjectorWithNothingPendingCommitsNothingHoweverLongItWaits() {
        rig.advance(10_000);

        EdgeLogProjector.Next next = rig.run(projector);

        assertEquals(Cadence.IDLE, next.cadence());
        assertEquals(ShipState.ProjectionPhase.ACTIVE, rig.state.snapshot().projection().phase());
        assertFalse(rig.table.load().isPresent());
    }

    @Test
    void aBufferThatReachesTheFlushSizeIsWrittenAsADataFileBeforeTheCommit() {
        open(ProjectionSettings.defaults().withFlushRows(50));
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").relation(1, "viewer");
        Chunk first = rig.script.transaction(T0, b -> {
            for (int i = 0; i < 60; i++) {
                b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1);
            }
        });
        Chunk second = rig.script.transaction(T0 + 1, b -> {
            for (int i = 0; i < 60; i++) {
                b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1);
            }
        });
        Chunk small = rig.script.transaction(T0 + 2, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1));
        rig.ship(first);
        rig.ship(second);
        rig.ship(small);

        rig.run(projector);

        assertEquals(2, rig.dataFiles());
        assertEquals(0, rig.state.snapshot().projection().commits());
        assertTrue(projector.drain());
        assertEquals(3, rig.dataFiles());
        assertEquals(121, rig.rows().size());
        assertEquals(1, rig.table.load().orElseThrow().state().snapshots().size());
    }

    @Test
    void oneObjectsRowsAreNeverSplitAcrossFilesBelowTheFlushSize() {
        open(ProjectionSettings.defaults().withFlushRows(50));
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").relation(1, "viewer");
        Chunk big = rig.script.transaction(T0, b -> {
            for (int i = 0; i < 120; i++) {
                b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1);
            }
        });

        project(big);

        assertEquals(1, rig.dataFiles());
        assertEquals(120, rig.rows().size());
    }

    @Test
    void theSnapshotCarriesTheProjectedPosition() {
        project(rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)),
                rig.script.transaction(T0 + 1, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)));

        var summary = rig.table.load().orElseThrow().state().current().orElseThrow().summary();

        assertEquals("14", summary.get(EdgeLogProjector.PROJECTED_LSN));
        assertEquals("1", summary.get(EdgeLogProjector.PROJECTED_EPOCH));
        assertEquals("1", summary.get(EdgeLogProjector.PROJECTED_SCHEMA_VERSION));
        assertEquals("3", summary.get(EdgeLogProjector.PROJECTED_CHAIN_SEQ));
        assertTrue(summary.containsKey(CommitRecord.PROPERTY));
        ShipState.ProjectionStatus status = rig.state.snapshot().projection();
        assertEquals(14, status.projectedLsn());
        assertEquals(3, status.projectedSeq());
        assertEquals(2, status.rows());
        assertEquals(rig.table.load().orElseThrow().state().currentSnapshotId(), status.snapshotId());
    }
}
