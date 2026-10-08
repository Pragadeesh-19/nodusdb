package io.nodusdb.projection;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.SignedRecord;
import io.nodusdb.iceberg.TableState;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.FaultyObjectStore.Call;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.SimulatedCrash;
import io.nodusdb.projection.EdgeLogProjector.Cadence;
import io.nodusdb.projection.ProjectionRig.Row;
import io.nodusdb.projection.StreamScript.Chunk;
import io.nodusdb.ship.ChainBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeLogProjectorCrashTest {

    private static final long T0 = ProjectionRig.DAY_ZERO + 3_600_000_000L;
    private static final int PHASES = 3;
    private static final int BACKOFF_STEPS = 40;

    private record Played(int interruptions, int storeCalls, List<Call> calls, List<Row> rows, int files,
                          TableState state) {
    }

    private static List<List<Chunk>> script(ProjectionRig rig) {
        StreamScript s = rig.script;
        List<Chunk> first = List.of(
                s.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)),
                s.transaction(T0 + 1, b -> b.epoch(2, 3, 12)),
                s.transaction(T0 + 2, b -> b.tuple(RecordType.TUPLE_REMOVE, 0, 1, 0, 1)));
        List<Chunk> second = List.of(
                s.transaction(T0 + ProjectionRig.DAY + 5, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)),
                s.transaction(T0 + ProjectionRig.DAY + 6, b -> b.schema(2, RecordFixtures.digest(), new int[0],
                        new int[0], new int[0], new int[0], "version two".getBytes(StandardCharsets.UTF_8))),
                s.transaction(T0 + ProjectionRig.DAY + 7, b -> {
                    StreamScript.symbol(b, 9, "folder:root");
                    b.tuple(RecordType.TUPLE_ADD, 9, 1, 0, 0);
                }));
        List<Chunk> third = List.of(
                s.transaction(T0 + 2 * ProjectionRig.DAY, b -> b.erase(1, RecordFixtures.pseudonym())),
                s.transaction(T0 + 2 * ProjectionRig.DAY + 1, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)));
        return List.of(first, second, third);
    }

    private static EdgeLogProjector.Next settle(ProjectionRig rig, EdgeLogProjector projector) {
        EdgeLogProjector.Next next = rig.run(projector);
        for (int i = 0; i < BACKOFF_STEPS && next.cadence() == Cadence.BACKOFF; i++) {
            next = rig.run(projector);
        }
        return next;
    }

    private static Played play(ProjectionRig rig) throws IOException {
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").relation(1, "viewer");
        List<List<Chunk>> phases = script(rig);
        boolean[] shipped = new boolean[PHASES];
        EdgeLogProjector projector = rig.projector();
        int interruptions = 0;
        int phase = 0;
        while (phase < PHASES) {
            try {
                if (!shipped[phase]) {
                    phases.get(phase).forEach(rig::ship);
                    shipped[phase] = true;
                }
                settle(rig, projector);
                rig.advance(61);
                if (phase == PHASES - 1) {
                    boolean drained = false;
                    for (int i = 0; i < BACKOFF_STEPS && !drained; i++) {
                        drained = projector.drain();
                    }
                    assertTrue(drained, "the last phase must drain");
                } else {
                    EdgeLogProjector.Next next = settle(rig, projector);
                    assertEquals(Cadence.IDLE, next.cadence());
                }
                phase++;
            } catch (SimulatedCrash crash) {
                interruptions++;
                projector = rig.restart();
            }
        }
        return new Played(interruptions, rig.faulty.callCount(), rig.faulty.calls(), rig.rows(), rig.dataFiles(),
                rig.table.load().orElseThrow().state());
    }

    private static Played cleanRun() throws IOException {
        Path directory = Files.createTempDirectory("projector-clean");
        try {
            return play(new ProjectionRig(directory, ProjectionSettings.defaults()));
        } finally {
            delete(directory);
        }
    }

    private static void delete(Path directory) {
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void theScriptedProjectionIsDeterministic() throws IOException {
        Played first = cleanRun();
        Played second = cleanRun();

        assertEquals(0, first.interruptions());
        assertEquals(first.storeCalls(), second.storeCalls());
        assertEquals(first.rows(), second.rows());
        assertTrue(first.storeCalls() >= 20, "only " + first.storeCalls() + " store calls are worth sweeping");
        assertEquals(3, first.state().snapshots().size());
        assertEquals(8, first.rows().size());
    }

    @ParameterizedTest
    @EnumSource(value = Fault.class, names = {"CRASH_BEFORE", "CRASH_AFTER", "FAIL_BEFORE", "FAIL_AFTER", "FATAL"})
    void aFaultAtEveryStoreCallNeverLosesDuplicatesOrCorruptsARow(Fault fault) throws IOException {
        Played clean = cleanRun();

        for (int ordinal = 1; ordinal <= clean.storeCalls(); ordinal++) {
            Path directory = Files.createTempDirectory("projector-fault");
            try {
                ProjectionRig rig = new ProjectionRig(directory, ProjectionSettings.defaults());
                rig.faulty.failCall(ordinal, fault);

                Played played = play(rig);

                String where = fault + " at call " + ordinal + " (" + clean.calls().get(ordinal - 1) + ")";
                boolean crash = fault == Fault.CRASH_BEFORE || fault == Fault.CRASH_AFTER;
                if (crash) {
                    assertEquals(1, played.interruptions(), where);
                }
                assertEquals(clean.rows(), played.rows(), where);
                assertEquals(clean.state().snapshots().size() >= 1, played.state().snapshots().size() >= 1, where);
                assertEquals(Long.parseLong(played.state().current().orElseThrow().summary().get("total-records")),
                        played.rows().size(), where);
                assertEquals(Long.parseLong(played.state().current().orElseThrow().summary().get("total-data-files")),
                        played.files(), where + ": a data file is stored that no snapshot counts");
                assertCommitRecordsLinked(played.state(), where);
            } finally {
                delete(directory);
            }
        }
    }

    private static void assertCommitRecordsLinked(TableState state, String where) {
        ChainHash previous = null;
        for (TableState.SnapshotEntry snapshot : state.snapshots()) {
            SignedRecord.Opened opened = SignedRecord.open(ChainBuilder.keyring(), CommitRecord.DOMAIN,
                    snapshot.summary().get(CommitRecord.PROPERTY));
            JsonObject payload = JsonParser.parseObject(opened.payload());
            assertEquals(previous == null ? "" : previous.hex(), payload.requireString("prev"), where);
            previous = ChainHash.sha256(opened.payload());
        }
    }

    @Test
    void twoFaultsInARowStillConverge() throws IOException {
        Played clean = cleanRun();
        List<Integer> ordinals = new ArrayList<>();
        for (int ordinal = 1; ordinal < clean.storeCalls(); ordinal += 3) {
            ordinals.add(ordinal);
        }

        for (int ordinal : ordinals) {
            Path directory = Files.createTempDirectory("projector-double");
            try {
                ProjectionRig rig = new ProjectionRig(directory, ProjectionSettings.defaults());
                rig.faulty.failCall(ordinal, Fault.CRASH_AFTER);
                rig.faulty.failCall(ordinal + 1, Fault.FAIL_AFTER);

                Played played = play(rig);

                assertEquals(clean.rows(), played.rows(), "faults at calls " + ordinal + " and " + (ordinal + 1));
            } finally {
                delete(directory);
            }
        }
    }
}
