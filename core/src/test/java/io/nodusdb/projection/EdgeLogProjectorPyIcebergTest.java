package io.nodusdb.projection;

import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonNumber;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonString;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.projection.ProjectionRig.Row;
import io.nodusdb.verify.PythonVerifier;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EdgeLogProjectorPyIcebergTest {

    private static final long T0 = ProjectionRig.DAY_ZERO + 3_600_000_000L;

    @TempDir
    Path root;

    private static long number(JsonArray array, int index) {
        return ((JsonNumber) array.get(index)).asLong();
    }

    private static String text(JsonArray array, int index) {
        return ((JsonString) array.get(index)).value();
    }

    private static List<Row> rowsOf(JsonObject columns) {
        JsonArray lsn = columns.requireArray("lsn");
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < lsn.size(); i++) {
            rows.add(new Row(number(lsn, i), number(columns.requireArray("commit_ts"), i),
                    number(columns.requireArray("txn_lsn"), i), number(columns.requireArray("epoch"), i),
                    text(columns.requireArray("event"), i), text(columns.requireArray("object_type"), i),
                    text(columns.requireArray("object_id"), i), text(columns.requireArray("relation"), i),
                    text(columns.requireArray("subject_type"), i), text(columns.requireArray("subject_id"), i),
                    text(columns.requireArray("subject_relation"), i),
                    number(columns.requireArray("schema_version"), i), text(columns.requireArray("detail"), i)));
        }
        rows.sort(Comparator.comparingLong(Row::lsn));
        return rows;
    }

    @Test
    void aRealIcebergReaderSeesExactlyTheRowsTheProjectorWrote() {
        ProjectionRig rig = new ProjectionRig(root, ProjectionSettings.defaults().withFlushRows(3));
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").symbol(2, "group:eng")
                .relation(1, "viewer").relation(2, "member");
        EdgeLogProjector projector = rig.projector();
        rig.ship(rig.script.transaction(T0, b -> {
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1);
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 2, 2);
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 2);
            b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 2);
        }));
        rig.ship(rig.script.transaction(T0 + 1, b -> b.epoch(5, 3, 20)));
        rig.ship(rig.script.transaction(T0 + 2, b -> b.schema(2, RecordFixtures.digest(), new int[0], new int[0],
                new int[0], new int[0], "type user".getBytes(StandardCharsets.UTF_8))));
        rig.ship(rig.script.transaction(ProjectionRig.DAY_ZERO + 2 * ProjectionRig.DAY + 9,
                b -> b.tuple(RecordType.TUPLE_REMOVE, 0, 1, 0, 1)));
        rig.ship(rig.script.transaction(ProjectionRig.DAY_ZERO + 2 * ProjectionRig.DAY + 10,
                b -> b.erase(1, RecordFixtures.pseudonym())));
        rig.ship(rig.script.autocommit(ProjectionRig.DAY_ZERO + 2 * ProjectionRig.DAY + 11, RecordType.TUPLE_ADD, 0,
                1, 0, 1));
        projector.drain();
        Optional<JsonObject> report = PythonVerifier.run(root, "iceberg_scan.py",
                rig.table.uriOf("iceberg/metadata/v1.metadata.json"));
        Assumptions.assumeTrue(report.isPresent(), "pyiceberg is not available");

        List<Row> seen = rowsOf(report.get().requireObject("columns"));

        assertEquals(rig.rows(), seen);
        assertEquals(9, seen.size());
        assertEquals(2, report.get().requireLong("format_version"));
        assertEquals(1, report.get().requireArray("snapshots").size());
        assertEquals(rig.dataFiles(), report.get().requireArray("files").size());
    }

    @Test
    void aLaterCommitAddsToTheSameTableAndEveryRowStaysVisible() {
        ProjectionRig rig = new ProjectionRig(root, ProjectionSettings.defaults());
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").relation(1, "viewer");
        EdgeLogProjector projector = rig.projector();
        rig.ship(rig.script.transaction(T0, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)));
        projector.drain();
        projector = rig.restart();
        rig.ship(rig.script.transaction(T0 + 1, b -> b.tuple(RecordType.TUPLE_REMOVE, 0, 1, 0, 1)));
        projector.drain();
        Optional<JsonObject> report = PythonVerifier.run(root, "iceberg_scan.py",
                rig.table.uriOf("iceberg/metadata/v2.metadata.json"), "event = 'remove'", "lsn > 100");
        Assumptions.assumeTrue(report.isPresent(), "pyiceberg is not available");

        List<Row> seen = rowsOf(report.get().requireObject("columns"));

        assertEquals(rig.rows(), seen);
        assertEquals(2, report.get().requireArray("snapshots").size());
        JsonObject filters = report.get().requireObject("filters");
        assertEquals(1, filters.requireObject("event = 'remove'").requireLong("rows"));
        assertEquals(0, filters.requireObject("lsn > 100").requireLong("files"));
    }
}
