package io.nodusdb.storage;

import io.nodusdb.error.UpgradeRequiredException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.log.LogConfig;
import io.nodusdb.storage.DirectoryFormat.Layout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphUpgradeTest {

    private static final class SimulatedCrash extends RuntimeException {

        private static final long serialVersionUID = 1L;
    }

    @TempDir
    Path directory;

    @Test
    void anIntegerGraphKeepsEveryEdgeFromItsSnapshotAndItsLog() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);

        GraphUpgrade.Report report = GraphUpgrade.upgrade(directory);

        assertTrue(report.performed());
        assertEquals(1_040, report.edges());
        try (GraphKernel graph = StoredGraphs.open(directory)) {
            assertEquals(KeyKind.INTEGER, graph.keyKind());
            assertTrue(graph.hasEdge(1, 2));
            assertTrue(graph.hasEdge(999, 1_000));
            assertTrue(graph.hasEdge(2_000, 2_001));
            assertTrue(graph.hasEdge(2_001, 2_002));
            assertFalse(graph.hasEdge(0, 1), "a removal in the old log was lost");
            assertFalse(graph.hasEdge(5_000, 5_001), "a removal in the old log was lost");
            assertTrue(graph.hasEdge(5_000, 5_002));
            assertEquals(39, graph.getDegree(5_000));
            assertEquals(1, graph.getInDegree(5_002));
        }
    }

    @Test
    void aStringGraphKeepsItsNamesAndIds() throws IOException {
        GoldenDirectories.copyVersionOne("string", directory);

        GraphUpgrade.upgrade(directory);

        try (GraphKernel graph = StoredGraphs.open(directory)) {
            assertEquals(KeyKind.STRING, graph.keyKind());
            assertEquals(8, graph.symbols().size());
            assertEquals(0, lookup(graph, "user:alice"));
            assertEquals(3, lookup(graph, "group:staff"));
            assertEquals(7, lookup(graph, "doc:plan"));
            assertArrayEquals("user:carol".getBytes(StandardCharsets.UTF_8), graph.symbols().resolve(4));
            assertTrue(graph.hasEdge(0, 1));
            assertTrue(graph.hasEdge(6, 1));
            assertTrue(graph.hasEdge(7, 3));
            assertEquals(4, graph.getInDegree(1));
            assertEquals(3, graph.getInDegree(3));
        }
    }

    @Test
    void aVersionOneSnapshotWithoutALogIsUpgraded() throws IOException {
        GoldenDirectories.copyVersionOne("legacy-snapshot", directory);

        GraphUpgrade.Report report = GraphUpgrade.upgrade(directory);

        assertEquals(7, report.edges());
        try (GraphKernel graph = StoredGraphs.open(directory)) {
            assertTrue(graph.hasEdge(0, 3));
            assertTrue(graph.hasEdge(3, 4));
            assertTrue(graph.hasEdge(4, 5));
            assertEquals(3, graph.getDegree(0));
        }
    }

    @Test
    void theOriginalFilesAreKeptByteForByte() throws IOException {
        GoldenDirectories.copyVersionOne("string", directory);
        byte[] snapshot = Files.readAllBytes(directory.resolve("snapshot.bin"));
        byte[] log = Files.readAllBytes(directory.resolve("nodus.wal"));
        byte[] symbols = Files.readAllBytes(directory.resolve("symbols.nodus"));

        GraphUpgrade.upgrade(directory);

        Path backup = directory.resolve(GraphFiles.BACKUP_DIRECTORY);
        assertArrayEquals(snapshot, Files.readAllBytes(backup.resolve("snapshot.bin")));
        assertArrayEquals(log, Files.readAllBytes(backup.resolve("nodus.wal")));
        assertArrayEquals(symbols, Files.readAllBytes(backup.resolve("symbols.nodus")));
        assertFalse(Files.exists(directory.resolve(GraphFiles.LEGACY_SYMBOLS)));
    }

    @Test
    void anEarlierVersionRefusesTheUpgradedDirectory() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);

        GraphUpgrade.upgrade(directory);

        assertEquals(DirectoryFormat.CURRENT_VERSION,
                DirectoryFormat.readTripwireVersion(directory.resolve(GraphFiles.TRIPWIRE)));
        ByteBuffer snapshot = ByteBuffer.wrap(Files.readAllBytes(directory.resolve(GraphFiles.SNAPSHOT)));
        assertEquals(3, snapshot.getShort(4), "an earlier version must see an unknown snapshot version");
    }

    @Test
    void upgradingAnUpgradedDirectoryChangesNothing() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);
        GraphUpgrade.upgrade(directory);
        byte[] snapshot = Files.readAllBytes(directory.resolve(GraphFiles.SNAPSHOT));

        GraphUpgrade.Report again = GraphUpgrade.upgrade(directory);

        assertFalse(again.performed());
        assertArrayEquals(snapshot, Files.readAllBytes(directory.resolve(GraphFiles.SNAPSHOT)));
    }

    @Test
    void anEmptyDirectoryHasNothingToUpgrade() throws IOException {
        assertFalse(GraphUpgrade.upgrade(directory).performed());
        assertEquals(Layout.NEW, DirectoryFormat.detect(directory));
    }

    @Test
    void theUpgradedGraphAcceptsWritesAndSurvivesRestarts() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);
        GraphUpgrade.upgrade(directory);

        try (GraphKernel graph = StoredGraphs.open(directory, StoredGraphs.SYNC)) {
            assertTrue(graph.addEdge(7_000, 7_001));
            assertTrue(graph.removeEdge(1, 2));
        }

        try (GraphKernel graph = StoredGraphs.open(directory)) {
            assertTrue(graph.hasEdge(7_000, 7_001));
            assertFalse(graph.hasEdge(1, 2));
            assertTrue(graph.hasEdge(2, 3));
            assertEquals(4, graph.epochHistory().size());
        }
    }

    @Test
    void cleanupRemovesOnlyTheBackup() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);
        GraphUpgrade.upgrade(directory);

        GraphUpgrade.cleanup(directory);

        assertFalse(Files.exists(directory.resolve(GraphFiles.BACKUP_DIRECTORY)));
        try (GraphKernel graph = StoredGraphs.open(directory)) {
            assertTrue(graph.hasEdge(2_000, 2_001));
        }
    }

    @Test
    void cleanupRefusesADirectoryThatWasNeverUpgraded() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);

        assertThrows(IllegalStateException.class, () -> GraphUpgrade.cleanup(directory));
    }

    @Test
    void aTornTailInTheOldLogDoesNotBlockTheUpgrade() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);
        Files.write(directory.resolve("nodus.wal"), new byte[] {9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
                StandardOpenOption.APPEND);

        GraphUpgrade.Report report = GraphUpgrade.upgrade(directory);

        assertEquals(1_040, report.edges());
    }

    @Test
    void aStagingDirectoryLeftByAnEarlierAttemptIsReplaced() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);
        Path staging = Files.createDirectories(directory.resolve(GraphFiles.STAGING_DIRECTORY).resolve("log"));
        Files.writeString(staging.resolve("stale"), "left behind");

        GraphUpgrade.Report report = GraphUpgrade.upgrade(directory);

        assertEquals(1_040, report.edges());
        assertFalse(Files.exists(directory.resolve(GraphFiles.STAGING_DIRECTORY)));
    }

    @Test
    void openingBeforeTheUpgradeIsRefusedAtEveryPoint() throws IOException {
        GoldenDirectories.copyVersionOne("integer", directory);

        assertThrows(UpgradeRequiredException.class, () -> StoredGraphs.open(directory));
        assertEquals(Layout.LEGACY, DirectoryFormat.detect(directory));
    }

    @ParameterizedTest
    @ValueSource(strings = {"integer", "string", "legacy-snapshot"})
    void aCrashAfterAnyStepIsFinishedByRunningTheUpgradeAgain(String fixture) throws IOException {
        int steps = countSteps(fixture);
        GraphDigest expected = referenceDigest(fixture);

        for (int crashAfter = 1; crashAfter <= steps; crashAfter++) {
            Path attempt = Files.createDirectories(directory.resolve(fixture + "-" + crashAfter));
            GoldenDirectories.copyVersionOne(fixture, attempt);
            int seen = crashAfter;
            int[] counter = {0};

            assertThrows(SimulatedCrash.class, () -> GraphUpgrade.upgrade(attempt, step -> {
                if (++counter[0] == seen) {
                    throw new SimulatedCrash();
                }
            }), "crash after step " + crashAfter);
            assertRefusesToOpenUntilUpgraded(attempt, crashAfter);

            GraphUpgrade.Report report = GraphUpgrade.upgrade(attempt);

            assertTrue(report.performed() || DirectoryFormat.detect(attempt) == Layout.CURRENT,
                    "resume after step " + crashAfter);
            try (GraphKernel graph = StoredGraphs.open(attempt)) {
                assertEquals(expected, GraphDigest.of(graph), "after a crash at step " + crashAfter);
            }
            assertOriginalsPreserved(fixture, attempt, crashAfter);
        }
    }

    private void assertRefusesToOpenUntilUpgraded(Path attempt, int crashAfter) throws IOException {
        Layout layout = DirectoryFormat.detect(attempt);
        if (layout == Layout.CURRENT) {
            try (GraphKernel graph = StoredGraphs.open(attempt)) {
                assertTrue(graph.isDurable());
            }
            return;
        }
        assertThrows(UpgradeRequiredException.class, () -> StoredGraphs.open(attempt),
                "a half-upgraded directory opened after step " + crashAfter);
    }

    private void assertOriginalsPreserved(String fixture, Path attempt, int crashAfter) throws IOException {
        Path reference = Files.createDirectories(directory.resolve("reference-" + fixture + "-" + crashAfter));
        GoldenDirectories.copyVersionOne(fixture, reference);
        try (Stream<Path> files = Files.list(reference)) {
            for (Path original : files.toList()) {
                Path saved = attempt.resolve(GraphFiles.BACKUP_DIRECTORY).resolve(original.getFileName());
                assertArrayEquals(Files.readAllBytes(original), Files.readAllBytes(saved),
                        original.getFileName() + " changed after a crash at step " + crashAfter);
            }
        }
    }

    private int countSteps(String fixture) throws IOException {
        Path counting = Files.createDirectories(directory.resolve("count-" + fixture));
        GoldenDirectories.copyVersionOne(fixture, counting);
        List<UpgradeStep> seen = new ArrayList<>();
        GraphUpgrade.upgrade(counting, seen::add);
        return seen.size();
    }

    private GraphDigest referenceDigest(String fixture) throws IOException {
        Path reference = Files.createDirectories(directory.resolve("digest-" + fixture));
        GoldenDirectories.copyVersionOne(fixture, reference);
        GraphUpgrade.upgrade(reference);
        try (GraphKernel graph = StoredGraphs.open(reference)) {
            return GraphDigest.of(graph);
        }
    }

    private static long lookup(GraphKernel graph, String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return graph.symbols().lookup(bytes, 0, bytes.length);
    }
}
