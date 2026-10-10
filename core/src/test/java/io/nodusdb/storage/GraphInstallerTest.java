package io.nodusdb.storage;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.CapturingLog;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphInstallerTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final long OPEN_EPOCH_RECORDS = 2;

    private static final class SimulatedCrash extends Error {

        private static final long serialVersionUID = 1L;
    }

    @TempDir
    Path root;

    private final byte[] salt = salt();
    private GraphKernel primary;
    private GraphKernel replica;

    private static byte[] salt() {
        byte[] bytes = new byte[32];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i * 5 + 3);
        }
        return bytes;
    }

    @BeforeEach
    void build() {
        CapturingLog log = new CapturingLog();
        primary = new GraphKernel();
        primary.attachLog(log, CapturingLog.noStorage());
        TupleStore tuples = TupleStore.open(primary);
        tuples.applySchema(SchemaFixtures.DOCUMENTS);
        for (int i = 0; i < 40; i++) {
            tuples.add("document:d" + i, "viewer", "user:u" + i % 5);
        }
        replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
        replica.recordEpoch(1, 1, 0);
        CapturingLog.Drained drained = log.drain();
        replica.applyReplicated(drained.records(), drained.first(), drained.last());
    }

    private GraphKernel open(Path directory) throws IOException {
        return DurableGraph.open(directory, SYNC).kernel();
    }

    private static List<String> namesIn(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    @Test
    void anInstalledDirectoryOpensToTheGraphItWasBuiltFrom() throws IOException {
        Path target = root.resolve("restored");

        GraphInstaller.install(replica, salt, target);

        GraphKernel restored = open(target);
        try {
            assertEquals(GraphDigest.of(primary), GraphDigest.of(restored));
            assertEquals(replica.appliedLsn() + OPEN_EPOCH_RECORDS, restored.appliedLsn());
            assertTrue(TupleStore.open(restored).check("document:d3", "view", "user:u3"));
            assertFalse(TupleStore.open(restored).check("document:d3", "view", "user:u4"));
        } finally {
            restored.close();
        }
    }

    @Test
    void anInstalledDirectoryAcceptsWritesAndSurvivesAReopen() throws IOException {
        Path target = root.resolve("restored");
        GraphInstaller.install(replica, salt, target);

        GraphKernel restored = open(target);
        TupleStore tuples = TupleStore.open(restored);
        tuples.add("document:new", "viewer", "user:alice");
        restored.close();

        GraphKernel reopened = open(target);
        try {
            assertTrue(TupleStore.open(reopened).check("document:new", "view", "user:alice"));
            assertTrue(TupleStore.open(reopened).check("document:d0", "view", "user:u0"));
        } finally {
            reopened.close();
        }
    }

    @Test
    void theSaltIsKeptSoTheRestoredGraphKeepsItsIdentity() throws IOException {
        Path target = root.resolve("restored");

        GraphInstaller.install(replica, salt, target);

        SnapshotMeta meta = SnapshotReader.load(target.resolve(GraphFiles.SNAPSHOT), new GraphKernel());
        assertArrayEquals(salt, meta.salt());
        assertEquals(replica.appliedLsn(), meta.lsn());
        assertEquals(replica.epoch(), meta.epoch());
        assertEquals(replica.lastCommitMicros(), meta.lastCommitMicros());
    }

    @Test
    void theEpochHistoryOfTheReplicaIsCarriedIntoTheInstalledGraph() throws IOException {
        Path target = root.resolve("restored");
        GraphInstaller.install(replica, salt, target);

        GraphKernel restored = open(target);
        try {
            assertEquals(1, restored.epochHistory().tenureAt(0).epoch());
            assertEquals(2, restored.epoch());
        } finally {
            restored.close();
        }
    }

    @Test
    void anInstalledDirectoryHoldsTheFilesOfACurrentFormatGraphAndNothingElse() throws IOException {
        Path target = root.resolve("restored");

        GraphInstaller.install(replica, salt, target);

        assertEquals(List.of("FORMAT", "log", "nodus.wal", "snapshot.bin"), namesIn(target));
        assertTrue(Files.isDirectory(target.resolve("log")));
        assertFalse(Files.exists(root.resolve("restored" + GraphInstaller.STAGING_SUFFIX)));
    }

    @Test
    void anExistingEmptyDirectoryIsAcceptedAsTheTarget() throws IOException {
        Path target = Files.createDirectory(root.resolve("restored"));

        GraphInstaller.install(replica, salt, target);

        GraphKernel restored = open(target);
        try {
            assertEquals(GraphDigest.of(primary), GraphDigest.of(restored));
        } finally {
            restored.close();
        }
    }

    @Test
    void aMissingParentDirectoryIsCreated() throws IOException {
        Path target = root.resolve("a").resolve("b").resolve("restored");

        GraphInstaller.install(replica, salt, target);

        assertTrue(Files.isRegularFile(target.resolve(GraphFiles.SNAPSHOT)));
    }

    @Test
    void aNonEmptyTargetIsRefusedAndLeftUntouched() throws IOException {
        Path target = Files.createDirectory(root.resolve("restored"));
        Files.writeString(target.resolve("precious.txt"), "keep");

        assertThrows(UnsupportedFeatureException.class, () -> GraphInstaller.install(replica, salt, target));

        assertEquals(List.of("precious.txt"), namesIn(target));
        assertFalse(Files.exists(root.resolve("restored" + GraphInstaller.STAGING_SUFFIX)));
    }

    @Test
    void aTargetThatIsAFileIsRefused() throws IOException {
        Path target = root.resolve("restored");
        Files.writeString(target, "a file");

        assertThrows(UnsupportedFeatureException.class, () -> GraphInstaller.install(replica, salt, target));

        assertEquals("a file", Files.readString(target));
    }

    @Test
    void aSaltOfTheWrongLengthIsRefusedBeforeAnythingIsWritten() {
        Path target = root.resolve("restored");

        assertThrows(IllegalArgumentException.class,
                () -> GraphInstaller.install(replica, new byte[5], target));

        assertFalse(Files.exists(target));
        assertFalse(Files.exists(root.resolve("restored" + GraphInstaller.STAGING_SUFFIX)));
    }

    @Test
    void aLeftoverStagingDirectoryFromACrashIsReplaced() throws IOException {
        Path staging = Files.createDirectory(root.resolve("restored" + GraphInstaller.STAGING_SUFFIX));
        Files.writeString(staging.resolve("junk"), "from a crashed attempt");
        Path target = root.resolve("restored");

        GraphInstaller.install(replica, salt, target);

        assertEquals(List.of("FORMAT", "log", "nodus.wal", "snapshot.bin"), namesIn(target));
        assertFalse(Files.exists(staging));
    }

    @Test
    void anEmptyReplicaInstallsAsAnEmptyGraph() throws IOException {
        Path target = root.resolve("restored");

        GraphInstaller.install(GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT), salt, target);

        GraphKernel restored = open(target);
        try {
            assertFalse(restored.hasEdges());
            assertEquals(OPEN_EPOCH_RECORDS, restored.appliedLsn());
            assertEquals(1, restored.epoch());
        } finally {
            restored.close();
        }
    }

    @Test
    void aFailureWhileStagingLeavesNothingAtTheTargetAndNoStagingDirectory() {
        Path target = root.resolve("restored");

        assertThrows(IllegalStateException.class, () -> GraphInstaller.install(replica, salt, target, step -> {
            if (step == InstallStep.SNAPSHOT_WRITTEN) {
                throw new IllegalStateException("disk full");
            }
        }));

        assertFalse(Files.exists(target));
        assertFalse(Files.exists(root.resolve("restored" + GraphInstaller.STAGING_SUFFIX)));
    }

    @Test
    void aCrashAtEveryStepLeavesNoDirectoryThatLooksLikeAGraphUntilTheRename() throws IOException {
        for (boolean existingEmptyTarget : new boolean[] {false, true}) {
            for (InstallStep crashAt : InstallStep.values()) {
                if (crashAt == InstallStep.TARGET_CLEARED && !existingEmptyTarget) {
                    continue;
                }
                Path target = root.resolve("t-" + existingEmptyTarget + "-" + crashAt);
                if (existingEmptyTarget) {
                    Files.createDirectory(target);
                }
                List<InstallStep> seen = new ArrayList<>();

                assertThrows(SimulatedCrash.class, () -> GraphInstaller.install(replica, salt, target, step -> {
                    seen.add(step);
                    if (step == crashAt) {
                        throw new SimulatedCrash();
                    }
                }), crashAt.toString());

                boolean renamed = crashAt == InstallStep.RENAMED;
                if (renamed) {
                    assertTrue(Files.isRegularFile(target.resolve(GraphFiles.FORMAT)), crashAt.toString());
                } else if (existingEmptyTarget && !seen.contains(InstallStep.TARGET_CLEARED)) {
                    assertEquals(List.of(), namesIn(target), crashAt.toString());
                } else {
                    assertFalse(Files.exists(target), crashAt.toString());
                }
                Path fresh = existingEmptyTarget && !renamed
                        ? target : root.resolve("again-" + existingEmptyTarget + "-" + crashAt);
                if (renamed) {
                    assertEquals(GraphDigest.of(primary), digestOfOpened(target), crashAt.toString());
                } else {
                    GraphInstaller.install(replica, salt, fresh);
                    assertEquals(GraphDigest.of(primary), digestOfOpened(fresh), crashAt.toString());
                }
            }
        }
    }

    private GraphDigest digestOfOpened(Path directory) throws IOException {
        GraphKernel opened = open(directory);
        try {
            return GraphDigest.of(opened);
        } finally {
            opened.close();
        }
    }

    @Test
    void theObserverSeesEveryStepInOrderOnASuccessfulInstall() throws IOException {
        List<InstallStep> seen = new ArrayList<>();

        GraphInstaller.install(replica, salt, root.resolve("restored"), seen::add);

        assertEquals(Arrays.asList(InstallStep.STAGING_CREATED, InstallStep.SNAPSHOT_WRITTEN,
                InstallStep.TRIPWIRE_WRITTEN, InstallStep.FORMAT_WRITTEN, InstallStep.RENAMED), seen);
    }
}
