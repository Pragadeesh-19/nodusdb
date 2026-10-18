package io.nodusdb.storage;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoldenDirectoryTest {

    @TempDir
    Path directory;

    private static void assertGoldenAnswers(TupleStore store) {
        assertEquals(1, store.schemaVersion());
        assertTrue(store.check("document:plan", "view", "user:alice"), "alice reaches the plan through two nested groups");
        assertTrue(store.check("document:plan", "view", "user:dave"), "dave joined the group in the log tail");
        assertFalse(store.check("document:plan", "view", "user:bob"));
        assertTrue(store.check("document:readme", "view", "user:bob"), "bob is an editor");
        assertTrue(store.check("document:readme", "view", "user:carol"), "carol was granted in the log tail");
        assertFalse(store.check("document:readme", "view", "user:alice"));
        assertFalse(store.check("document:old", "view", "user:erin"), "erin's grant was removed in the log tail");
    }

    @Test
    void aDirectoryWrittenByThisFormatRecoversItsSnapshotAndItsLogTail() throws IOException {
        GoldenDirectories.copyVersionTwo(directory);

        try (GraphKernel graph = StoredGraphs.open(directory)) {
            assertGoldenAnswers(TupleStore.open(graph));
        }
    }

    @Test
    void theAnswersSurviveACleanCloseAndAReopen() throws IOException {
        GoldenDirectories.copyVersionTwo(directory);
        StoredGraphs.open(directory).close();

        try (GraphKernel graph = StoredGraphs.open(directory)) {
            assertGoldenAnswers(TupleStore.open(graph));
        }
    }

    @Test
    void aDirectoryOfThisFormatNeedsNoUpgrade() throws IOException {
        GoldenDirectories.copyVersionTwo(directory);

        assertFalse(GraphUpgrade.upgrade(directory).performed());
    }
}
