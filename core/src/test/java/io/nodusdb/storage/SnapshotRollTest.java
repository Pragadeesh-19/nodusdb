package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotRollTest {

    private static final int NODES = 60_000;
    private static final int BASE_EDGES = 500_000;
    private static final int DELTA_EDGES = 10_000;

    @TempDir
    Path directory;

    @TempDir
    Path image;

    @Test
    void checkpointRollsTheLogAndRecoveryReplaysOnlyTheDelta() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(31L, BASE_EDGES + DELTA_EDGES, NODES);
        long[] base = Arrays.copyOf(pairs, 2 * BASE_EDGES);
        long[] delta = Arrays.copyOfRange(pairs, 2 * BASE_EDGES, 2 * (BASE_EDGES + DELTA_EDGES));

        GraphKernel live = StoredGraphs.open(directory);
        try {
            live.addEdges(base, BASE_EDGES);
            live.checkpoint();

            assertTrue(Files.exists(directory.resolve(GraphFiles.SNAPSHOT)), "snapshot missing after checkpoint");
            assertEquals(1, segmentNames(directory).size(), "old segments were not trimmed");
            assertEquals(live.appliedLsn() + 1, baseLsnOf(segmentNames(directory).get(0)));

            live.addEdges(delta, DELTA_EDGES);
            live.sync();
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        Recovery recovery = StoredGraphs.recover(image);
        try {
            assertEquals(DELTA_EDGES, recovery.recordsReplayed());
            assertEquals(0L, recovery.truncatedBytes());
            assertEquals(BASE_EDGES + DELTA_EDGES, GraphFixtures.edgeCount(recovery.kernel(), NODES));
            GraphFixtures.assertInDegreesMatchForward(recovery.kernel(), NODES);
            assertEveryEdgePresent(recovery.kernel(), pairs);
        } finally {
            recovery.kernel().close();
        }
    }

    private static List<String> segmentNames(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory.resolve(GraphFiles.LOG_DIRECTORY))) {
            return files.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(".nlog")).sorted()
                    .toList();
        }
    }

    private static long baseLsnOf(String segmentName) {
        return Long.parseLong(segmentName.substring(0, segmentName.indexOf('.')));
    }

    private static void assertEveryEdgePresent(GraphKernel kernel, long[] pairs) {
        for (int i = 0; i < pairs.length / 2; i++) {
            assertTrue(kernel.hasEdge(pairs[2 * i], pairs[2 * i + 1]), "edge " + i + " lost");
        }
    }
}
