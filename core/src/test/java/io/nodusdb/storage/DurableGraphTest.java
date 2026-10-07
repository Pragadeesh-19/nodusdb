package io.nodusdb.storage;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.error.UpgradeRequiredException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableGraphTest {

    private static final int NODES = 1_000;

    @TempDir
    Path directory;

    @Test
    void aNewGraphIsCreatedInTheCurrentFormat() throws IOException {
        StoredGraphs.open(directory).close();

        assertEquals("nodus-format 2\n", Files.readString(directory.resolve(GraphFiles.FORMAT),
                StandardCharsets.US_ASCII));
        assertEquals(DirectoryFormat.CURRENT_VERSION,
                DirectoryFormat.readTripwireVersion(directory.resolve(GraphFiles.TRIPWIRE)));
        assertTrue(Files.isRegularFile(directory.resolve(GraphFiles.SNAPSHOT)));
        assertTrue(Files.isDirectory(directory.resolve(GraphFiles.LOG_DIRECTORY)));
    }

    @Test
    void duplicateAddAndMissingRemoveWriteNothingToTheLog() throws IOException {
        GraphKernel graph = StoredGraphs.open(directory);
        try {
            assertTrue(graph.addEdge(1, 2));
            graph.sync();
            Token afterAdd = graph.token();

            for (int i = 0; i < 1_000; i++) {
                assertFalse(graph.addEdge(1, 2));
                assertFalse(graph.removeEdge(7, 8));
            }
            graph.sync();

            assertEquals(afterAdd, graph.token());
        } finally {
            graph.close();
        }
    }

    @Test
    void batchMutationsReplayAfterCleanClose() throws IOException {
        long[] adds = {1, 2, 2, 3, 3, 4, 4, 5, 1, 5};
        long[] removes = {2, 3, 9, 9};
        GraphKernel graph = StoredGraphs.open(directory);
        assertEquals(5, graph.addEdges(adds, 5));
        assertEquals(1, graph.removeEdges(removes, 2));
        graph.close();

        GraphKernel reopened = StoredGraphs.open(directory);
        try {
            assertEquals(4, GraphFixtures.edgeCount(reopened, NODES));
            assertFalse(reopened.hasEdge(2, 3));
            assertTrue(reopened.hasEdge(4, 5));
            assertTrue(reopened.hasEdge(1, 5));
        } finally {
            reopened.close();
        }
    }

    @Test
    void cleanCloseLeavesASnapshotAndNothingToReplay() throws IOException {
        GraphKernel graph = StoredGraphs.open(directory);
        graph.addEdge(10, 20);
        graph.close();

        Recovery recovery = StoredGraphs.recover(directory);
        try {
            assertEquals(0, recovery.recordsReplayed());
            assertEquals(0, recovery.truncatedBytes());
            assertTrue(recovery.kernel().hasEdge(10, 20));
        } finally {
            recovery.kernel().close();
        }
    }

    @Test
    void everyOpenStartsANewTenureAndTokensStayMonotone() throws IOException {
        GraphKernel first = StoredGraphs.open(directory);
        first.addEdge(1, 2);
        Token firstToken = first.token();
        first.close();

        GraphKernel second = StoredGraphs.open(directory);
        try {
            second.addEdge(2, 3);
            Token secondToken = second.token();

            assertEquals(firstToken.epoch() + 1, secondToken.epoch());
            assertTrue(secondToken.lsn() > firstToken.lsn());
            assertEquals(2, second.epochHistory().size());
        } finally {
            second.close();
        }
    }

    @Test
    void directoryCannotBeOpenedTwiceAtTheSameTime() throws IOException {
        GraphKernel first = StoredGraphs.open(directory);
        try {
            assertThrows(IllegalStateException.class, () -> StoredGraphs.open(directory));
        } finally {
            first.close();
        }
        StoredGraphs.open(directory).close();
    }

    @Test
    void mutationsAfterCloseAreRejected() throws IOException {
        GraphKernel graph = StoredGraphs.open(directory);
        graph.close();

        assertThrows(IllegalStateException.class, () -> graph.addEdge(1, 2));
        assertThrows(IllegalStateException.class, graph::checkpoint);
    }

    @Test
    void inMemoryGraphHasNothingToCheckpointOrSync() {
        GraphKernel graph = GraphKernel.openInMemory();

        assertFalse(graph.isDurable());
        assertThrows(IllegalStateException.class, graph::checkpoint);
        assertThrows(IllegalStateException.class, graph::sync);
        graph.close();
    }

    @Test
    void aDirectoryFromAnEarlierVersionIsRefusedUntilItIsUpgraded() throws IOException {
        Files.write(directory.resolve(GraphFiles.TRIPWIRE), earlierVersionHeader());

        assertThrows(UpgradeRequiredException.class, () -> StoredGraphs.open(directory));
        assertFalse(Files.exists(directory.resolve(GraphFiles.FORMAT)));
    }

    @Test
    void aDirectoryFromALaterVersionIsRefused() throws IOException {
        Files.writeString(directory.resolve(GraphFiles.FORMAT), "nodus-format 9\n", StandardCharsets.US_ASCII);

        assertThrows(UnsupportedFeatureException.class, () -> StoredGraphs.open(directory));
    }

    @Test
    void smallSegmentsStillRecoverEverythingAcrossRolls() throws IOException {
        LogConfig small = LogConfig.DEFAULT.withSegmentBytes(4_096);
        GraphKernel graph = StoredGraphs.open(directory, small);
        for (int i = 0; i < 2_000; i++) {
            graph.addEdge(i, i + 1);
        }
        graph.close();

        GraphKernel reopened = StoredGraphs.open(directory, small);
        try {
            for (int i = 0; i < 2_000; i++) {
                assertTrue(reopened.hasEdge(i, i + 1), "edge " + i + " lost");
            }
        } finally {
            reopened.close();
        }
    }

    private static byte[] earlierVersionHeader() {
        ByteBuffer header = ByteBuffer.allocate(DirectoryFormat.TRIPWIRE_BYTES);
        header.putInt(0x4E4F4455).putShort((short) 1).putShort((short) 0).putLong(0L);
        return header.array();
    }
}
