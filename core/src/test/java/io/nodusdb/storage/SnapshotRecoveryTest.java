package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnapshotRecoveryTest {

    private static final int NODES = 20_000;
    private static final int FIRST = 1_000;
    private static final int SECOND = 500;
    private static final int SECTION_BODY_OFFSET = 48 + 20;

    @TempDir
    Path directory;

    @TempDir
    Path image;

    @Test
    void crashAfterSnapshotRenameBeforeLogTrimReplaysOnlyWhatTheSnapshotLacks() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(41L, FIRST + SECOND, NODES);
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            live.addEdges(pairs, FIRST);
            installSnapshotWithoutTrimming(live);
            live.addEdges(Arrays.copyOfRange(pairs, 2 * FIRST, 2 * (FIRST + SECOND)), SECOND);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        Recovery recovery = StoredGraphs.recover(image);
        try {
            assertEquals(FIRST + SECOND, GraphFixtures.edgeCount(recovery.kernel(), NODES));
            assertEquals(SECOND, recovery.recordsReplayed());
            GraphFixtures.assertInDegreesMatchForward(recovery.kernel(), NODES);
        } finally {
            recovery.kernel().close();
        }
    }

    @Test
    void removalAfterSnapshotStillWinsOverSnapshotContents() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(42L, FIRST, NODES);
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            live.addEdges(pairs, FIRST);
            installSnapshotWithoutTrimming(live);
            live.removeEdge(pairs[0], pairs[1]);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        Recovery recovery = StoredGraphs.recover(image);
        try {
            assertFalse(recovery.kernel().hasEdge(pairs[0], pairs[1]), "removed edge came back");
            assertEquals(FIRST - 1, GraphFixtures.edgeCount(recovery.kernel(), NODES));
        } finally {
            recovery.kernel().close();
        }
    }

    @Test
    void corruptedSnapshotBodyIsRejectedBeforeAnyEdgeIsApplied() throws IOException {
        writeCleanSnapshot();
        flipOneBit(directory.resolve(GraphFiles.SNAPSHOT), SECTION_BODY_OFFSET + 1);

        assertThrows(IOException.class, () -> StoredGraphs.open(directory));
    }

    @Test
    void corruptedSnapshotHeaderIsRejected() throws IOException {
        writeCleanSnapshot();
        flipOneBit(directory.resolve(GraphFiles.SNAPSHOT), 12);

        assertThrows(IOException.class, () -> StoredGraphs.open(directory));
    }

    @Test
    void truncatedSnapshotIsRejected() throws IOException {
        writeCleanSnapshot();
        Path snapshot = directory.resolve(GraphFiles.SNAPSHOT);
        try (FileChannel channel = FileChannel.open(snapshot, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() / 2);
        }

        assertThrows(IOException.class, () -> StoredGraphs.open(directory));
    }

    @Test
    void aSnapshotWithTrailingBytesIsRejected() throws IOException {
        writeCleanSnapshot();
        Files.write(directory.resolve(GraphFiles.SNAPSHOT), new byte[] {1, 2, 3}, StandardOpenOption.APPEND);

        assertThrows(IOException.class, () -> StoredGraphs.open(directory));
    }

    private void installSnapshotWithoutTrimming(GraphKernel kernel) throws IOException {
        kernel.sync();
        SnapshotMeta meta = new SnapshotMeta(kernel.appliedLsn(), kernel.epoch(), 0, new byte[32]);
        Path temporary = directory.resolve(GraphFiles.SNAPSHOT_TEMP);
        SnapshotWriter.write(kernel, meta, temporary);
        Files.move(temporary, directory.resolve(GraphFiles.SNAPSHOT), StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    private void writeCleanSnapshot() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(43L, FIRST, NODES);
        GraphKernel kernel = StoredGraphs.open(directory);
        kernel.addEdges(pairs, FIRST);
        kernel.checkpoint();
        kernel.close();
    }

    private static void flipOneBit(Path file, long position) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, position);
            one.flip();
            byte damaged = (byte) (one.get(0) ^ 0x10);
            channel.write(ByteBuffer.wrap(new byte[] {damaged}), position);
        }
    }
}
