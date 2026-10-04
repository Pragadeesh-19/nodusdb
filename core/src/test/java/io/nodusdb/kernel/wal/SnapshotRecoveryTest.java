package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotRecoveryTest {

    private static final int NODES = 20_000;
    private static final int FIRST = 1_000;
    private static final int SECOND = 500;

    @TempDir
    Path directory;

    @Test
    void crashAfterSnapshotRenameBeforeLogResetReplaysTheWholeLogIdempotently() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(41L, FIRST + SECOND, NODES);
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.withSyncMode(SyncMode.SYNC));
        opened.kernel().addEdges(pairs, FIRST);
        Path temporary = directory.resolve(DurableStore.SNAPSHOT_TEMP);
        SnapshotFile.write(opened.kernel(), temporary);
        Files.move(temporary, directory.resolve(DurableStore.SNAPSHOT), StandardCopyOption.ATOMIC_MOVE);
        opened.kernel().addEdges(Arrays.copyOfRange(pairs, 2 * FIRST, 2 * (FIRST + SECOND)), SECOND);
        opened.store().abandon();

        RecoveryManager.Recovery recovery = RecoveryManager.recover(directory, WalConfig.DEFAULT);
        try {
            assertEquals(FIRST + SECOND, GraphFixtures.edgeCount(recovery.kernel(), NODES));
            assertEquals(FIRST + SECOND, recovery.framesApplied());
            GraphFixtures.assertInDegreesMatchForward(recovery.kernel(), NODES);
        } finally {
            recovery.kernel().close();
        }
    }

    @Test
    void removalAfterSnapshotStillWinsOverSnapshotContents() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(42L, FIRST, NODES);
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.withSyncMode(SyncMode.SYNC));
        opened.kernel().addEdges(pairs, FIRST);
        Path temporary = directory.resolve(DurableStore.SNAPSHOT_TEMP);
        SnapshotFile.write(opened.kernel(), temporary);
        Files.move(temporary, directory.resolve(DurableStore.SNAPSHOT), StandardCopyOption.ATOMIC_MOVE);
        opened.kernel().removeEdge(pairs[0], pairs[1]);
        opened.store().abandon();

        RecoveryManager.Recovery recovery = RecoveryManager.recover(directory, WalConfig.DEFAULT);
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
        flipOneBit(directory.resolve(DurableStore.SNAPSHOT), SnapshotFile.HEADER_BYTES + 3);

        assertThrows(IOException.class, () -> GraphKernel.open(directory, WalConfig.DEFAULT));
    }

    @Test
    void truncatedSnapshotIsRejected() throws IOException {
        writeCleanSnapshot();
        Path snapshot = directory.resolve(DurableStore.SNAPSHOT);
        try (FileChannel channel = FileChannel.open(snapshot, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() / 2);
        }

        assertThrows(IOException.class, () -> GraphKernel.open(directory, WalConfig.DEFAULT));
    }

    private void writeCleanSnapshot() throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(43L, FIRST, NODES);
        GraphKernel kernel = GraphKernel.open(directory, WalConfig.DEFAULT);
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
