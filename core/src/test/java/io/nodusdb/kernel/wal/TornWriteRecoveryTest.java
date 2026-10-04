package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TornWriteRecoveryTest {

    private static final int NODES = 20_000;
    private static final int FRAMES = 1_000;
    private static final int HUNDRED_THOUSAND = 100_000;
    private static final long HEADER = WalFormat.HEADER_BYTES;
    private static final long FRAME = WalFormat.FRAME_BYTES;

    @TempDir
    Path directory;

    @Test
    void partialTailFrameIsTruncatedAndIntactFramesSurvive() throws IOException {
        long[] pairs = writeFramesThenCrash(FRAMES);
        Path log = directory.resolve(DurableStore.LOG);
        byte[] garbage = new byte[11];
        new Random(99L).nextBytes(garbage);
        Files.write(log, garbage, StandardOpenOption.APPEND);

        RecoveryManager.Recovery recovery = RecoveryManager.recover(directory, WalConfig.DEFAULT);
        try {
            GraphKernel kernel = recovery.kernel();
            assertEquals(FRAMES, recovery.framesApplied());
            assertEquals(11, recovery.truncatedBytes());
            assertEquals(HEADER + FRAMES * FRAME, Files.size(log));
            assertEquals(FRAMES, GraphFixtures.edgeCount(kernel, NODES));
            for (int i = 0; i < FRAMES; i++) {
                assertTrue(kernel.hasEdge(pairs[2 * i], pairs[2 * i + 1]), "edge " + i + " lost");
            }
        } finally {
            recovery.kernel().close();
        }
    }

    @Test
    void hundredThousandEdgesSurviveAFourteenByteTornRecord() throws IOException {
        long[] pairs = writeFramesThenCrash(HUNDRED_THOUSAND);
        Path log = directory.resolve(DurableStore.LOG);
        byte[] torn = new byte[14];
        new Random(14L).nextBytes(torn);
        Files.write(log, torn, StandardOpenOption.APPEND);

        RecoveryManager.Recovery recovery = RecoveryManager.recover(directory, WalConfig.DEFAULT);
        try {
            GraphKernel kernel = recovery.kernel();
            assertEquals(14, recovery.truncatedBytes());
            assertEquals(HUNDRED_THOUSAND, recovery.framesApplied());
            assertEquals(HEADER + HUNDRED_THOUSAND * FRAME, Files.size(log));
            assertEquals(HUNDRED_THOUSAND, GraphFixtures.edgeCount(kernel, NODES));
            for (int i = 0; i < HUNDRED_THOUSAND; i++) {
                assertTrue(kernel.hasEdge(pairs[2 * i], pairs[2 * i + 1]), "edge " + i + " lost");
            }
            GraphFixtures.assertInDegreesMatchForward(kernel, NODES);
        } finally {
            recovery.kernel().close();
        }
    }

    @Test
    void checksumMismatchStopsReplayAtTheDamagedFrame() throws IOException {
        writeFramesThenCrash(FRAMES);
        Path log = directory.resolve(DurableStore.LOG);
        int damagedFrame = 500;
        flipOneBit(log, HEADER + damagedFrame * FRAME + 9);

        RecoveryManager.Recovery recovery = RecoveryManager.recover(directory, WalConfig.DEFAULT);
        try {
            assertEquals(damagedFrame, recovery.framesApplied());
            assertEquals((FRAMES - damagedFrame) * FRAME, recovery.truncatedBytes());
            assertEquals(damagedFrame, GraphFixtures.edgeCount(recovery.kernel(), NODES));
            assertEquals(HEADER + damagedFrame * FRAME, Files.size(log));
        } finally {
            recovery.kernel().close();
        }
    }

    private long[] writeFramesThenCrash(int frames) throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(21L, frames, NODES);
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.withSyncMode(SyncMode.SYNC));
        opened.kernel().addEdges(pairs, frames);
        opened.store().abandon();
        return pairs;
    }

    private static void flipOneBit(Path file, long position) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, position);
            one.flip();
            byte damaged = (byte) (one.get(0) ^ 0x40);
            channel.write(ByteBuffer.wrap(new byte[] {damaged}), position);
        }
    }
}
