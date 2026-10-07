package io.nodusdb.storage;

import io.nodusdb.error.CorruptLogException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.record.RecordFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TornWriteRecoveryTest {

    private static final int NODES = 20_000;
    private static final int RECORDS = 1_000;
    private static final int HUNDRED_THOUSAND = 100_000;
    private static final int SEGMENT_HEADER_BYTES = 32;
    private static final int TENURE_BYTES = RecordFormat.EPOCH_BYTES + RecordFormat.COMMIT_BYTES;
    private static final int MARK_SLOT_BYTES = 32;
    private static final long MARK_WAIT_MILLIS = 10_000;

    @TempDir
    Path directory;

    @TempDir
    Path image;

    @Test
    void partialTailRecordIsTruncatedAndIntactRecordsSurvive() throws IOException {
        long[] pairs = writeThenCrash(RECORDS, StoredGraphs.SYNC);
        Path tail = lastSegment(image);
        long intactSize = Files.size(tail);
        byte[] garbage = new byte[11];
        new Random(99L).nextBytes(garbage);
        Files.write(tail, garbage, StandardOpenOption.APPEND);

        Recovery recovery = StoredGraphs.recover(image);
        try {
            GraphKernel kernel = recovery.kernel();
            assertEquals(RECORDS, recovery.recordsReplayed());
            assertEquals(11, recovery.truncatedBytes());
            assertEquals(intactSize + TENURE_BYTES, Files.size(tail));
            assertEquals(RECORDS, GraphFixtures.edgeCount(kernel, NODES));
            for (int i = 0; i < RECORDS; i++) {
                assertTrue(kernel.hasEdge(pairs[2 * i], pairs[2 * i + 1]), "edge " + i + " lost");
            }
        } finally {
            recovery.kernel().close();
        }
    }

    @Test
    void hundredThousandEdgesSurviveAFourteenByteTornRecord() throws IOException {
        long[] pairs = writeThenCrash(HUNDRED_THOUSAND, StoredGraphs.SYNC);
        Path tail = lastSegment(image);
        long intactSize = Files.size(tail);
        byte[] torn = new byte[14];
        new Random(14L).nextBytes(torn);
        Files.write(tail, torn, StandardOpenOption.APPEND);

        Recovery recovery = StoredGraphs.recover(image);
        try {
            GraphKernel kernel = recovery.kernel();
            assertEquals(14, recovery.truncatedBytes());
            assertEquals(HUNDRED_THOUSAND, recovery.recordsReplayed());
            assertEquals(intactSize + TENURE_BYTES, Files.size(tail));
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
    void damageInsideTheDurablePartOfTheLogRefusesToOpen() throws IOException {
        LogConfig small = new LogConfig(StoredGraphs.SYNC_WITH_FAST_MARKS.syncMode(), 1L,
                StoredGraphs.SYNC_WITH_FAST_MARKS.bufferBytes(), 4_096L, 1L);
        GraphKernel live = StoredGraphs.open(directory, small);
        try {
            for (int i = 0; i < 500; i++) {
                live.addEdge(i, i + 1);
            }
            List<Path> segments = segments(directory);
            assertTrue(segments.size() > 2, "the log never rolled");
            awaitMarkBeyond(directory, baseLsnOf(segments.get(1)));
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }
        flipOneBit(segments(image).get(0), SEGMENT_HEADER_BYTES + 9);

        assertThrows(CorruptLogException.class, () -> StoredGraphs.open(image));
    }

    private long[] writeThenCrash(int edges, LogConfig config) throws IOException {
        long[] pairs = GraphFixtures.skewedPairs(21L, edges, NODES);
        GraphKernel live = StoredGraphs.open(directory, config);
        try {
            live.addEdges(pairs, edges);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }
        return pairs;
    }

    private static List<Path> segments(Path root) throws IOException {
        try (Stream<Path> files = Files.list(root.resolve(GraphFiles.LOG_DIRECTORY))) {
            return files.filter(path -> path.getFileName().toString().endsWith(".nlog")).sorted().toList();
        }
    }

    private static Path lastSegment(Path root) throws IOException {
        List<Path> segments = segments(root);
        return segments.get(segments.size() - 1);
    }

    private static long baseLsnOf(Path segment) {
        String name = segment.getFileName().toString();
        return Long.parseLong(name.substring(0, name.indexOf('.')));
    }

    private static void awaitMarkBeyond(Path root, long segmentBase) throws IOException {
        long deadline = System.nanoTime() + MARK_WAIT_MILLIS * 1_000_000L;
        while (markSegment(root) < segmentBase) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the forced mark never advanced past segment " + segmentBase);
            }
            Thread.onSpinWait();
        }
    }

    private static long markSegment(Path root) throws IOException {
        Path mark = root.resolve(GraphFiles.LOG_DIRECTORY).resolve("FORCED");
        if (!Files.exists(mark)) {
            return -1;
        }
        ByteBuffer slots = ByteBuffer.wrap(Files.readAllBytes(mark));
        long bestSequence = -1;
        long segment = -1;
        for (int offset = 0; offset + MARK_SLOT_BYTES <= slots.capacity(); offset += MARK_SLOT_BYTES) {
            long sequence = slots.getLong(offset + 4);
            if (sequence > bestSequence) {
                bestSequence = sequence;
                segment = slots.getLong(offset + 12);
            }
        }
        return segment;
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
