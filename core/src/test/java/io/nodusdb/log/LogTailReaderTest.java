package io.nodusdb.log;

import io.nodusdb.error.CorruptLogException;
import io.nodusdb.log.LogTailReader.Batch;
import io.nodusdb.log.io.DirectoryLogFileSystem;
import io.nodusdb.log.io.LogChannel;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.log.record.SegmentHeader;
import io.nodusdb.log.simulation.LogHarness;
import io.nodusdb.log.simulation.LogHarness.Opened;
import io.nodusdb.log.simulation.SimulatedDisk;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogTailReaderTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final int NO_LIMIT = 1 << 30;

    @TempDir
    Path scratch;

    private static byte[] transactionBytes(long firstLsn, int tuples) {
        RecordBatch batch = new RecordBatch();
        for (int i = 0; i < tuples; i++) {
            batch.tuple(RecordType.TUPLE_ADD, i, 1, 0, i + 1);
        }
        batch.commit();
        batch.seal(firstLsn, RecordFixtures.COMMIT_MICROS);
        return RecordFixtures.copyOf(batch);
    }

    private static byte[] autocommitBytes(long lsn) {
        RecordBatch batch = new RecordBatch();
        batch.autocommitTuple(RecordType.TUPLE_ADD, 1, 2, 0, 3);
        batch.seal(lsn, RecordFixtures.COMMIT_MICROS);
        return RecordFixtures.copyOf(batch);
    }

    private static void segment(LogFileSystem files, long base, byte[]... parts) throws IOException {
        int size = SegmentHeader.BYTES;
        for (byte[] part : parts) {
            size += part.length;
        }
        ByteBuffer content = ByteBuffer.allocate(size);
        SegmentHeader.write(content, 0, 1, base);
        content.position(SegmentHeader.BYTES);
        for (byte[] part : parts) {
            content.put(part);
        }
        try (LogChannel channel = files.create(SegmentNames.of(base))) {
            channel.write(content.rewind(), 0);
            channel.force();
        }
    }

    private static byte[] firstRecords(byte[] whole, int records) {
        ByteBuffer buffer = ByteBuffer.wrap(whole);
        int offset = 0;
        for (int i = 0; i < records; i++) {
            offset += buffer.getInt(offset);
        }
        return Arrays.copyOfRange(whole, 0, offset);
    }

    private static byte[] afterRecords(byte[] whole, int records) {
        return Arrays.copyOfRange(whole, firstRecords(whole, records).length, whole.length);
    }

    private static List<Long> lsns(Batch batch) {
        List<Long> lsns = new ArrayList<>();
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(batch.records()), 0, batch.records().length);
        while (reader.hasRecord()) {
            lsns.add(reader.lsn());
            reader.advance();
        }
        return lsns;
    }

    @Test
    void aFreshLogYieldsItsTenureTransactionAndThenNothing() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            Batch batch = reader.read(opened.log().durableLsn(), NO_LIMIT);

            assertEquals(1, batch.lsnFirst());
            assertEquals(2, batch.lsnLast());
            assertEquals(List.of(1L, 2L), lsns(batch));
            assertNull(reader.read(opened.log().durableLsn(), NO_LIMIT));
            assertEquals(3, reader.nextLsn());
        }
        opened.log().close();
    }

    @Test
    void everyAppendedTransactionIsDeliveredInOrderAndOnlyOnce() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        SegmentedLog log = opened.log();
        for (int i = 0; i < 20; i++) {
            log.awaitDurable(log.append(LogHarness.tuples(i * 10, 3)));
        }
        log.force();

        List<Long> seen = new ArrayList<>();
        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            Batch batch;
            while ((batch = reader.read(log.durableLsn(), 1)) != null) {
                assertEquals(seen.size() + 1, batch.lsnFirst());
                seen.addAll(lsns(batch));
            }
        }

        assertEquals(log.lastLsn(), seen.size());
        for (int i = 0; i < seen.size(); i++) {
            assertEquals(i + 1, seen.get(i));
        }
        log.close();
    }

    @Test
    void aBatchStopsAtTheFirstCommitPastTheSizeLimitButAlwaysHoldsAWholeTransaction() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, transactionBytes(1, 3), transactionBytes(5, 3), transactionBytes(9, 3));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            Batch first = reader.read(12, 1);
            Batch rest = reader.read(12, NO_LIMIT);

            assertEquals(List.of(1L, 2L, 3L, 4L), lsns(first));
            assertEquals(List.of(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), lsns(rest));
        }
    }

    @Test
    void theDurableBoundCutsBackToTheLastWholeTransaction() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, transactionBytes(1, 3), transactionBytes(5, 3));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            Batch first = reader.read(6, NO_LIMIT);
            assertEquals(List.of(1L, 2L, 3L, 4L), lsns(first));
            assertNull(reader.read(7, NO_LIMIT));
            assertEquals(5, reader.nextLsn());
            Batch second = reader.read(8, NO_LIMIT);
            assertEquals(List.of(5L, 6L, 7L, 8L), lsns(second));
        }
    }

    @Test
    void recordsPastTheDurableBoundAreNotDeliveredEvenWhenTheyAreComplete() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, autocommitBytes(1), autocommitBytes(2), autocommitBytes(3));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            Batch batch = reader.read(2, NO_LIMIT);

            assertEquals(List.of(1L, 2L), lsns(batch));
            assertNull(reader.read(2, NO_LIMIT));
        }
    }

    @Test
    void garbageAfterTheDurableBoundIsIgnored() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, autocommitBytes(1), autocommitBytes(2));
        String name = SegmentNames.of(1);
        byte[] garbage = new byte[37];
        new Random(4).nextBytes(garbage);
        byte[] content = disk.contentOf(name);
        byte[] withGarbage = Arrays.copyOf(content, content.length + garbage.length);
        System.arraycopy(garbage, 0, withGarbage, content.length, garbage.length);
        disk.replaceContent(name, withGarbage);

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            assertEquals(List.of(1L, 2L), lsns(reader.read(2, NO_LIMIT)));
            assertNull(reader.read(2, NO_LIMIT));
        }
    }

    @Test
    void aTransactionMayCrossASegmentBoundary() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        byte[] whole = transactionBytes(1, 4);
        segment(disk, 1, firstRecords(whole, 3));
        segment(disk, 4, afterRecords(whole, 3), autocommitBytes(6));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            Batch batch = reader.read(6, NO_LIMIT);

            assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L), lsns(batch));
        }
    }

    @Test
    void anOpenTransactionWaitsForItsCommitInTheNextSegment() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        byte[] whole = transactionBytes(1, 4);
        segment(disk, 1, firstRecords(whole, 3));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            assertNull(reader.read(3, NO_LIMIT));
            segment(disk, 4, afterRecords(whole, 3));
            Batch batch = reader.read(5, NO_LIMIT);

            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), lsns(batch));
        }
    }

    @Test
    void readingMayStartAfterAnyLsnEvenInALaterSegment() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, transactionBytes(1, 3), transactionBytes(5, 3));
        segment(disk, 9, transactionBytes(9, 3), autocommitBytes(13));

        try (LogTailReader reader = new LogTailReader(disk, 4)) {
            assertEquals(List.of(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L), lsns(reader.read(13, NO_LIMIT)));
        }
        try (LogTailReader reader = new LogTailReader(disk, 8)) {
            assertEquals(List.of(9L, 10L, 11L, 12L, 13L), lsns(reader.read(13, NO_LIMIT)));
        }
        try (LogTailReader reader = new LogTailReader(disk, 12)) {
            assertEquals(List.of(13L), lsns(reader.read(13, NO_LIMIT)));
        }
    }

    @Test
    void aReaderPastTheEndOfEverythingWaits() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, autocommitBytes(1));

        try (LogTailReader reader = new LogTailReader(disk, 1)) {
            assertNull(reader.read(1, NO_LIMIT));
            assertNull(reader.read(0, NO_LIMIT));
        }
    }

    @Test
    void anLsnTheLogNoLongerHoldsIsAnError() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 50, transactionBytes(50, 2));

        try (LogTailReader reader = new LogTailReader(disk, 10)) {
            CorruptLogException refused = assertThrows(CorruptLogException.class, () -> reader.read(52, NO_LIMIT));

            assertTrue(refused.getMessage().contains("no longer holds LSN 11"), refused.getMessage());
        }
    }

    @Test
    void damageInsideTheDurablePartIsCorruption() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, transactionBytes(1, 3));
        String name = SegmentNames.of(1);
        byte[] content = disk.contentOf(name);
        content[SegmentHeader.BYTES + 20] ^= 0x01;
        disk.replaceContent(name, content);

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            assertThrows(CorruptLogException.class, () -> reader.read(4, NO_LIMIT));
        }
    }

    @Test
    void aRecordCutShortInsideTheDurablePartIsCorruption() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        byte[] whole = transactionBytes(1, 3);
        segment(disk, 1, Arrays.copyOf(whole, whole.length - 10));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            assertThrows(CorruptLogException.class, () -> reader.read(4, NO_LIMIT));
        }
    }

    @Test
    void aSkippedLsnIsCorruption() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, autocommitBytes(1), autocommitBytes(3));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            CorruptLogException refused = assertThrows(CorruptLogException.class, () -> reader.read(3, NO_LIMIT));

            assertTrue(refused.getMessage().contains("expected LSN 2 but found 3"), refused.getMessage());
        }
    }

    @Test
    void aMissingNextSegmentIsCorruptionWhenTheLsnIsDurable() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, autocommitBytes(1));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            CorruptLogException refused = assertThrows(CorruptLogException.class, () -> reader.read(2, NO_LIMIT));

            assertTrue(refused.getMessage().contains("no segment starts there"), refused.getMessage());
        }
    }

    @Test
    void aDamagedSegmentHeaderIsCorruption() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        segment(disk, 1, autocommitBytes(1));
        String name = SegmentNames.of(1);
        byte[] content = disk.contentOf(name);
        content[17] ^= 0x01;
        disk.replaceContent(name, content);

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            assertThrows(CorruptLogException.class, () -> reader.read(1, NO_LIMIT));
        }
    }

    @Test
    void aCommitThatDoesNotMatchItsRecordsIsCorruption() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        byte[] whole = transactionBytes(1, 3);
        segment(disk, 1, firstRecords(whole, 1), afterRecords(whole, 2));

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            assertThrows(CorruptLogException.class, () -> reader.read(4, NO_LIMIT));
        }
    }

    @Test
    void theUnreadBytesShrinkAsTheReaderAdvances() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        byte[] first = transactionBytes(1, 3);
        byte[] second = transactionBytes(5, 3);
        segment(disk, 1, first, second);

        try (LogTailReader reader = new LogTailReader(disk, 0)) {
            assertEquals(first.length + second.length, reader.unreadBytes());
            reader.read(4, NO_LIMIT);
            assertEquals(second.length, reader.unreadBytes());
            reader.read(8, NO_LIMIT);
            assertEquals(0, reader.unreadBytes());
        }
    }

    @Test
    void theUnreadBytesCoverEverySegmentFromTheCursorOn() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        byte[] a = transactionBytes(1, 3);
        byte[] b = transactionBytes(5, 3);
        byte[] c = autocommitBytes(9);
        segment(disk, 1, a);
        segment(disk, 5, b);
        segment(disk, 9, c);

        try (LogTailReader reader = new LogTailReader(disk, 4)) {
            assertEquals(b.length + c.length, reader.unreadBytes());
            reader.read(5, NO_LIMIT);
            assertEquals(b.length + c.length, reader.unreadBytes());
        }
    }

    @Test
    void aReaderKeepsUpWithAWriterThatRollsSegmentsConstantly() throws Exception {
        Path directory = scratch.resolve("log");
        Files.createDirectories(directory);
        DirectoryLogFileSystem files = new DirectoryLogFileSystem(directory);
        Opened opened = LogHarness.open(files, SYNC.withSegmentBytes(2_048), 0);
        SegmentedLog log = opened.log();
        int transactions = 600;
        Thread writer = new Thread(() -> {
            for (int i = 0; i < transactions; i++) {
                log.awaitDurable(log.append(LogHarness.tuples(i * 10, 1 + i % 4)));
            }
        });
        writer.start();

        List<Long> seen = new ArrayList<>();
        try (LogTailReader reader = new LogTailReader(files, 0)) {
            long deadline = System.nanoTime() + 60_000_000_000L;
            while ((writer.isAlive() || reader.nextLsn() <= log.lastLsn()) && System.nanoTime() < deadline) {
                log.force();
                Batch batch = reader.read(log.durableLsn(), 4_096);
                if (batch != null) {
                    seen.addAll(lsns(batch));
                }
            }
        }
        writer.join();

        assertEquals(log.lastLsn(), seen.size());
        for (int i = 0; i < seen.size(); i++) {
            assertEquals(i + 1, seen.get(i));
        }
        assertTrue(log.segmentBases().size() > 10, "the writer should have rolled many segments");
        log.close();
    }

    @Test
    void trimmingBehindTheReaderNeverDisturbsIt() throws Exception {
        Path directory = scratch.resolve("trimmed");
        Files.createDirectories(directory);
        DirectoryLogFileSystem files = new DirectoryLogFileSystem(directory);
        Opened opened = LogHarness.open(files, LogConfig.DEFAULT.withSegmentBytes(2_048), 0);
        SegmentedLog log = opened.log();

        List<Long> seen = new ArrayList<>();
        try (LogTailReader reader = new LogTailReader(files, 0)) {
            for (int round = 0; round < 12; round++) {
                for (int i = 0; i < 30; i++) {
                    log.append(LogHarness.tuples(i, 2));
                }
                log.force();
                Batch batch;
                while ((batch = reader.read(log.durableLsn(), 4_096)) != null) {
                    seen.addAll(lsns(batch));
                }
                log.trim(reader.nextLsn() - 1);
            }
        }

        assertEquals(log.lastLsn(), seen.size());
        assertTrue(log.segmentBases().size() < 6, "trim should have removed the segments the reader finished");
        log.close();
    }

    @Test
    void closingTheReaderReleasesEverySegmentFile() throws Exception {
        Path directory = scratch.resolve("release");
        Files.createDirectories(directory);
        DirectoryLogFileSystem files = new DirectoryLogFileSystem(directory);
        Opened opened = LogHarness.open(files, LogConfig.DEFAULT.withSegmentBytes(2_048), 0);
        SegmentedLog log = opened.log();
        for (int i = 0; i < 100; i++) {
            log.append(LogHarness.tuples(i, 3));
        }
        log.force();
        LogTailReader reader = new LogTailReader(files, 0);
        assertNotNull(reader.read(log.durableLsn(), NO_LIMIT));
        reader.close();
        log.close();

        List<Path> segments;
        try (var listing = Files.list(directory)) {
            segments = listing.filter(path -> path.toString().endsWith(".nlog")).toList();
        }
        for (Path segment : segments) {
            Files.delete(segment);
        }
        assertEquals(0, Files.list(directory).filter(path -> path.toString().endsWith(".nlog")).count());
    }

    @Test
    void theReaderOnlyOpensSegmentsForReading() throws Exception {
        Path directory = scratch.resolve("readonly");
        Files.createDirectories(directory);
        DirectoryLogFileSystem files = new DirectoryLogFileSystem(directory);
        Opened opened = LogHarness.open(files, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 2)));
        Path segment = directory.resolve(SegmentNames.of(1));
        byte[] before = Files.readAllBytes(segment);

        try (LogTailReader reader = new LogTailReader(files, 0)) {
            reader.read(opened.log().durableLsn(), NO_LIMIT);
        }

        assertArrayEquals(before, Files.readAllBytes(segment));
        opened.log().close();
    }

    @Test
    void aNegativeStartingLsnIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new LogTailReader(new SimulatedDisk(), -1));
    }
}
