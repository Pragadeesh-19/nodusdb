package io.nodusdb.log;

import io.nodusdb.error.CorruptLogException;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFormat;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.log.record.SegmentHeader;
import io.nodusdb.log.simulation.LogHarness;
import io.nodusdb.log.simulation.LogHarness.CollectingSink;
import io.nodusdb.log.simulation.LogHarness.Opened;
import io.nodusdb.log.simulation.SimulatedDisk;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogRecoveryTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);

    @Test
    void anEmptyDirectoryRecoversToTheSnapshotPoint() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();

        RecoveryResult result = LogRecovery.recover(disk, 50, new CollectingSink());

        assertEquals(50, result.lastLsn());
        assertFalse(result.hasSegments());
    }

    @Test
    void aTornTailAboveTheMarkIsTruncatedAndEveryCommittedUnitSurvives() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 3)));
        opened.log().force();
        opened.log().awaitDurable(opened.log().append(LogHarness.single(9, 10)));
        opened.log().abort();
        String segment = lastSegment(disk);
        byte[] garbage = new byte[11];
        new Random(5L).nextBytes(garbage);
        disk.replaceContent(segment, concat(disk.contentOf(segment), garbage));

        CollectingSink sink = new CollectingSink();
        RecoveryResult result = LogRecovery.recover(disk, 0, sink);

        assertEquals(3, sink.units().size());
        assertEquals(11, result.truncatedBytes());
        assertEquals(disk.contentOf(segment).length, result.tailOffset());
    }

    @Test
    void damageInsideForcedDataRefusesToOpen() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        for (int i = 0; i < 5; i++) {
            opened.log().awaitDurable(opened.log().append(LogHarness.tuples(i, 2)));
        }
        opened.log().close();
        String segment = lastSegment(disk);
        byte[] content = disk.contentOf(segment);
        content[SegmentHeader.BYTES + 130] ^= 0x10;
        disk.replaceContent(segment, content);

        CorruptLogException failure = assertThrows(CorruptLogException.class,
                () -> LogRecovery.recover(disk, 0, new CollectingSink()));
        assertTrue(failure.getMessage().contains("durable"), failure.getMessage());
    }

    @Test
    void aRecordCutShortInsideForcedDataRefusesToOpen() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 4)));
        opened.log().close();
        String segment = lastSegment(disk);
        byte[] content = disk.contentOf(segment);
        disk.replaceContent(segment, Arrays.copyOf(content, content.length - 10));

        assertThrows(CorruptLogException.class, () -> LogRecovery.recover(disk, 0, new CollectingSink()));
    }

    @Test
    void aLogShorterThanTheForcedMarkRefusesToOpen() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 4)));
        opened.log().close();
        String segment = lastSegment(disk);
        byte[] content = disk.contentOf(segment);
        int firstUnitEnd = SegmentHeader.BYTES + RecordFormat.EPOCH_BYTES + RecordFormat.COMMIT_BYTES;
        disk.replaceContent(segment, Arrays.copyOf(content, firstUnitEnd));

        assertThrows(CorruptLogException.class, () -> LogRecovery.recover(disk, 0, new CollectingSink()));
    }

    @Test
    void anUncommittedTransactionAtTheEndIsDiscardedAndTruncated() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 2)));
        long lastLsn = opened.log().lastLsn();
        opened.log().close();
        String segment = lastSegment(disk);
        int committedEnd = disk.contentOf(segment).length;
        byte[] uncommitted = uncommittedTransaction(lastLsn + 1, 3);
        disk.replaceContent(segment, concat(disk.contentOf(segment), uncommitted));

        CollectingSink sink = new CollectingSink();
        RecoveryResult result = LogRecovery.recover(disk, 0, sink);

        assertEquals(2, sink.units().size());
        assertEquals(3, result.discardedRecords());
        assertEquals(lastLsn, result.lastLsn());
        assertEquals(uncommitted.length, result.truncatedBytes());
        assertEquals(committedEnd, disk.contentOf(segment).length);
    }

    @Test
    void recoveryLowersTheMarkWhenItTruncatesBelowIt() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 2)));
        long lastLsn = opened.log().lastLsn();
        opened.log().close();
        String segment = lastSegment(disk);
        long committedEnd = disk.contentOf(segment).length;
        byte[] uncommitted = uncommittedTransaction(lastLsn + 1, 3);
        disk.replaceContent(segment, concat(disk.contentOf(segment), uncommitted));
        try (ForcedMark mark = ForcedMark.open(disk)) {
            mark.record(SegmentNames.baseLsnOf(segment), committedEnd + uncommitted.length);
        }

        LogRecovery.recover(disk, 0, new CollectingSink());
        CollectingSink again = new CollectingSink();
        RecoveryResult second = LogRecovery.recover(disk, 0, again);

        assertEquals(2, again.units().size());
        assertEquals(0, second.truncatedBytes());
        assertEquals(committedEnd, ForcedMark.read(disk).offset());
    }

    @Test
    void aMissingSegmentIsCorruption() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC.withSegmentBytes(1024), 0);
        for (int i = 0; i < 40; i++) {
            opened.log().awaitDurable(opened.log().append(LogHarness.tuples(i, 4)));
        }
        List<Long> bases = opened.log().segmentBases();
        opened.log().close();
        disk.delete(SegmentNames.of(bases.get(2)));

        CorruptLogException failure = assertThrows(CorruptLogException.class,
                () -> LogRecovery.recover(disk, 0, new CollectingSink()));
        assertTrue(failure.getMessage().contains("missing"), failure.getMessage());
    }

    @Test
    void aSegmentWhoseHeaderWasNeverForcedIsDeleted() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 2)));
        long lastLsn = opened.log().lastLsn();
        opened.log().close();
        String torn = SegmentNames.of(lastLsn + 1);
        disk.create(torn).write(ByteBuffer.wrap(new byte[10]), 0);

        RecoveryResult result = LogRecovery.recover(disk, 0, new CollectingSink());

        assertFalse(disk.exists(torn));
        assertEquals(lastLsn, result.lastLsn());
    }

    @Test
    void unitsAtOrBelowTheSnapshotAreSkippedButStillCounted() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(1, 2)));
        long snapshotLsn = opened.log().lastLsn();
        opened.log().awaitDurable(opened.log().append(LogHarness.tuples(7, 2)));
        opened.log().awaitDurable(opened.log().append(LogHarness.single(5, 6)));
        long lastLsn = opened.log().lastLsn();
        opened.log().close();

        CollectingSink sink = new CollectingSink();
        RecoveryResult result = LogRecovery.recover(disk, snapshotLsn, sink);

        assertEquals(2, sink.units().size());
        assertEquals(List.of("TUPLE_ADD 7 1 0 8", "TUPLE_ADD 8 1 0 9"), sink.units().get(0).records());
        assertEquals(lastLsn, result.lastLsn());
    }

    @Test
    void aLogThatStartsAfterTheSnapshotLeavesAGap() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 100);
        opened.log().awaitDurable(opened.log().append(LogHarness.single(1, 2)));
        opened.log().close();

        assertThrows(CorruptLogException.class, () -> LogRecovery.recover(disk, 10, new CollectingSink()));
    }

    @Test
    void aLogThatEndsBeforeTheSnapshotIsCorrupt() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        opened.log().awaitDurable(opened.log().append(LogHarness.single(1, 2)));
        opened.log().close();

        assertThrows(CorruptLogException.class, () -> LogRecovery.recover(disk, 1_000, new CollectingSink()));
    }

    @Test
    void aCommitThatDisagreesWithItsTransactionIsCorruptEvenWithAValidChecksum() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        long lastLsn = opened.log().lastLsn();
        opened.log().close();
        String segment = lastSegment(disk);
        byte[] transaction = committedTransaction(lastLsn + 1, 2);
        int commitStart = transaction.length - RecordFormat.COMMIT_BYTES;
        ByteBuffer.wrap(transaction).putInt(commitStart + RecordFormat.COMMIT_COUNT_OFFSET, 5);
        reseal(transaction, commitStart);
        disk.replaceContent(segment, concat(disk.contentOf(segment), transaction));

        assertThrows(CorruptLogException.class, () -> LogRecovery.recover(disk, 0, new CollectingSink()));
    }

    private static String lastSegment(SimulatedDisk disk) {
        return disk.list().stream().filter(SegmentNames::isSegment).max(String::compareTo).orElseThrow();
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] joined = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }

    private static byte[] committedTransaction(long firstLsn, int tuples) {
        RecordBatch batch = new RecordBatch();
        for (int i = 0; i < tuples; i++) {
            batch.tuple(RecordType.TUPLE_ADD, 100 + i, 1, 0, 200 + i);
        }
        batch.commit();
        batch.seal(firstLsn, 1_000);
        byte[] bytes = new byte[batch.size()];
        batch.bytes().get(0, bytes);
        return bytes;
    }

    private static byte[] uncommittedTransaction(long firstLsn, int tuples) {
        byte[] full = committedTransaction(firstLsn, tuples);
        return Arrays.copyOf(full, full.length - RecordFormat.COMMIT_BYTES);
    }

    private static void reseal(byte[] bytes, int recordStart) {
        ByteBuffer view = ByteBuffer.wrap(bytes);
        int length = view.getInt(recordStart);
        CRC32C crc = new CRC32C();
        crc.update(bytes, recordStart, length - RecordFormat.CHECKSUM_BYTES);
        view.putInt(recordStart + length - RecordFormat.CHECKSUM_BYTES, (int) crc.getValue());
    }
}
