package io.nodusdb.log;

import io.nodusdb.error.IndeterminateOutcomeException;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.simulation.LogHarness;
import io.nodusdb.log.simulation.LogHarness.CollectingSink;
import io.nodusdb.log.simulation.LogHarness.Opened;
import io.nodusdb.log.simulation.LogHarness.Unit;
import io.nodusdb.log.simulation.SimulatedDisk;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentedLogTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final LogConfig ASYNC = LogConfig.withSyncMode(SyncMode.ASYNC);

    @Test
    void aFreshLogBeginsATenureAndRecoversIt() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);

        assertEquals(1, opened.log().epoch());
        assertEquals(2, opened.log().lastLsn());
        assertEquals(2, opened.log().durableLsn());
        opened.log().close();

        List<Unit> units = LogHarness.recoverOnly(disk, 0).units();
        assertEquals(1, units.size());
        assertEquals(List.of("EPOCH 1 0"), units.get(0).records());
        assertEquals(2, units.get(0).commitLsn());
    }

    @Test
    void transactionsRecoverInOrderAndEachReopenStartsANewTenure() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened first = LogHarness.open(disk, SYNC, 0);
        first.log().awaitDurable(first.log().append(LogHarness.tuples(10, 3)));
        first.log().awaitDurable(first.log().append(LogHarness.single(1, 2)));
        first.log().close();

        Opened second = LogHarness.open(disk, SYNC, 0);
        assertEquals(2, second.log().epoch());
        assertEquals(7, second.recovered().units().stream().mapToLong(Unit::commitLsn).max().orElseThrow());
        second.log().close();

        List<Unit> units = LogHarness.recoverOnly(disk, 0).units();
        assertEquals(4, units.size());
        assertEquals(List.of("EPOCH 1 0"), units.get(0).records());
        assertEquals(List.of("TUPLE_ADD 10 1 0 11", "TUPLE_ADD 11 1 0 12", "TUPLE_ADD 12 1 0 13"),
                units.get(1).records());
        assertEquals(List.of("TUPLE_ADD 1 1 0 2"), units.get(2).records());
        assertEquals(List.of("EPOCH 2 7"), units.get(3).records());
        assertEquals(9, units.get(3).commitLsn());
    }

    @Test
    void commitTimeNeverGoesBackwardsEvenWhenTheClockDoes() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        AtomicLong clock = new AtomicLong(1_000_000);
        Opened opened = LogHarness.open(disk, SYNC, 0, () -> clock.getAndAdd(-1_000));
        for (int i = 0; i < 5; i++) {
            opened.log().awaitDurable(opened.log().append(LogHarness.single(i, i + 1)));
        }
        opened.log().close();

        List<Unit> units = LogHarness.recoverOnly(disk, 0).units();
        for (int i = 1; i < units.size(); i++) {
            assertTrue(units.get(i).commitMicros() > units.get(i - 1).commitMicros(), "unit " + i);
        }
    }

    @Test
    void segmentsRollWhenFullAndRecoveryReadsAcrossTheBoundaries() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC.withSegmentBytes(1024), 0);
        List<List<String>> expected = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            RecordBatch batch = LogHarness.tuples(i * 10, 4);
            expected.add(LogHarness.describeAll(batch));
            opened.log().awaitDurable(opened.log().append(batch));
        }
        opened.log().close();

        long segments = disk.list().stream().filter(name -> name.endsWith(".nlog")).count();
        assertTrue(segments > 3, "expected several segments, found " + segments);
        List<Unit> units = LogHarness.recoverOnly(disk, 0).units();
        assertEquals(41, units.size());
        for (int i = 0; i < 40; i++) {
            assertEquals(expected.get(i), units.get(i + 1).records(), "unit " + i);
        }
    }

    @Test
    void trimDeletesOnlySegmentsWhollyAtOrBelowTheWatermark() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC.withSegmentBytes(1024), 0);
        for (int i = 0; i < 40; i++) {
            opened.log().awaitDurable(opened.log().append(LogHarness.tuples(i * 10, 4)));
        }
        SegmentedLog log = opened.log();
        List<Long> before = log.segmentBases();

        log.trim(before.get(2) - 1);

        List<Long> after = log.segmentBases();
        assertEquals(before.subList(2, before.size()), after);
        log.trim(Long.MAX_VALUE);
        assertEquals(1, log.segmentBases().size());
        assertEquals(before.get(before.size() - 1), log.segmentBases().get(0));
        log.close();
    }

    @Test
    void rollSegmentStartsANewSegmentAtTheNextLsn() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        SegmentedLog log = opened.log();
        log.awaitDurable(log.append(LogHarness.tuples(1, 2)));

        long base = log.rollSegment();

        assertEquals(log.lastLsn() + 1, base);
        assertEquals(2, log.segmentBases().size());
        log.awaitDurable(log.append(LogHarness.single(5, 6)));
        log.close();
        assertEquals(3, LogHarness.recoverOnly(disk, 0).units().size());
    }

    @Test
    void asynchronousAppendsAreNotDurableUntilForced() throws Exception {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, ASYNC, 0);
        SegmentedLog log = opened.log();
        long durableAtStart = log.durableLsn();

        long lsn = log.append(LogHarness.tuples(1, 2));
        log.awaitDurable(lsn);
        waitForWrites(disk, 3);

        assertEquals(durableAtStart, log.durableLsn());
        log.force();
        assertEquals(lsn, log.durableLsn());
        log.close();
    }

    @Test
    void synchronousAppendsAreDurableWhenAwaited() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);

        long lsn = opened.log().append(LogHarness.single(1, 2));
        opened.log().awaitDurable(lsn);

        assertEquals(lsn, opened.log().durableLsn());
        opened.log().close();
    }

    @Test
    void aTransactionLargerThanTheBufferSpansSeveralFlushes() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        RecordBatch batch = LogHarness.tuples(0, 100_000);

        long lsn = opened.log().append(batch);
        opened.log().awaitDurable(lsn);
        opened.log().close();

        List<Unit> units = LogHarness.recoverOnly(disk, 0).units();
        assertEquals(2, units.size());
        assertEquals(100_000, units.get(1).records().size());
    }

    @Test
    void aFailedForcePoisonsTheLogAndLaterWritesAreRefused() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened opened = LogHarness.open(disk, SYNC, 0);
        SegmentedLog log = opened.log();
        disk.failForcesOn(".nlog");

        long lsn = log.append(LogHarness.single(1, 2));
        assertThrows(IndeterminateOutcomeException.class, () -> log.awaitDurable(lsn));

        assertThrows(IndeterminateOutcomeException.class, () -> log.append(LogHarness.single(3, 4)));
        assertThrows(IndeterminateOutcomeException.class, log::force);
        assertThrows(IndeterminateOutcomeException.class, log::close);
    }

    @Test
    void aClosedLogRefusesAppends() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        SegmentedLog log = LogHarness.open(disk, SYNC, 0).log();
        log.close();

        assertThrows(IllegalStateException.class, () -> log.append(LogHarness.single(1, 2)));
        log.close();
    }

    @Test
    void recoveryReopensTheTailSegmentAndKeepsAppendingInIt() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        Opened first = LogHarness.open(disk, SYNC, 0);
        first.log().awaitDurable(first.log().append(LogHarness.tuples(1, 2)));
        first.log().close();
        long segmentsBefore = disk.list().stream().filter(name -> name.endsWith(".nlog")).count();

        Opened second = LogHarness.open(disk, SYNC, 0);
        second.log().awaitDurable(second.log().append(LogHarness.tuples(7, 2)));
        second.log().close();

        long segmentsAfter = disk.list().stream().filter(name -> name.endsWith(".nlog")).count();
        assertEquals(segmentsBefore, segmentsAfter);
        CollectingSink recovered = LogHarness.recoverOnly(disk, 0);
        assertEquals(4, recovered.units().size());
    }

    private static void waitForWrites(SimulatedDisk disk, int writes) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            long seen = disk.events().stream()
                    .filter(event -> event.kind() == SimulatedDisk.Kind.WRITE && event.file().endsWith(".nlog"))
                    .count();
            if (seen >= writes) {
                return;
            }
            Thread.sleep(5);
        }
    }
}
