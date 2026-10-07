package io.nodusdb.log;

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
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrashExplorationTest {

    private static final int OPERATIONS = 30;
    private static final long[] MASKS = {SimulatedDisk.NO_PENDING, SimulatedDisk.ALL_PENDING, 0x5555_5555L, 0xAAAA_AAAAL};

    private record Acknowledged(int event, int units) {
    }

    private record Workload(SimulatedDisk disk, List<Unit> expected, List<Acknowledged> acknowledged) {

        int acknowledgedAt(int eventsKept) {
            int units = 0;
            for (Acknowledged ack : acknowledged) {
                if (ack.event() <= eventsKept) {
                    units = Math.max(units, ack.units());
                }
            }
            return units;
        }
    }

    @Test
    void everyCrashPointOfASynchronousWorkloadRecoversAnAcknowledgedPrefix() throws IOException {
        Workload workload = runSynchronousWorkload();

        explore(workload, true);
    }

    @Test
    void everyCrashPointOfAnAsynchronousWorkloadRecoversAPrefixAndKeepsForcedUnits() throws IOException {
        Workload workload = runAsynchronousWorkload();

        explore(workload, false);
    }

    private Workload runSynchronousWorkload() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        LogConfig config = LogConfig.withSyncMode(SyncMode.SYNC).withSegmentBytes(2048);
        Opened opened = LogHarness.open(disk, config, 0);
        List<Unit> expected = new ArrayList<>();
        List<Acknowledged> acknowledged = new ArrayList<>();
        expected.add(new Unit(2, 0, List.of("EPOCH 1 0")));
        acknowledged.add(new Acknowledged(disk.eventCount(), 1));
        for (int i = 0; i < OPERATIONS; i++) {
            RecordBatch batch = batchFor(i);
            List<String> records = LogHarness.describeAll(batch);
            long commitLsn = opened.log().append(batch);
            opened.log().awaitDurable(commitLsn);
            expected.add(new Unit(commitLsn, 0, records));
            acknowledged.add(new Acknowledged(disk.eventCount(), expected.size()));
        }
        opened.log().close();
        return new Workload(disk, expected, acknowledged);
    }

    private Workload runAsynchronousWorkload() throws IOException {
        SimulatedDisk disk = new SimulatedDisk();
        LogConfig config = LogConfig.withSyncMode(SyncMode.ASYNC).withSegmentBytes(2048);
        Opened opened = LogHarness.open(disk, config, 0);
        List<Unit> expected = new ArrayList<>();
        List<Acknowledged> acknowledged = new ArrayList<>();
        expected.add(new Unit(2, 0, List.of("EPOCH 1 0")));
        acknowledged.add(new Acknowledged(disk.eventCount(), 1));
        for (int i = 0; i < OPERATIONS; i++) {
            RecordBatch batch = batchFor(i);
            List<String> records = LogHarness.describeAll(batch);
            long commitLsn = opened.log().append(batch);
            expected.add(new Unit(commitLsn, 0, records));
            if (i % 7 == 6) {
                opened.log().force();
                acknowledged.add(new Acknowledged(disk.eventCount(), expected.size()));
            }
        }
        opened.log().close();
        acknowledged.add(new Acknowledged(disk.eventCount(), expected.size()));
        return new Workload(disk, expected, acknowledged);
    }

    private static RecordBatch batchFor(int index) {
        return index % 3 == 0 ? LogHarness.single(index, index + 1000) : LogHarness.tuples(index * 10, 1 + index % 5);
    }

    private void explore(Workload workload, boolean exhaustiveTears) throws IOException {
        SimulatedDisk disk = workload.disk();
        int events = disk.eventCount();
        Random random = new Random(11L);
        int images = 0;
        for (int kept = 0; kept <= events; kept++) {
            for (long mask : masksFor(random)) {
                verify(workload, disk.crashImage(kept, mask), kept, "kept " + kept + " mask " + mask);
                images++;
            }
            if (kept > 0) {
                int written = disk.sizeOfWrite(kept - 1);
                int stride = exhaustiveTears && written <= 64 ? 1 : 13;
                for (int prefix = 0; prefix < written; prefix += stride) {
                    verify(workload, disk.crashImage(kept, SimulatedDisk.NO_PENDING, prefix), kept,
                            "kept " + kept + " torn at " + prefix);
                    verify(workload, disk.crashImage(kept, SimulatedDisk.ALL_PENDING, prefix), kept,
                            "kept " + kept + " all pending torn at " + prefix);
                    images += 2;
                }
            }
        }
        assertTrue(images > 300, "explored only " + images + " crash images");
    }

    private static long[] masksFor(Random random) {
        long[] masks = new long[MASKS.length + 2];
        System.arraycopy(MASKS, 0, masks, 0, MASKS.length);
        masks[MASKS.length] = random.nextLong();
        masks[MASKS.length + 1] = random.nextLong();
        return masks;
    }

    private void verify(Workload workload, SimulatedDisk image, int kept, String label) throws IOException {
        Opened reopened = LogHarness.open(image, LogConfig.withSyncMode(SyncMode.SYNC), 0);
        List<Unit> recovered = reopened.recovered().units();
        List<Unit> expected = workload.expected();
        assertTrue(recovered.size() <= expected.size(), label + ": more units than were ever written");
        for (int i = 0; i < recovered.size(); i++) {
            assertEquals(expected.get(i).records(), recovered.get(i).records(), label + ": unit " + i);
            assertEquals(expected.get(i).commitLsn(), recovered.get(i).commitLsn(), label + ": lsn of unit " + i);
        }
        assertTrue(recovered.size() >= workload.acknowledgedAt(kept),
                label + ": recovered " + recovered.size() + " units but " + workload.acknowledgedAt(kept)
                        + " were acknowledged");
        long next = reopened.log().lastLsn() + 1;
        long lsn = reopened.log().append(LogHarness.single(1, 2));
        reopened.log().awaitDurable(lsn);
        assertTrue(lsn >= next, label + ": LSNs went backwards");
        reopened.log().close();
        CollectingSink again = LogHarness.recoverOnly(image, 0);
        assertEquals(recovered.size() + 2, again.units().size(), label + ": reopen lost or repeated units");
    }
}
