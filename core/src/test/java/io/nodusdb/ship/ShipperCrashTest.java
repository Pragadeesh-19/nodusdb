package io.nodusdb.ship;

import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.FaultyObjectStore.Call;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.FaultyObjectStore.SimulatedCrash;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.ship.ShipperCore.Cadence;
import io.nodusdb.ship.ShipperRig.Session;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShipperCrashTest {

    private static final String PROBE_PREFIX = "_nodus/probe/";
    private static final int TRANSACTIONS = 12;
    private static final int BACKOFF_STEPS = 30;

    private record Played(int interruptions, int storeCalls, List<Call> calls, ChainAudit.Result audit) {
    }

    private static void closeQuietly(Session session) {
        if (session == null) {
            return;
        }
        try {
            session.feed().close();
        } catch (IOException ignored) {
            return;
        }
    }

    private static Played play(ShipperRig rig) {
        int interruptions = 0;
        int nonce = 0;
        int done = 0;
        Session session = null;
        while (true) {
            try {
                if (session == null) {
                    session = rig.open(++nonce);
                }
                while (done < TRANSACTIONS) {
                    rig.append(1 + done % 4);
                    done++;
                    if (done == 6) {
                        session.state().offerReference(rig.referenceAt(rig.log.lastLsn()));
                    }
                    if (done % 2 == 0) {
                        rig.runPastBackoffs(session, BACKOFF_STEPS);
                    }
                }
                ShipperCore.Next last = rig.runPastBackoffs(session, BACKOFF_STEPS);
                assertEquals(Cadence.IDLE, last.cadence(), session.state().snapshot().lastError());
                assertEquals(rig.log.lastLsn(), session.state().shippedLsn());
                ChainAudit.Result audit = ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
                return new Played(interruptions, rig.store.callCount(), rig.store.calls(), audit);
            } catch (SimulatedCrash | ObjectStoreException interruption) {
                interruptions++;
                closeQuietly(session);
                session = null;
            }
        }
    }

    private static Played cleanRun() {
        try (ShipperRig rig = new ShipperRig()) {
            return play(rig);
        }
    }

    @Test
    void theScriptedWorkloadIsDeterministic() {
        Played first = cleanRun();
        Played second = cleanRun();

        assertEquals(0, first.interruptions());
        assertEquals(first.storeCalls(), second.storeCalls());
        assertEquals(shapeOf(first.calls()), shapeOf(second.calls()));
        assertTrue(first.storeCalls() > 20, "only " + first.storeCalls() + " store calls are worth sweeping");
    }

    private static List<String> shapeOf(List<Call> calls) {
        return calls.stream().map(call -> call.operation() + " " + (isProbe(call.key()) ? "probe" : call.key()))
                .toList();
    }

    private static boolean isProbe(String key) {
        return key.startsWith(PROBE_PREFIX);
    }

    @ParameterizedTest
    @EnumSource(value = Fault.class, names = {"CRASH_BEFORE", "CRASH_AFTER", "FAIL_BEFORE", "FAIL_AFTER", "FATAL",
            "EXPIRED"})
    void aFaultAtEveryStoreCallNeverLosesDuplicatesOrReordersARecord(Fault fault) {
        Played clean = cleanRun();

        for (int ordinal = 1; ordinal <= clean.storeCalls(); ordinal++) {
            try (ShipperRig rig = new ShipperRig()) {
                rig.store.failCall(ordinal, fault);

                Played played = play(rig);

                String where = fault + " at call " + ordinal + " (" + clean.calls().get(ordinal - 1) + ")";
                boolean crashFault = fault == Fault.CRASH_BEFORE || fault == Fault.CRASH_AFTER;
                if (crashFault) {
                    assertEquals(1, played.interruptions(), where);
                }
                assertTrue(played.storeCalls() >= ordinal, where);
                assertStrictlyIncreasing(played.audit().epochs(), where);
            }
        }
    }

    @Test
    void aConflictAtEveryConditionalWriteIsAbsorbed() {
        Played clean = cleanRun();
        int swept = 0;

        for (Call call : clean.calls()) {
            if (call.operation() != Operation.PUT_IF_ABSENT || isProbe(call.key())) {
                continue;
            }
            try (ShipperRig rig = new ShipperRig()) {
                rig.store.failCall(call.ordinal(), Fault.CONFLICT);

                Played played = play(rig);

                assertStrictlyIncreasing(played.audit().epochs(), call.toString());
                swept++;
            }
        }

        assertTrue(swept >= 5, "only " + swept + " conditional writes were swept");
    }

    @Test
    void twoFaultsInARowAtEveryCallStillConverge() {
        Played clean = cleanRun();

        for (int ordinal = 1; ordinal < clean.storeCalls(); ordinal++) {
            try (ShipperRig rig = new ShipperRig()) {
                rig.store.failCall(ordinal, Fault.CRASH_AFTER);
                rig.store.failCall(ordinal + 1, Fault.FAIL_AFTER);

                Played played = play(rig);

                assertStrictlyIncreasing(played.audit().epochs(), "faults at calls " + ordinal + " and "
                        + (ordinal + 1));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946})
    void randomFaultsAndRestartsNeverCorruptTheChain(long seed) {
        Random random = new Random(seed);
        try (ShipperRig rig = new ShipperRig(new MemoryObjectStore(), ShipSettings.defaults(), SyncMode.SYNC)) {
            rig.store.randomFaults(seed, 0.08, 0.08, 0.08).exemptFromRandomConflicts(PROBE_PREFIX);
            Session session = null;
            int nonce = 0;
            int appended = 0;
            int steps = 0;
            while (appended < 60 || steps < 400) {
                steps++;
                try {
                    if (session == null) {
                        session = rig.open(++nonce);
                    }
                    if (appended < 60 && random.nextInt(3) == 0) {
                        rig.append(1 + random.nextInt(5));
                        appended++;
                    }
                    session.core().step();
                    if (random.nextInt(20) == 0) {
                        closeQuietly(session);
                        session = null;
                    }
                } catch (SimulatedCrash | ObjectStoreException interruption) {
                    closeQuietly(session);
                    session = null;
                }
                assertTrue(steps < 5_000, "seed=" + seed + " did not finish");
            }

            rig.store.stopRandomFaults();
            if (session == null) {
                session = rig.open(++nonce);
            }
            ShipperCore.Next last = rig.runPastBackoffs(session, BACKOFF_STEPS);

            assertEquals(Cadence.IDLE, last.cadence(), "seed=" + seed + " " + session.state().snapshot().lastError());
            assertEquals(rig.log.lastLsn(), session.state().shippedLsn(), "seed=" + seed);
            ChainAudit.Result audit = ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
            assertStrictlyIncreasing(audit.epochs(), "seed=" + seed);
        }
    }

    private static void assertStrictlyIncreasing(List<Long> epochs, String context) {
        for (int i = 1; i < epochs.size(); i++) {
            assertTrue(epochs.get(i) > epochs.get(i - 1), context + ": epochs " + epochs);
        }
    }
}
