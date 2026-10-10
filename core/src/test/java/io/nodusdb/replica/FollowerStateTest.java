package io.nodusdb.replica;

import io.nodusdb.replica.FollowerState.Phase;
import io.nodusdb.ship.EventLog;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FollowerStateTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private final FollowerState state = new FollowerState();

    private static List<String> messages(FollowerState state) {
        return state.recentEvents().stream().map(EventLog.Event::message).toList();
    }

    @Test
    void aNewFollowerIsBootstrappingAtPositionZeroAndHasNeverConfirmedTheHead() {
        FollowerState.Snapshot snapshot = state.snapshot(5 * SECOND);

        assertEquals(Phase.BOOTSTRAPPING, snapshot.phase());
        assertEquals(0, snapshot.appliedLsn());
        assertEquals(0, snapshot.chainSeq());
        assertEquals(0, snapshot.epoch());
        assertEquals(FollowerState.NEVER_CONFIRMED, snapshot.nanosSinceConfirmed());
        assertFalse(state.terminal());
    }

    @Test
    void aBootstrapMovesToCatchingUpAndRecordsTheAnchor() {
        state.bootstrapped(40, 55, 7, 2);

        FollowerState.Snapshot snapshot = state.snapshot(2 * SECOND);

        assertEquals(Phase.CATCHING_UP, snapshot.phase());
        assertEquals(40, snapshot.snapshotLsn());
        assertEquals(55, snapshot.appliedLsn());
        assertEquals(7, snapshot.chainSeq());
        assertEquals(2, snapshot.epoch());
        assertEquals(1, snapshot.bootstraps());
    }

    @Test
    void anAppliedObjectAdvancesThePositionAndTheCounters() {
        state.bootstrapped(40, 40, 3, 1);

        state.applied(4, 60, 1, 1_000);
        state.applied(5, 75, 2, 2_000);

        FollowerState.Snapshot snapshot = state.snapshot(SECOND);
        assertEquals(75, snapshot.appliedLsn());
        assertEquals(5, snapshot.chainSeq());
        assertEquals(2, snapshot.epoch());
        assertEquals(2, snapshot.objectsApplied());
        assertEquals(3_000, snapshot.bytesApplied());
        assertEquals(Phase.CATCHING_UP, snapshot.phase());
    }

    @Test
    void findingNothingNewConfirmsTheHeadAndMakesTheFollowerCurrent() {
        state.bootstrapped(40, 40, 3, 1);

        state.current(3 * SECOND);

        FollowerState.Snapshot snapshot = state.snapshot(10 * SECOND);
        assertEquals(Phase.CURRENT, snapshot.phase());
        assertEquals(7 * SECOND, snapshot.nanosSinceConfirmed());
    }

    @Test
    void aNewObjectAfterCurrentMovesBackToCatchingUp() {
        state.bootstrapped(40, 40, 3, 1);
        state.current(2 * SECOND);

        state.applied(4, 50, 1, 100);

        assertEquals(Phase.CATCHING_UP, state.phase());
    }

    @Test
    void aStoreFailureAfterBootstrapMakesTheFollowerLaggingAndKeepsServingItsPosition() {
        state.bootstrapped(40, 40, 3, 1);
        state.current(2 * SECOND);

        state.lagging("the store timed out");
        state.lagging("the store timed out again");

        FollowerState.Snapshot snapshot = state.snapshot(3 * SECOND);
        assertEquals(Phase.LAGGING, snapshot.phase());
        assertEquals(2, snapshot.consecutiveFailures());
        assertEquals("the store timed out again", snapshot.lastError());
        assertEquals(40, snapshot.appliedLsn());
        assertEquals(1, messages(state).stream().filter(text -> text.contains("falling behind")).count());
    }

    @Test
    void recoveryFromLaggingClearsTheFailureStateAndLogsIt() {
        state.bootstrapped(40, 40, 3, 1);
        state.lagging("the store timed out");

        state.current(4 * SECOND);

        FollowerState.Snapshot snapshot = state.snapshot(4 * SECOND);
        assertEquals(Phase.CURRENT, snapshot.phase());
        assertEquals(0, snapshot.consecutiveFailures());
        assertEquals("", snapshot.lastError());
        assertTrue(messages(state).stream().anyMatch(text -> text.contains("recovered")));
    }

    @Test
    void aFailureDuringBootstrapKeepsTheFollowerBootstrappingAndCountsIt() {
        state.lagging("could not list the snapshots");

        FollowerState.Snapshot snapshot = state.snapshot(SECOND);

        assertEquals(Phase.BOOTSTRAPPING, snapshot.phase());
        assertEquals(1, snapshot.consecutiveFailures());
        assertEquals("could not list the snapshots", snapshot.lastError());
    }

    @Test
    void waitingForTheFirstSnapshotIsLoggedOnce() {
        state.waitingForSnapshot();
        state.waitingForSnapshot();

        assertEquals(1, state.recentEvents().size());
        assertEquals(Phase.BOOTSTRAPPING, state.phase());
    }

    @Test
    void stalledIsTerminalAndNothingMovesItBack() {
        state.bootstrapped(40, 40, 3, 1);

        state.stalled("object 9 does not extend object 8");
        state.current(5 * SECOND);
        state.applied(10, 90, 1, 10);
        state.lagging("late failure");
        state.bootstrapped(80, 80, 20, 3);

        FollowerState.Snapshot snapshot = state.snapshot(6 * SECOND);
        assertEquals(Phase.STALLED, snapshot.phase());
        assertEquals("object 9 does not extend object 8", snapshot.stallReason());
        assertEquals(40, snapshot.appliedLsn());
        assertTrue(state.terminal());
    }

    @Test
    void aStallIsLoggedOnceWithItsReason() {
        state.stalled("rollback");
        state.stalled("rollback again");

        assertEquals(List.of("stalled: rollback"), messages(state));
    }

    @Test
    void closedBeatsStalledAndEverythingElse() {
        state.bootstrapped(40, 40, 3, 1);
        state.stalled("rollback");

        state.closed();
        state.stalled("again");

        assertEquals(Phase.CLOSED, state.phase());
        assertTrue(state.terminal());
    }

    @Test
    void thePositionNeverMovesBackwards() {
        state.bootstrapped(40, 60, 8, 2);

        assertThrows(IllegalArgumentException.class, () -> state.applied(7, 70, 2, 1));
        assertThrows(IllegalArgumentException.class, () -> state.applied(9, 50, 2, 1));
        assertThrows(IllegalArgumentException.class, () -> state.applied(9, 70, 1, 1));
        state.applied(9, 70, 2, 1);
        assertEquals(70, state.snapshot(SECOND).appliedLsn());
    }

    @Test
    void aStopRequestEndsAPauseEarly() throws Exception {
        CountDownLatch paused = new CountDownLatch(1);
        long[] waited = new long[1];
        Thread waiter = new Thread(() -> {
            long begin = System.nanoTime();
            paused.countDown();
            state.pause(TimeUnit.SECONDS.toNanos(30));
            waited[0] = System.nanoTime() - begin;
        });
        waiter.start();
        paused.await();

        state.requestStop();
        waiter.join(10_000);

        assertFalse(waiter.isAlive());
        assertTrue(waited[0] < TimeUnit.SECONDS.toNanos(10));
        assertTrue(state.stopRequested());
    }

    @Test
    void everyChangeAdvancesTheVersionAndAWaiterReturnsWithTheNewOne() throws Exception {
        long before = state.version();
        long[] seen = new long[1];
        CountDownLatch waiting = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            waiting.countDown();
            seen[0] = state.awaitChange(before, TimeUnit.SECONDS.toNanos(30));
        });
        waiter.start();
        waiting.await();

        state.bootstrapped(40, 40, 3, 1);
        waiter.join(10_000);

        assertFalse(waiter.isAlive());
        assertTrue(seen[0] > before);
        assertEquals(state.version(), seen[0]);
    }

    @Test
    void awaitingAnOutdatedVersionReturnsAtOnce() {
        state.bootstrapped(40, 40, 3, 1);
        long before = System.nanoTime();

        long version = state.awaitChange(0, TimeUnit.SECONDS.toNanos(30));

        assertEquals(state.version(), version);
        assertTrue(System.nanoTime() - before < TimeUnit.SECONDS.toNanos(5));
    }

    @Test
    void awaitingTheCurrentVersionTimesOutWithoutAChange() {
        long begin = System.nanoTime();

        long version = state.awaitChange(state.version(), TimeUnit.MILLISECONDS.toNanos(60));

        assertEquals(state.version(), version);
        assertTrue(System.nanoTime() - begin >= TimeUnit.MILLISECONDS.toNanos(50));
    }

    @Test
    void aPauseThatIsNotInterruptedLastsItsWholeDuration() {
        long begin = System.nanoTime();

        state.pause(TimeUnit.MILLISECONDS.toNanos(60));

        assertTrue(System.nanoTime() - begin >= TimeUnit.MILLISECONDS.toNanos(50));
    }
}
