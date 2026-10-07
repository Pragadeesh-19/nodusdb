package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.ShipTimeoutException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.ship.ShipState.BacklogEvent;
import io.nodusdb.ship.ShipState.Phase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShipStateTest {

    private static final long CAP = 1_000;
    private static final long SECOND = 1_000_000_000L;

    private static ShipState state() {
        return new ShipState(3, 100, 7, CAP);
    }

    @Test
    void theStateStartsWhereTheChainWasLeft() {
        ShipState state = state();

        ShipState.Snapshot snapshot = state.snapshot();

        assertEquals(Phase.STARTING, snapshot.phase());
        assertEquals(3, snapshot.epoch());
        assertEquals(100, snapshot.shippedLsn());
        assertEquals(7, snapshot.chainSeq());
        assertTrue(state.configured());
        assertEquals(100, state.shippedLsn());
    }

    @Test
    void waitingForAnAlreadyShippedLsnReturnsAtOnceAndLeavesNoWaiter() {
        ShipState state = state();

        state.awaitShipped(3, 100, SECOND);
        state.awaitShipped(3, 50, SECOND);

        assertFalse(state.hasWaiters());
    }

    @Test
    void aWaiterIsReleasedWhenTheLsnIsShipped() throws Exception {
        ShipState state = state();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch waiting = new CountDownLatch(1);
            Future<Long> waiter = pool.submit(() -> {
                waiting.countDown();
                long started = System.nanoTime();
                state.awaitShipped(3, 150, 30 * SECOND);
                return System.nanoTime() - started;
            });
            assertTrue(waiting.await(10, TimeUnit.SECONDS));
            waitUntil(state::hasWaiters);

            state.shipped(120, 8, 10, System.nanoTime());
            assertTrue(state.hasWaiters());
            state.shipped(150, 9, 10, System.nanoTime());

            assertTrue(waiter.get(10, TimeUnit.SECONDS) < 10 * SECOND);
            assertFalse(state.hasWaiters());
        } finally {
            pool.shutdownNow();
        }
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 10 * SECOND;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertTrue(condition.getAsBoolean(), "the condition never became true");
    }

    @Test
    void aTimeoutCarriesTheTokenAndNamesThePhase() {
        ShipState state = state();
        state.active();

        ShipTimeoutException timeout = assertThrows(ShipTimeoutException.class,
                () -> state.awaitShipped(3, 200, 20_000_000L));

        assertEquals(3, timeout.epoch());
        assertEquals(200, timeout.lsn());
        assertTrue(timeout.getMessage().contains("ACTIVE"), timeout.getMessage());
        assertFalse(state.hasWaiters());
    }

    @Test
    void aZeroTimeoutFailsAtOnceWhenTheLsnIsNotShipped() {
        assertThrows(ShipTimeoutException.class, () -> state().awaitShipped(3, 101, 0));
    }

    @Test
    void beingFencedWhileWaitingIsReportedAsFenced() throws Exception {
        ShipState state = state();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> waiter = pool.submit(() -> state.awaitShipped(3, 150, 30 * SECOND));
            waitUntil(state::hasWaiters);

            state.fenced("epoch 4 exists");

            Exception failure = assertThrows(ExecutionException.class,
                    () -> waiter.get(10, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof WriterFencedException, String.valueOf(failure.getCause()));
            assertEquals("epoch 4 exists", failure.getCause().getMessage());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aFencedStateRefusesEveryLaterWaitImmediately() {
        ShipState state = state();
        state.fenced("a higher epoch exists");

        assertThrows(WriterFencedException.class, () -> state.awaitShipped(3, 101, 30 * SECOND));
        state.shipped(500, 20, 1, 1);
        state.awaitShipped(3, 400, SECOND);
    }

    @Test
    void aFailedShipperFailsWaitsFastWithItsReason() {
        ShipState state = state();
        state.failed("access denied");
        long started = System.nanoTime();

        ShipTimeoutException failure = assertThrows(ShipTimeoutException.class,
                () -> state.awaitShipped(3, 101, 30 * SECOND));

        assertTrue(System.nanoTime() - started < 5 * SECOND);
        assertTrue(failure.getMessage().contains("access denied"), failure.getMessage());
    }

    @Test
    void aClosedStateFailsWaitsFast() {
        ShipState state = state();
        state.closed();

        ShipTimeoutException failure = assertThrows(ShipTimeoutException.class,
                () -> state.awaitShipped(3, 101, 30 * SECOND));

        assertTrue(failure.getMessage().contains("closed"), failure.getMessage());
    }

    @Test
    void anInterruptedWaitReportsATimeoutAndKeepsTheInterruptFlag() throws Exception {
        ShipState state = state();
        AtomicBoolean flagKept = new AtomicBoolean();
        AtomicBoolean reported = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            try {
                state.awaitShipped(3, 150, 30 * SECOND);
            } catch (ShipTimeoutException expected) {
                reported.set(true);
                flagKept.set(Thread.currentThread().isInterrupted());
            }
        });
        waiter.start();
        waitUntil(state::hasWaiters);

        waiter.interrupt();
        waiter.join(10_000);

        assertTrue(reported.get());
        assertTrue(flagKept.get());
    }

    @Test
    void theShippedLsnNeverMovesBackwards() {
        ShipState state = state();

        state.shipped(150, 8, 10, 1);
        state.shipped(120, 9, 10, 2);

        assertEquals(150, state.shippedLsn());
        assertEquals(9, state.snapshot().chainSeq());
        assertEquals(2, state.snapshot().objectsShipped());
        assertEquals(20, state.snapshot().bytesShipped());
    }

    @Test
    void thePhasesFollowTheLifecycleAndTerminalPhasesAreSticky() {
        ShipState state = state();

        state.active();
        assertEquals(Phase.ACTIVE, state.phase());
        state.retrying("timeout");
        assertEquals(Phase.RETRYING, state.phase());
        assertEquals(1, state.snapshot().consecutiveFailures());
        assertEquals("timeout", state.snapshot().lastError());
        state.active();
        assertEquals(0, state.snapshot().consecutiveFailures());
        assertEquals("", state.snapshot().lastError());
        state.failed("denied");
        assertEquals(Phase.FAILED, state.phase());
        state.fenced("newer writer");
        assertEquals(Phase.FENCED, state.phase());
        assertTrue(state.terminal());
        state.active();
        state.retrying("late");
        state.failed("late");
        assertEquals(Phase.FENCED, state.phase());
        state.closed();
        assertEquals(Phase.CLOSED, state.phase());
        state.fenced("after close");
        assertEquals(Phase.CLOSED, state.phase());
    }

    @Test
    void writesAreRefusedFromTheCapUntilTheBacklogFallsBelowNinetyPercent() {
        ShipState state = state();

        assertEquals(BacklogEvent.NONE, state.backlog(100));
        assertNull(state.refusal());
        assertEquals(BacklogEvent.WARNING, state.backlog(500));
        assertNull(state.refusal());
        assertEquals(BacklogEvent.NONE, state.backlog(900));
        assertEquals(BacklogEvent.REFUSING, state.backlog(1_000));
        assertNotNull(state.refusal());
        assertEquals(BacklogEvent.NONE, state.backlog(1_500));
        assertEquals(BacklogEvent.NONE, state.backlog(950));
        assertNotNull(state.refusal());
        assertEquals(BacklogEvent.RESUMED, state.backlog(899));
        assertNull(state.refusal());
    }

    @Test
    void theWarningFiresOncePerCrossingAndRearmsWellBelowIt() {
        ShipState state = state();

        assertEquals(BacklogEvent.WARNING, state.backlog(500));
        assertEquals(BacklogEvent.NONE, state.backlog(600));
        assertEquals(BacklogEvent.NONE, state.backlog(400));
        assertEquals(BacklogEvent.NONE, state.backlog(510));
        assertEquals(BacklogEvent.NONE, state.backlog(100));
        assertEquals(BacklogEvent.WARNING, state.backlog(520));
    }

    @Test
    void aBacklogThatJumpsStraightPastTheCapRefusesAndWarnsAsOne() {
        ShipState state = state();

        assertEquals(BacklogEvent.REFUSING, state.backlog(5_000));

        assertEquals(2, state.drainEvents().size());
    }

    @Test
    void eventsAreDrainedOnceAndBounded() {
        ShipState state = state();
        for (int i = 0; i < 100; i++) {
            state.failed("failure " + i);
            state.active();
        }

        List<String> events = state.drainEvents();

        assertEquals(64, events.size());
        assertEquals("shipping failed: failure 99", events.get(63));
        assertEquals(List.of(), state.drainEvents());
    }

    @Test
    void theRunnerWaitsForWorkUntilItIsWoken() throws Exception {
        ShipState state = state();
        long started = System.nanoTime();
        state.awaitWork(30_000_000L);
        assertTrue(System.nanoTime() - started >= 20_000_000L);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Long> idle = pool.submit(() -> {
                long begin = System.nanoTime();
                state.awaitWork(30 * SECOND);
                return System.nanoTime() - begin;
            });
            Thread.sleep(50);
            state.wake();
            assertTrue(idle.get(10, TimeUnit.SECONDS) < 10 * SECOND);
        } finally {
            pool.shutdownNow();
        }
        state.awaitWork(0);
        state.awaitWork(-5);
    }

    @Test
    void aWakeBeforeTheRunnerWaitsIsNotLostAndIsConsumedOnce() {
        ShipState state = state();
        state.wake();

        long started = System.nanoTime();
        state.awaitWork(30 * SECOND);
        assertTrue(System.nanoTime() - started < 10 * SECOND);

        started = System.nanoTime();
        state.awaitWork(30_000_000L);
        assertTrue(System.nanoTime() - started >= 20_000_000L, "the second wait must not return at once");
    }

    @Test
    void aWaiterThatArrivesWhileTheRunnerIsBusyWakesItsNextWaitOnlyOnce() throws Exception {
        ShipState state = state();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Throwable> waiter = pool.submit(() -> {
                try {
                    state.awaitShipped(3, 150, 30 * SECOND);
                    return null;
                } catch (Throwable thrown) {
                    return thrown;
                }
            });
            waitUntil(state::hasWaiters);

            long started = System.nanoTime();
            state.awaitWork(30 * SECOND);
            assertTrue(System.nanoTime() - started < 10 * SECOND);

            started = System.nanoTime();
            state.awaitWork(30_000_000L);
            assertTrue(System.nanoTime() - started >= 20_000_000L, "a lingering waiter must not spin the runner");

            state.shipped(150, 8, 1, 0);
            assertNull(waiter.get(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aPauseLastsItsFullLengthDespiteWorkSignals() throws Exception {
        ShipState state = state();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            state.wake();
            long started = System.nanoTime();
            Future<?> pausing = pool.submit(() -> state.pause(150_000_000L));
            Thread.sleep(30);
            state.wake();
            state.wake();
            pausing.get(10, TimeUnit.SECONDS);

            assertTrue(System.nanoTime() - started >= 140_000_000L);
        } finally {
            pool.shutdownNow();
        }
    }

    private static ChainBody.SnapshotRef reference(long lsn) {
        return new ChainBody.SnapshotRef(ChainLayout.snapshotKey(lsn), ChainHash.sha256(new byte[]{(byte) lsn}), lsn);
    }

    @Test
    void anOfferedReferenceIsTakenOnceAndTheNewestOfferWins() {
        ShipState state = state();
        assertNull(state.takeReference());

        state.offerReference(reference(10));
        state.offerReference(reference(20));
        state.offerReference(reference(15));

        assertEquals(20, state.takeReference().lsn());
        assertNull(state.takeReference());
        state.offerReference(reference(5));
        assertEquals(5, state.takeReference().lsn());
    }

    @Test
    void aReferenceWithTheSameLsnReplacesTheOfferedOne() {
        ShipState state = state();
        ChainBody.SnapshotRef first = reference(10);
        ChainBody.SnapshotRef second = new ChainBody.SnapshotRef(first.path(), ChainHash.sha256(new byte[]{1}), 10);

        state.offerReference(first);
        state.offerReference(second);

        assertEquals(second, state.takeReference());
    }

    @Test
    void offeringAReferenceWakesTheRunnerOnce() {
        ShipState state = state();

        state.offerReference(reference(10));
        long started = System.nanoTime();
        state.awaitWork(30 * SECOND);
        assertTrue(System.nanoTime() - started < 10 * SECOND);

        started = System.nanoTime();
        state.awaitWork(30_000_000L);
        assertTrue(System.nanoTime() - started >= 20_000_000L);
    }

    @Test
    void referenceProgressAndFailuresAreReported() {
        ShipState state = state();
        ShipState.ReferenceStatus initial = state.snapshot().references();
        assertEquals(0, initial.seq());
        assertEquals(-1, initial.lsn());
        assertEquals(0, initial.failures());
        assertEquals("", initial.lastError());

        state.referenceFailed("first");
        state.referenceFailed("second");
        ShipState.ReferenceStatus failing = state.snapshot().references();
        assertEquals(2, failing.failures());
        assertEquals("second", failing.lastError());

        state.referenceCommitted(9, 77, 123);
        ShipState.ReferenceStatus committed = state.snapshot().references();
        assertEquals(9, committed.seq());
        assertEquals(77, committed.lsn());
        assertEquals(123, committed.committedNanos());
        assertEquals(2, committed.failures());
        assertEquals("", committed.lastError());
    }

    @Test
    void aStopRequestEndsEveryWaitAtOnceAndStaysRequested() {
        ShipState state = state();
        assertFalse(state.stopRequested());

        state.requestStop();

        assertTrue(state.stopRequested());
        long started = System.nanoTime();
        state.awaitWork(30 * SECOND);
        state.pause(30 * SECOND);
        assertTrue(System.nanoTime() - started < 10 * SECOND);
        assertTrue(state.stopRequested());
    }

    @Test
    void aStopRequestWakesARunnerThatIsPausedOrIdle() throws Exception {
        ShipState state = state();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Long> paused = pool.submit(() -> {
                long begin = System.nanoTime();
                state.pause(30 * SECOND);
                return System.nanoTime() - begin;
            });
            Future<Long> idle = pool.submit(() -> {
                long begin = System.nanoTime();
                state.awaitWork(30 * SECOND);
                return System.nanoTime() - begin;
            });
            Thread.sleep(50);

            state.requestStop();

            assertTrue(paused.get(10, TimeUnit.SECONDS) < 10 * SECOND);
            assertTrue(idle.get(10, TimeUnit.SECONDS) < 10 * SECOND);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aPauseEndsWhenTheStateIsClosedAndNeverStartsAfterwards() throws Exception {
        ShipState state = state();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Long> pausing = pool.submit(() -> {
                long begin = System.nanoTime();
                state.pause(30 * SECOND);
                return System.nanoTime() - begin;
            });
            Thread.sleep(50);
            state.closed();

            assertTrue(pausing.get(10, TimeUnit.SECONDS) < 10 * SECOND);
            long started = System.nanoTime();
            state.pause(30 * SECOND);
            assertTrue(System.nanoTime() - started < 10 * SECOND);
            state.pause(0);
            state.pause(-1);
        } finally {
            pool.shutdownNow();
        }
    }
}
