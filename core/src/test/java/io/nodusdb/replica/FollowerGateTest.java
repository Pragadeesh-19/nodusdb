package io.nodusdb.replica;

import io.nodusdb.error.StaleReadException;
import io.nodusdb.kernel.CapturingLog;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FollowerGateTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private final AtomicLong nanos = new AtomicLong(100 * SECOND);
    private final GraphKernel kernel = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
    private final FollowerState state = new FollowerState();
    private final GraphKernel primary = new GraphKernel();
    private final CapturingLog log = new CapturingLog();

    @BeforeEach
    void attach() {
        primary.attachLog(log, CapturingLog.noStorage());
        kernel.recordEpoch(1, 1, 0);
    }

    private FollowerGate gate(Duration staleness, Duration readWait) {
        FollowerConfig.Follow follow = new FollowerConfig.Follow(Duration.ofMillis(100), readWait, staleness, 2, null);
        return new FollowerGate(kernel, state, follow, nanos::get);
    }

    private void replicateOneEdge(int u, int v) {
        primary.addEdge(u, v);
        CapturingLog.Drained drained = log.drain();
        kernel.applyReplicated(drained.records(), drained.first(), drained.last());
    }

    @Test
    void aFollowerThatHasNotBootstrappedRefusesReads() {
        FollowerGate gate = gate(null, Duration.ofMillis(50));

        StaleReadException refused = assertThrows(StaleReadException.class, gate::requireReadable);

        assertTrue(refused.getMessage().contains("snapshot"), refused.getMessage());
    }

    @Test
    void aBootstrappedFollowerWithoutAStalenessBoundServesReadsInEveryLiveState() {
        FollowerGate gate = gate(null, Duration.ofMillis(50));
        state.bootstrapped(0, 0, 1, 1);

        assertDoesNotThrow(gate::requireReadable);
        state.lagging("the store is down");
        assertDoesNotThrow(gate::requireReadable);
        state.stalled("rollback");
        assertDoesNotThrow(gate::requireReadable);
    }

    @Test
    void aClosedFollowerRefusesReads() {
        FollowerGate gate = gate(null, Duration.ofMillis(50));
        state.bootstrapped(0, 0, 1, 1);
        state.closed();

        assertThrows(IllegalStateException.class, gate::requireReadable);
    }

    @Test
    void aStalenessBoundRefusesReadsUntilTheHeadWasConfirmedAndAgainOnceItIsOld() {
        FollowerGate gate = gate(Duration.ofSeconds(5), Duration.ofMillis(50));
        state.bootstrapped(0, 0, 1, 1);

        assertThrows(StaleReadException.class, gate::requireReadable);

        state.current(nanos.get());
        assertDoesNotThrow(gate::requireReadable);

        nanos.addAndGet(5 * SECOND);
        assertDoesNotThrow(gate::requireReadable);

        nanos.addAndGet(1);
        StaleReadException stale = assertThrows(StaleReadException.class, gate::requireReadable);
        assertTrue(stale.getMessage().contains("5000"), stale.getMessage());

        state.current(nanos.get());
        assertDoesNotThrow(gate::requireReadable);
    }

    @Test
    void aStalledFollowerWithAStalenessBoundStopsServingOnceTheBoundPasses() {
        FollowerGate gate = gate(Duration.ofSeconds(5), Duration.ofMillis(50));
        state.bootstrapped(0, 0, 1, 1);
        state.current(nanos.get());
        state.stalled("rollback");

        assertDoesNotThrow(gate::requireReadable);
        nanos.addAndGet(6 * SECOND);

        assertThrows(StaleReadException.class, gate::requireReadable);
    }

    @Test
    void aTokenTheFollowerAlreadyHoldsIsNotWaitedFor() {
        FollowerGate gate = gate(null, Duration.ofSeconds(30));
        state.bootstrapped(0, 0, 1, 1);
        replicateOneEdge(1, 2);
        long begin = System.nanoTime();

        gate.awaitToken(kernel.token());

        assertTrue(System.nanoTime() - begin < TimeUnit.SECONDS.toNanos(5));
    }

    @Test
    void aTokenAheadOfTheFollowerIsWaitedForUntilItArrives() throws Exception {
        FollowerGate gate = gate(null, Duration.ofSeconds(30));
        state.bootstrapped(0, 0, 1, 1);
        Token ahead = new Token(1, 1);
        CountDownLatch waiting = new CountDownLatch(1);
        boolean[] reached = new boolean[1];
        Thread reader = new Thread(() -> {
            waiting.countDown();
            gate.awaitToken(ahead);
            reached[0] = kernel.appliedLsn() >= ahead.lsn();
        });
        reader.start();
        waiting.await();
        Thread.sleep(50);

        replicateOneEdge(1, 2);
        state.applied(2, kernel.appliedLsn(), 1, 10);
        reader.join(10_000);

        assertFalse(reader.isAlive());
        assertTrue(reached[0]);
    }

    @Test
    void aTokenThatNeverArrivesIsWaitedForOnlyAsLongAsTheReadWait() {
        FollowerGate gate = gate(null, Duration.ofMillis(80));
        state.bootstrapped(0, 0, 1, 1);
        long begin = System.nanoTime();

        gate.awaitToken(new Token(1, 500));

        long waited = System.nanoTime() - begin;
        assertTrue(waited >= TimeUnit.MILLISECONDS.toNanos(70), "waited " + waited);
        assertTrue(waited < TimeUnit.SECONDS.toNanos(10), "waited " + waited);
    }

    @Test
    void aTokenFromANewerEpochIsWaitedFor() {
        FollowerGate gate = gate(null, Duration.ofMillis(60));
        state.bootstrapped(0, 0, 1, 1);
        long begin = System.nanoTime();

        gate.awaitToken(new Token(2, 1));

        assertTrue(System.nanoTime() - begin >= TimeUnit.MILLISECONDS.toNanos(50));
    }

    @Test
    void aTokenFromAnOlderEpochIsNotWaitedFor() {
        kernel.recordEpoch(3, 10, 9);
        FollowerGate gate = gate(null, Duration.ofSeconds(30));
        state.bootstrapped(0, 0, 1, 1);
        long begin = System.nanoTime();

        gate.awaitToken(new Token(1, 500));

        assertTrue(System.nanoTime() - begin < TimeUnit.SECONDS.toNanos(5));
    }

    @Test
    void aStalledFollowerDoesNotWaitForATokenItCanNeverReach() {
        FollowerGate gate = gate(null, Duration.ofSeconds(30));
        state.bootstrapped(0, 0, 1, 1);
        state.stalled("rollback");
        long begin = System.nanoTime();

        gate.awaitToken(new Token(1, 500));

        assertTrue(System.nanoTime() - begin < TimeUnit.SECONDS.toNanos(5));
    }
}
