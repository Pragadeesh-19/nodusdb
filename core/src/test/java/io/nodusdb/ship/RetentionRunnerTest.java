package io.nodusdb.ship;

import io.nodusdb.chain.ChainLayout;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.MemoryObjectStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetentionRunnerTest {

    private static final long SECOND = 1_000_000_000L;
    private static final long HOUR = 3_600_000L;
    private static final long DAY = 24 * HOUR;
    private static final Duration SHORT = Duration.ofMillis(15);

    private final AtomicLong clock = new AtomicLong(1_000 * DAY);
    private final MemoryObjectStore memory = new MemoryObjectStore(clock::get);
    private final FaultyObjectStore store = new FaultyObjectStore(memory);
    private final ShipState state = new ShipState(2, 0, 15, 1 << 30);
    private final AtomicLong projected = new AtomicLong(ChainRetention.NO_PROJECTOR);
    private RetentionRunner runner;
    private long referenceLsn;

    @AfterEach
    void tearDown() {
        if (runner != null) {
            runner.close();
        }
    }

    private void buildChain() {
        ChainBuilder chain = new ChainBuilder(memory, 1).epoch(1);
        chain.snapshotRef(10, 1);
        for (int i = 2; i <= 10; i++) {
            clock.addAndGet(HOUR);
            chain.records(1);
        }
        clock.addAndGet(HOUR);
        chain.snapshotRef(chain.lastLsn(), 11);
        referenceLsn = chain.lastLsn();
        for (int i = 12; i <= 15; i++) {
            clock.addAndGet(HOUR);
            chain.records(1);
        }
        clock.addAndGet(8 * DAY);
    }

    private RetentionRunner start(Duration interval) {
        ChainRetention retention = new ChainRetention(store, Duration.ofDays(7), clock::get);
        runner = RetentionRunner.start(retention, state, projected::get, interval, "test-retention");
        return runner;
    }

    private List<Long> remaining() {
        List<Long> seqs = new ArrayList<>();
        for (String key : ChainAudit.chainKeys(memory)) {
            ChainLayout.chainSeq(key).ifPresent(seqs::add);
        }
        return seqs;
    }

    private static void waitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + 20 * SECOND;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertTrue(condition.getAsBoolean(), "the condition never became true");
    }

    @Test
    void nothingIsSweptBeforeTheFirstReferenceIsCommitted() throws Exception {
        buildChain();
        start(SHORT);

        Thread.sleep(150);

        assertEquals(15, remaining().size());
        assertEquals(0, state.snapshot().retention().sweeps());
    }

    @Test
    void oldObjectsAreDeletedOnceAReferenceIsCommitted() {
        buildChain();
        state.referenceCommitted(11, referenceLsn, 0);

        start(SHORT);

        waitUntil(() -> state.snapshot().retention().sweeps() >= 1);
        assertEquals(9, state.snapshot().retention().chainObjectsDeleted());
        assertEquals(6, remaining().size());
    }

    @Test
    void sweepsRepeatAtTheInterval() {
        buildChain();
        state.referenceCommitted(11, referenceLsn, 0);

        start(SHORT);

        waitUntil(() -> state.snapshot().retention().sweeps() >= 4);
    }

    @Test
    void theProjectorBoundIsReadOnEverySweep() {
        buildChain();
        state.referenceCommitted(11, referenceLsn, 0);
        projected.set(0);

        start(SHORT);
        waitUntil(() -> state.snapshot().retention().sweeps() >= 2);
        assertEquals(15, remaining().size());

        projected.set(ChainRetention.NO_PROJECTOR);
        waitUntil(() -> remaining().size() == 6);
    }

    @Test
    void aFailedSweepIsRecordedAndTheNextOneRecovers() {
        buildChain();
        state.referenceCommitted(11, referenceLsn, 0);
        store.failNext(Operation.GET, Fault.FAIL_BEFORE);

        start(SHORT);

        waitUntil(() -> state.snapshot().retention().sweeps() >= 1);
        ShipState.RetentionStatus status = state.snapshot().retention();
        assertEquals(1, status.failures());
        assertEquals("", status.lastError());
        assertEquals(6, remaining().size());
    }

    @Test
    void aFailureIsReportedUntilASweepSucceeds() {
        buildChain();
        state.referenceCommitted(11, referenceLsn, 0);
        store.failNext(Operation.GET, 1_000, Fault.FAIL_BEFORE);

        start(SHORT);

        waitUntil(() -> state.snapshot().retention().failures() >= 2);
        assertTrue(state.snapshot().retention().lastError().contains("injected failure"));
        assertEquals(0, state.snapshot().retention().sweeps());
        assertEquals(15, remaining().size());
    }

    @Test
    void closingIsPromptEvenWithALongInterval() {
        buildChain();
        state.referenceCommitted(11, referenceLsn, 0);
        start(Duration.ofHours(1));
        waitUntil(() -> state.snapshot().retention().sweeps() == 1);

        long started = System.nanoTime();
        runner.close();

        assertTrue(System.nanoTime() - started < 5 * SECOND);
        runner.close();
    }

    @Test
    void aNonPositiveIntervalIsRefused() {
        ChainRetention retention = new ChainRetention(store, Duration.ofDays(7), clock::get);

        assertThrows(IllegalArgumentException.class,
                () -> RetentionRunner.start(retention, state, projected::get, Duration.ZERO, "x"));
        assertThrows(IllegalArgumentException.class,
                () -> RetentionRunner.start(retention, state, projected::get, Duration.ofSeconds(-1), "x"));
        assertThrows(IllegalArgumentException.class,
                () -> RetentionRunner.start(retention, state, projected::get, null, "x"));
    }
}
