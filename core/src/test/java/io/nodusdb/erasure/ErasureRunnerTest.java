package io.nodusdb.erasure;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.ErasurePendingException;
import io.nodusdb.kernel.ErasureProgress;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErasureRunnerTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    private final GraphKernel kernel = new GraphKernel();
    private final TupleStore store = TupleStore.open(kernel);
    private final AtomicLong clock = new AtomicLong();
    private ErasureRunner runner;

    private static final class ScriptedLayer implements ErasureLayer {

        private final AtomicLong covered = new AtomicLong();
        private final AtomicInteger failuresLeft = new AtomicInteger();
        private final AtomicInteger advances = new AtomicInteger();
        private volatile boolean dueSeen;

        @Override
        public String name() {
            return "scripted";
        }

        @Override
        public long coveredLsn() {
            return covered.get();
        }

        @Override
        public boolean covers(long requiredLsn, int erasedSymbols) {
            return covered.get() >= requiredLsn;
        }

        @Override
        public void advance(long targetLsn, boolean due) {
            advances.incrementAndGet();
            if (failuresLeft.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("the store is unreachable");
            }
            if (due) {
                dueSeen = true;
                covered.set(targetLsn);
            }
        }
    }

    private final ScriptedLayer layer = new ScriptedLayer();

    @AfterEach
    void stop() {
        if (runner != null) {
            runner.close();
        }
    }

    private ErasureRunner runner(Duration delay) {
        runner = new ErasureRunner(kernel, List.of(layer), delay, clock::get);
        return runner;
    }

    private Token eraseAlice() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:readme", "viewer", "user:alice");
        return store.erase("user:alice");
    }

    private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "condition not reached in time");
            Thread.sleep(5);
        }
    }

    @Test
    void nothingIsPendingBeforeAnyErasure() {
        ErasureRunner idle = runner(Duration.ofHours(1));

        idle.awaitErased(kernel.token(), Duration.ZERO);

        assertFalse(idle.layers().get(0).pending());
    }

    @Test
    void aZeroWaitReportsThePendingLayersAndTheToken() {
        Token token = eraseAlice();
        ErasureRunner pending = runner(Duration.ofHours(1));

        ErasurePendingException refused = assertThrows(ErasurePendingException.class,
                () -> pending.awaitErased(token, Duration.ZERO));

        assertEquals(List.of("scripted"), refused.pendingLayers());
        assertEquals(token.lsn(), refused.lsn());
        assertEquals(token.epoch(), refused.epoch());
    }

    @Test
    void aWaiterMakesTheWorkDueBeforeTheDelay() {
        Token token = eraseAlice();
        ErasureRunner waited = runner(Duration.ofHours(1));

        waited.awaitErased(token, WAIT);

        assertTrue(layer.dueSeen);
        assertEquals(token.lsn(), layer.coveredLsn());
    }

    @Test
    void withoutAWaiterTheWorkWaitsForTheDelay() throws InterruptedException {
        Token token = eraseAlice();
        ErasureRunner delayed = runner(Duration.ofSeconds(60));

        delayed.wake();
        awaitCondition(() -> layer.advances.get() >= 2);
        assertEquals(0, layer.coveredLsn());

        clock.addAndGet(Duration.ofSeconds(61).toNanos());
        awaitCondition(() -> layer.coveredLsn() == token.lsn());
        awaitCondition(() -> !runner.working());
    }

    @Test
    void aFailingLayerIsReportedAndRetriedUntilItSucceeds() throws InterruptedException {
        Token token = eraseAlice();
        layer.failuresLeft.set(2);
        ErasureRunner retrying = runner(Duration.ZERO);

        retrying.wake();
        awaitCondition(() -> !retrying.lastError().isEmpty());
        assertEquals("scripted: the store is unreachable", retrying.lastError());

        retrying.awaitErased(token, WAIT);
        assertEquals("", retrying.lastError());
        assertEquals(3, layer.advances.get());
    }

    @Test
    void theWorkerStopsOnceEveryLayerIsCleanAndStartsAgainForTheNextErasure() throws InterruptedException {
        Token first = eraseAlice();
        ErasureRunner running = runner(Duration.ZERO);
        running.awaitErased(first, WAIT);
        awaitCondition(() -> !runner.working());

        store.add("document:readme", "viewer", "user:bob");
        Token second = store.erase("user:bob");
        running.wake();

        running.awaitErased(second, WAIT);
        assertEquals(second.lsn(), layer.coveredLsn());
    }

    @Test
    void closingStopsTheWorkerAndRefusesWaiters() throws InterruptedException {
        Token token = eraseAlice();
        ErasureRunner closing = runner(Duration.ofHours(1));
        closing.wake();
        awaitCondition(closing::working);

        closing.close();

        assertFalse(closing.working());
        assertThrows(IllegalStateException.class, () -> closing.awaitErased(token, WAIT));
    }

    @Test
    void anInMemoryGraphHasNothingOutsideMemoryToWaitFor() {
        Token token = eraseAlice();

        kernel.erasureProgress().awaitErased(token, Duration.ZERO);

        assertEquals(ErasureProgress.NONE, kernel.erasureProgress());
        assertEquals(List.of(), kernel.erasureProgress().layers());
    }
}
