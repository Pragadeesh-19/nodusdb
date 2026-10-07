package io.nodusdb.ship;

import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.ShipTimeoutException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.PutResult;
import io.nodusdb.ship.ShipState.Phase;
import io.nodusdb.ship.ShipperRig.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShipperRunnerTest {

    private static final long SECOND = 1_000_000_000L;
    private static final ShipSettings FAST = ShipSettings.defaults()
            .withInterval(Duration.ofMillis(10))
            .withRetries(Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofMillis(40));

    private ShipperRig rig = new ShipperRig(new MemoryObjectStore(), FAST, SyncMode.SYNC);
    private ShipperRunner runner;

    @AfterEach
    void tearDown() {
        if (runner != null) {
            runner.close();
        }
        rig.close();
    }

    private ShipperRunner run(Session session, ShipSettings settings) {
        runner = ShipperRunner.start(session.core(), session.state(), session.feed(), settings, "test-shipper");
        return runner;
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
    void aWriterWaitingForItsLsnIsReleasedOnceTheShipperHasCommittedIt() {
        rig.append(2);
        Session session = rig.open(1);
        run(session, FAST);

        long lsn = rig.append(3);
        session.state().awaitShipped(session.start().epoch(), lsn, 20 * SECOND);

        assertTrue(session.state().shippedLsn() >= lsn);
        runner.close();
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    @Test
    void manyWritersWaitingTogetherAreAllReleasedAndTheChainStaysExact() throws Exception {
        rig.append(2);
        Session session = rig.open(1);
        run(session, FAST);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> writers = new ArrayList<>();
            for (int writer = 0; writer < 8; writer++) {
                writers.add(pool.submit(() -> {
                    for (int i = 0; i < 15; i++) {
                        long lsn = rig.append(1 + i % 3);
                        session.state().awaitShipped(session.start().epoch(), lsn, 30 * SECOND);
                    }
                    return null;
                }));
            }
            for (Future<?> writer : writers) {
                writer.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        runner.close();
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    @Test
    void closingShipsWhatWasAppendedEvenWhenTheShipperWasIdle() {
        rig.append(2);
        Session session = rig.open(1);
        ShipSettings slow = FAST.withInterval(Duration.ofSeconds(30));
        run(session, slow);
        waitUntil(() -> session.state().phase() == Phase.ACTIVE);

        long lsn = rig.append(4);
        runner.close();

        assertEquals(Phase.CLOSED, session.state().phase());
        assertTrue(session.state().shippedLsn() >= lsn);
        ChainAudit.verify(rig.memory, rig.disk, lsn);
    }

    @Test
    void closingAnIdleShipperIsPrompt() throws Exception {
        rig.append(2);
        Session session = rig.open(1);
        run(session, FAST.withInterval(Duration.ofSeconds(30)));
        waitUntil(() -> session.state().phase() == Phase.ACTIVE);

        long started = System.nanoTime();
        runner.close();

        assertTrue(System.nanoTime() - started < 5 * SECOND);
        assertEquals(Phase.CLOSED, session.state().phase());
    }

    @Test
    void closingDoesNotSitOutALongFailureBackoff() throws Exception {
        rig.append(2);
        Session session = rig.open(1);
        ShipSettings patient = FAST.withRetries(Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofSeconds(60));
        rig.store.failNext(Operation.PUT_IF_ABSENT, 1_000, Fault.FATAL);
        run(session, patient);
        waitUntil(() -> session.state().phase() == Phase.FAILED);

        long started = System.nanoTime();
        runner.close();

        assertTrue(System.nanoTime() - started < 5 * SECOND);
        assertEquals(Phase.CLOSED, session.state().phase());
    }

    @Test
    void closeCanBeCalledTwice() {
        rig.append(2);
        Session session = rig.open(1);
        run(session, FAST);

        runner.close();
        runner.close();

        assertEquals(Phase.CLOSED, session.state().phase());
    }

    @Test
    void aFencedShipperStopsAndKeepsItsPhaseForWaitersUntilItIsClosed() throws Exception {
        rig.append(2);
        Session session = rig.open(1);
        rig.memory.put(ChainLayout.epochKey(session.start().epoch() + 1), "{}".getBytes(StandardCharsets.UTF_8));
        run(session, FAST);

        long lsn = rig.append(2);
        waitUntil(() -> session.state().phase() == Phase.FENCED);

        assertThrows(WriterFencedException.class,
                () -> session.state().awaitShipped(session.start().epoch(), lsn, 5 * SECOND));
        assertEquals(Phase.FENCED, session.state().phase());
        runner.close();
        assertEquals(Phase.CLOSED, session.state().phase());
    }

    @Test
    void aWaiterIsToldAtOnceWhenShippingHasFailedAndIsReleasedWhenItRecovers() throws Exception {
        rig.append(2);
        Session session = rig.open(1);
        rig.store.failNext(Operation.PUT_IF_ABSENT, 3, Fault.FATAL);
        run(session, FAST);
        long lsn = rig.append(2);
        waitUntil(() -> session.state().phase() == Phase.FAILED);

        ShipTimeoutException refused = assertThrows(ShipTimeoutException.class,
                () -> session.state().awaitShipped(session.start().epoch(), lsn, 5 * SECOND));
        assertEquals(lsn, refused.lsn());
        assertEquals(session.start().epoch(), refused.epoch());

        waitUntil(() -> session.state().shippedLsn() >= lsn);
        assertEquals(Phase.ACTIVE, session.state().phase());
    }

    @Test
    void anUnexpectedExceptionFailsTheShipperInsteadOfKillingItAndItRecovers() throws Exception {
        rig.close();
        MemoryObjectStore memory = new MemoryObjectStore();
        ExplodingStore exploding = new ExplodingStore(memory);
        rig = new ShipperRig(memory, FAST, SyncMode.SYNC);
        rig.append(2);
        Session session = rig.open(1);
        exploding.armed.set(true);
        ShipperCore core = new ShipperCore(exploding, new EpochClaims(exploding, 1, ChainBuilder.KEY_ID, () -> 0),
                new WriterIdentity(ChainBuilder.signingKey(), 1), session.start(), session.feed(), session.state(),
                FAST, System::nanoTime);
        runner = ShipperRunner.start(core, session.state(), session.feed(), FAST, "test-shipper");
        long lsn = rig.append(2);

        waitUntil(() -> session.state().phase() == Phase.FAILED);
        assertTrue(session.state().snapshot().lastError().contains("unexpected failure"));
        exploding.armed.set(false);

        waitUntil(() -> session.state().shippedLsn() >= lsn);
        assertEquals(Phase.ACTIVE, session.state().phase());
    }

    @ParameterizedTest
    @ValueSource(longs = {7, 11, 19, 23, 31})
    void aWriterThreadAndARandomlyFailingStoreEndWithAnExactChain(long seed) throws Exception {
        rig.close();
        rig = new ShipperRig(new MemoryObjectStore(), FAST, SyncMode.SYNC);
        rig.append(2);
        Session session = rig.open(1);
        rig.store.randomFaults(seed, 0.1, 0.1, 0.1).exemptFromRandomConflicts("_nodus/probe/");
        run(session, FAST);
        Random random = new Random(seed);
        long last = 0;
        for (int i = 0; i < 120; i++) {
            last = rig.append(1 + random.nextInt(4));
            if (i % 6 == 0) {
                awaitThroughRetries(session, last);
            }
        }
        rig.store.stopRandomFaults();

        awaitThroughRetries(session, last);

        runner.close();
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    private void awaitThroughRetries(Session session, long lsn) {
        long deadline = System.nanoTime() + 60 * SECOND;
        while (true) {
            try {
                session.state().awaitShipped(session.start().epoch(), lsn, 5 * SECOND);
                return;
            } catch (ShipTimeoutException timeout) {
                assertTrue(System.nanoTime() < deadline, "LSN " + lsn + " was never shipped: " + timeout.getMessage());
            }
        }
    }

    private static final class ExplodingStore implements ObjectStore {

        private final ObjectStore delegate;
        private final AtomicBoolean armed = new AtomicBoolean();

        ExplodingStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata) {
            if (armed.get() && key.startsWith(ChainLayout.CHAIN_PREFIX)) {
                throw new IllegalStateException("simulated defect");
            }
            return delegate.putIfAbsent(key, content, metadata);
        }

        @Override
        public void put(String key, byte[] content) {
            delegate.put(key, content);
        }

        @Override
        public void putFile(String key, Path file, Map<String, String> metadata) {
            delegate.putFile(key, file, metadata);
        }

        @Override
        public Optional<byte[]> get(String key) {
            return delegate.get(key);
        }

        @Override
        public Optional<byte[]> getRange(String key, long offset, int length) {
            return delegate.getRange(key, offset, length);
        }

        @Override
        public Optional<ObjectInfo> head(String key) {
            return delegate.head(key);
        }

        @Override
        public ListPage list(String prefix, String startAfter, int maxKeys) {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(String key) {
            delegate.delete(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            delegate.deleteAll(keys);
        }
    }
}
