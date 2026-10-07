package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.ForwardingObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.ship.ShipState.Phase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotShipperTest {

    private static final long SECOND = 1_000_000_000L;
    private static final ShipSettings FAST = ShipSettings.defaults()
            .withRetries(Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofMillis(40));

    @TempDir
    Path scratch;

    private final MemoryObjectStore memory = new MemoryObjectStore();
    private final FaultyObjectStore store = new FaultyObjectStore(memory);
    private final ShipState state = new ShipState(2, 0, 5, 1 << 30);
    private final AtomicInteger released = new AtomicInteger();
    private SnapshotShipper shipper;

    @AfterEach
    void tearDown() {
        if (shipper != null) {
            shipper.close();
        }
    }

    private SnapshotShipper start(ShipSettings settings) {
        shipper = SnapshotShipper.start(new SnapshotUploader(store), state, settings, "test-snapshots");
        return shipper;
    }

    private StagedSnapshot staged(long lsn, int bytes) throws IOException {
        Path file = scratch.resolve("staged-" + lsn + ".bin");
        byte[] content = new byte[bytes];
        content[0] = (byte) lsn;
        Files.write(file, content);
        return new StagedSnapshot(file, lsn, released::incrementAndGet);
    }

    private ChainBody.SnapshotRef awaitReference() {
        long deadline = System.nanoTime() + 20 * SECOND;
        while (System.nanoTime() < deadline) {
            ChainBody.SnapshotRef taken = state.takeReference();
            if (taken != null) {
                return taken;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("no reference was offered: " + state.snapshot().references());
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
    void aSubmittedSnapshotIsUploadedOfferedAndReleased() throws IOException {
        start(FAST).submit(staged(30, 64));

        ChainBody.SnapshotRef reference = awaitReference();

        assertEquals(30, reference.lsn());
        assertEquals(ChainLayout.snapshotKey(30), reference.path());
        assertTrue(memory.exists(reference.path()));
        waitUntil(() -> released.get() == 1);
    }

    @Test
    void theChainFloorIsOneAboveTheChainPositionWhenTheUploadStarts() throws IOException {
        start(FAST).submit(staged(30, 64));

        awaitReference();

        assertEquals("6", memory.head(ChainLayout.snapshotKey(30)).orElseThrow().metadata()
                .get(ChainHead.FLOOR_METADATA));
    }

    @Test
    void transientFailuresAreRetriedWithBackoffUntilTheUploadSucceeds() throws IOException {
        store.failNext(Operation.PUT_FILE, 2, Fault.FAIL_BEFORE);
        start(FAST).submit(staged(30, 64));

        ChainBody.SnapshotRef reference = awaitReference();

        assertEquals(30, reference.lsn());
        ShipState.ReferenceStatus status = state.snapshot().references();
        assertEquals(2, status.failures());
        assertFalse(status.lastError().isEmpty());
    }

    @Test
    void aRejectedUploadIsRetriedAtTheSlowCadence() throws IOException {
        store.failNext(Operation.PUT_FILE, Fault.FATAL);
        start(FAST).submit(staged(30, 64));

        awaitReference();

        assertEquals(1, state.snapshot().references().failures());
    }

    @Test
    void aNewerSnapshotAbandonsAnOlderOneThatIsStillBeingRetried() throws IOException {
        ShipSettings patient = FAST.withRetries(Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofSeconds(60));
        store.failNext(Operation.PUT_FILE, Fault.FATAL);
        start(patient).submit(staged(30, 64));
        waitUntil(() -> state.snapshot().references().failures() == 1);

        shipper.submit(staged(60, 64));

        ChainBody.SnapshotRef reference = awaitReference();
        assertEquals(60, reference.lsn());
        assertFalse(memory.exists(ChainLayout.snapshotKey(30)));
        waitUntil(() -> released.get() == 2);
    }

    @Test
    void theLatestSubmissionWinsWhileAnUploadIsInFlight() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        ForwardingObjectStore blocking = new ForwardingObjectStore(store) {
            @Override
            public void putFile(String key, Path file, Map<String, String> metadata) {
                entered.countDown();
                try {
                    proceed.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.putFile(key, file, metadata);
            }
        };
        shipper = SnapshotShipper.start(new SnapshotUploader(blocking), state, FAST, "test-snapshots");
        shipper.submit(staged(10, 64));
        assertTrue(entered.await(10, TimeUnit.SECONDS));

        shipper.submit(staged(20, 64));
        shipper.submit(staged(30, 64));
        assertEquals(1, released.get());
        proceed.countDown();

        waitUntil(() -> released.get() == 3);
        assertTrue(memory.exists(ChainLayout.snapshotKey(10)));
        assertFalse(memory.exists(ChainLayout.snapshotKey(20)));
        assertTrue(memory.exists(ChainLayout.snapshotKey(30)));
        assertEquals(30, state.takeReference().lsn());
    }

    @Test
    void aConflictingSnapshotObjectFencesTheWriter() throws IOException {
        memory.put(ChainLayout.snapshotKey(30), new byte[]{9, 9, 9});
        start(FAST).submit(staged(30, 64));

        waitUntil(() -> state.phase() == Phase.FENCED);

        assertNull(state.takeReference());
        assertTrue(state.snapshot().lastError().contains("different content"));
        waitUntil(() -> released.get() == 1);
    }

    @Test
    void anUnreadableSnapshotFileIsAbandonedAndTheNextOneStillShips() throws IOException {
        Path gone = scratch.resolve("gone.bin");
        start(FAST).submit(new StagedSnapshot(gone, 10, released::incrementAndGet));
        waitUntil(() -> state.snapshot().references().failures() == 1);
        waitUntil(() -> released.get() == 1);

        shipper.submit(staged(20, 64));

        assertEquals(20, awaitReference().lsn());
        assertTrue(state.snapshot().references().lastError().contains("snapshot upload failed"));
    }

    @Test
    void closingReleasesAWaitingSnapshotAndDoesNotSitOutABackoff() throws IOException {
        ShipSettings patient = FAST.withRetries(Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofSeconds(60));
        store.failNext(Operation.PUT_FILE, 100, Fault.FATAL);
        start(patient).submit(staged(30, 64));
        waitUntil(() -> state.snapshot().references().failures() == 1);

        long started = System.nanoTime();
        shipper.close();

        assertTrue(System.nanoTime() - started < 5 * SECOND);
        assertEquals(1, released.get());
    }

    @Test
    void submittingAfterCloseReleasesTheSnapshotAndRefuses() throws IOException {
        start(FAST);
        shipper.close();
        StagedSnapshot late = staged(30, 64);

        assertThrows(IllegalStateException.class, () -> shipper.submit(late));

        assertEquals(1, released.get());
        assertNotNull(late);
    }

    @Test
    void closeCanBeCalledTwice() {
        start(FAST);

        shipper.close();
        shipper.close();
    }
}
