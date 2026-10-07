package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.TransientStoreException;
import io.nodusdb.ship.OpenReconciler.Local;
import io.nodusdb.ship.OpenReconciler.Start;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenReconcilerTest {

    private static final long SNAPSHOT_LSN = 40;

    private final MemoryObjectStore store = new MemoryObjectStore();
    private final List<Long> publishedFloors = new ArrayList<>();

    private SnapshotPublisher publisher(ObjectStore target, long lsn) {
        return floor -> {
            publishedFloors.add(floor);
            byte[] snapshot = ("snapshot " + lsn).getBytes(StandardCharsets.UTF_8);
            String path = ChainLayout.snapshotKey(lsn);
            target.put(path, snapshot);
            return new ChainBody.SnapshotRef(path, ChainHash.sha256(snapshot), lsn);
        };
    }

    private OpenReconciler reconciler(ObjectStore target, long nonce, Keyring keyring) {
        return new OpenReconciler(target, ChainBuilder.signingKey(), keyring, nonce, ShipSettings.defaults(),
                publisher(target, SNAPSHOT_LSN), () -> 1_700_000_000_000_000L);
    }

    private OpenReconciler reconciler() {
        return reconciler(store, 7, Keyring.empty());
    }

    @Test
    void anEmptyBucketStartsAChainWithTheSnapshotAndClaimsTheNextEpoch() {
        Start start = reconciler().reconcile(new Local(60, 4));

        assertEquals(5, start.epoch());
        assertEquals(1, start.cursor().seq());
        assertEquals(5, start.cursor().epoch());
        assertEquals(SNAPSHOT_LSN, start.cursor().lastLsn());
        assertEquals(SNAPSHOT_LSN, start.historyGapBeforeLsn());
        assertEquals(List.of(1L), publishedFloors);
        ChainObject first = ChainCodec.decode(store.get(ChainLayout.chainKey(1)).orElseThrow());
        assertEquals(ChainKind.SNAPSHOT_REF, first.header().kind());
        assertEquals(ChainHash.ZERO, first.header().prev());
        assertEquals(start.cursor().digest(), first.digest());
        assertTrue(store.exists(ChainLayout.epochKey(5)));
        assertFalse(store.exists(ChainLayout.epochKey(4)));
    }

    @Test
    void aSecondOpenContinuesTheChainInTheNextEpoch() {
        Start first = reconciler().reconcile(new Local(60, 4));

        Start second = reconciler().reconcile(new Local(60, first.epoch()));

        assertEquals(6, second.epoch());
        assertEquals(first.cursor().seq(), second.cursor().seq());
        assertEquals(first.cursor().digest(), second.cursor().digest());
        assertEquals(0, second.historyGapBeforeLsn());
        assertEquals(List.of(1L), publishedFloors, "the snapshot is only published when the chain starts");
    }

    @Test
    void aDirectoryAheadOfTheChainContinuesIt() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);

        Start start = reconciler().reconcile(new Local(chain.lastLsn() + 25, 3));

        assertEquals(4, start.epoch());
        assertEquals(2, start.cursor().seq());
        assertEquals(chain.lastLsn(), start.cursor().lastLsn());
        assertEquals(chain.digest(), start.cursor().digest());
        assertTrue(publishedFloors.isEmpty());
    }

    @Test
    void aDirectoryExactlyAtTheChainHeadContinuesIt() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);

        Start start = reconciler().reconcile(new Local(chain.lastLsn(), 3));

        assertEquals(4, start.epoch());
    }

    @Test
    void aDirectoryBehindTheChainIsRefusedAndClaimsNothing() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);
        int before = store.size();

        WriterFencedException refused = assertThrows(WriterFencedException.class,
                () -> reconciler().reconcile(new Local(chain.lastLsn() - 1, 3)));

        assertTrue(refused.getMessage().contains("older than the chain"), refused.getMessage());
        assertEquals(before, store.size());
    }

    @Test
    void aDirectoryThatNeverSawTheChainsEpochIsRefusedAndClaimsNothing() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(8);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);
        int before = store.size();

        WriterFencedException refused = assertThrows(WriterFencedException.class,
                () -> reconciler().reconcile(new Local(chain.lastLsn() + 10, 5)));

        assertTrue(refused.getMessage().contains("another writer has continued the chain"), refused.getMessage());
        assertEquals(before, store.size());
    }

    @Test
    void epochsClaimedByCrashedOpensAreSkipped() {
        for (int epoch = 5; epoch <= 7; epoch++) {
            store.put(ChainLayout.epochKey(epoch), "{}".getBytes(StandardCharsets.UTF_8));
        }

        Start start = reconciler().reconcile(new Local(60, 4));

        assertEquals(8, start.epoch());
    }

    @Test
    void aStoreThatIgnoresIfNoneMatchIsRefusedBeforeAnythingIsWritten() {
        MemoryObjectStore memory = new MemoryObjectStore();
        FaultyObjectStore ignoring = new FaultyObjectStore(memory).ignoreConditionalWrites();

        UnsupportedFeatureException refused = assertThrows(UnsupportedFeatureException.class,
                () -> reconciler(ignoring, 7, Keyring.empty()).reconcile(new Local(60, 4)));

        assertTrue(refused.getMessage().contains("ignores If-None-Match"), refused.getMessage());
        assertEquals(0, memory.size());
        assertTrue(publishedFloors.isEmpty());
    }

    @Test
    void aFailureToPublishTheSnapshotLeavesNothingBehind() {
        OpenReconciler failing = new OpenReconciler(store, ChainBuilder.signingKey(), Keyring.empty(), 7,
                ShipSettings.defaults(), floor -> {
                    throw new TransientStoreException("upload failed", 0);
                }, () -> 1);

        assertThrows(TransientStoreException.class, () -> failing.reconcile(new Local(60, 4)));

        assertEquals(0, store.size());
    }

    @Test
    void aFailureWhileCommittingTheFirstObjectLeavesOnlyAnOrphanClaimThatTheNextOpenSkips() {
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        faulty.failWhen(call -> call.key().equals(ChainLayout.chainKey(1)), 1, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class,
                () -> reconciler(faulty, 7, Keyring.empty()).reconcile(new Local(60, 4)));
        assertTrue(store.exists(ChainLayout.epochKey(5)));
        assertFalse(store.exists(ChainLayout.chainKey(1)));

        Start retried = reconciler(faulty, 7, Keyring.empty()).reconcile(new Local(60, 4));

        assertEquals(6, retried.epoch());
        assertTrue(store.exists(ChainLayout.chainKey(1)));
    }

    @Test
    void aStoreFailureWhileLookingForTheHeadIsPassedOn() {
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        faulty.failWhen(call -> call.operation() == Operation.LIST && call.key().equals(ChainLayout.SNAPSHOT_PREFIX),
                1, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class,
                () -> reconciler(faulty, 7, Keyring.empty()).reconcile(new Local(60, 4)));
    }

    @Test
    void aHeadSignedByAnUntrustedKeyIsRefusedWhenAKeyringIsConfigured() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(2);
        Keyring wrong = Keyring.single(ChainBuilder.KEY_ID, KeyFiles.generate().getPublic());

        assertThrows(ChainTrustException.class,
                () -> reconciler(store, 7, wrong).reconcile(new Local(chain.lastLsn(), 3)));
        assertEquals(Start.class,
                reconciler(store, 7, ChainBuilder.keyring()).reconcile(new Local(chain.lastLsn(), 3)).getClass());
    }

    @Test
    void twoWritersOpeningAnEmptyBucketTogetherLeaveExactlyOneOfThem() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 40; round++) {
                MemoryObjectStore shared = new MemoryObjectStore();
                CountDownLatch go = new CountDownLatch(1);
                AtomicInteger winners = new AtomicInteger();
                List<Future<?>> attempts = new ArrayList<>();
                for (int writer = 0; writer < 2; writer++) {
                    long nonce = writer + 1;
                    attempts.add(pool.submit(() -> {
                        go.await();
                        try {
                            reconciler(shared, nonce, Keyring.empty()).reconcile(new Local(60, 4));
                            winners.incrementAndGet();
                        } catch (WriterFencedException lost) {
                            return null;
                        }
                        return null;
                    }));
                }
                go.countDown();
                for (Future<?> attempt : attempts) {
                    try {
                        attempt.get(30, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        throw new AssertionError(e.getCause());
                    }
                }

                assertEquals(1, winners.get(), "round " + round);
                assertEquals(1, shared.list(ChainLayout.CHAIN_PREFIX, "", 100).entries().size(), "round " + round);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
