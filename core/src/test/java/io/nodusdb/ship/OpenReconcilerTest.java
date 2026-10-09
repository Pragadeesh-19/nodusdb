package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainLayout;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenReconcilerTest {

    private static final long SNAPSHOT_LSN = 40;

    private final MemoryObjectStore store = new MemoryObjectStore();
    private final List<Long> publishedFloors = new CopyOnWriteArrayList<>();

    private SnapshotPublisher publisher(ObjectStore target) {
        return floor -> {
            publishedFloors.add(floor);
            byte[] snapshot = ("snapshot " + SNAPSHOT_LSN).getBytes(StandardCharsets.UTF_8);
            String path = ChainLayout.snapshotKey(SNAPSHOT_LSN);
            target.put(path, snapshot);
            return new ChainBody.SnapshotRef(path, ChainHash.sha256(snapshot), SNAPSHOT_LSN);
        };
    }

    private OpenReconciler reconciler(ObjectStore target, long nonce, Keyring keyring) {
        return new OpenReconciler(target, new WriterIdentity(ChainBuilder.signingKey(), nonce), keyring,
                ShipSettings.defaults(), publisher(target), () -> 1_700_000_000_000_000L);
    }

    private OpenReconciler reconciler() {
        return reconciler(store, 7, Keyring.empty());
    }

    @Test
    void anEmptyBucketYieldsAnEpochAndTheSnapshotToStartTheChainWithoutWritingTheChain() {
        Start start = reconciler().reconcile(new Local(60, 4));

        assertEquals(5, start.epoch());
        assertTrue(start.startsChain());
        assertEquals(SNAPSHOT_LSN, start.firstReference().lsn());
        assertEquals(SNAPSHOT_LSN, start.shippedLsn());
        assertTrue(start.cursor().atStart());
        assertEquals(List.of(1L), publishedFloors);
        assertTrue(store.exists(ChainLayout.epochKey(5)));
        assertFalse(store.exists(ChainLayout.chainKey(1)));
        assertEquals(0, store.list(ChainLayout.CHAIN_PREFIX, "", 10).entries().size());
    }

    @Test
    void anOpenThatNeverStartedTheChainLeavesTheBucketEmptyAndTheNextOpenClaimsALaterEpoch() {
        Start first = reconciler().reconcile(new Local(60, 4));

        Start second = reconciler().reconcile(new Local(60, 4));

        assertEquals(5, first.epoch());
        assertEquals(6, second.epoch());
        assertTrue(second.startsChain());
        assertEquals(List.of(1L, 1L), publishedFloors);
    }

    @Test
    void aDirectoryAheadOfTheChainContinuesIt() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);

        Start start = reconciler().reconcile(new Local(chain.lastLsn() + 25, 3));

        assertEquals(4, start.epoch());
        assertFalse(start.startsChain());
        assertNull(start.firstReference());
        assertEquals(2, start.cursor().seq());
        assertEquals(chain.lastLsn(), start.cursor().lastLsn());
        assertEquals(chain.lastLsn(), start.shippedLsn());
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
    void aLogThatLostUnshippedRecordsIsRefusedAndClaimsNothing() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);
        int before = store.size();

        WriterFencedException refused = assertThrows(WriterFencedException.class,
                () -> reconciler().reconcile(new Local(chain.lastLsn() + 25, 3, chain.lastLsn() + 3)));

        assertTrue(refused.getMessage().contains("never shipped"), refused.getMessage());
        assertEquals(before, store.size());
    }

    @Test
    void aLogThatStillStartsRightAfterTheChainHeadContinuesIt() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);

        Start start = reconciler().reconcile(new Local(chain.lastLsn() + 25, 3, chain.lastLsn() + 1));

        assertEquals(4, start.epoch());
        assertEquals(chain.lastLsn(), start.shippedLsn());
    }

    @Test
    void aLogOneRecordShortOfTheChainHeadIsRefused() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);

        assertThrows(WriterFencedException.class,
                () -> reconciler().reconcile(new Local(chain.lastLsn() + 25, 3, chain.lastLsn() + 2)));
    }

    @Test
    void aFullyShippedDirectoryMayHaveTrimmedItsWholeLog() {
        ChainBuilder chain = new ChainBuilder(store, 9).epoch(3);
        chain.snapshotRef(SNAPSHOT_LSN, 1);
        chain.records(4);

        Start start = reconciler().reconcile(new Local(chain.lastLsn(), 3, chain.lastLsn() + 1));

        assertEquals(4, start.epoch());
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
        OpenReconciler failing = new OpenReconciler(store, new WriterIdentity(ChainBuilder.signingKey(), 7),
                Keyring.empty(), ShipSettings.defaults(), floor -> {
                    throw new TransientStoreException("upload failed", 0);
                }, () -> 1);

        assertThrows(TransientStoreException.class, () -> failing.reconcile(new Local(60, 4)));

        assertEquals(0, store.size());
    }

    @Test
    void aFailureWhileClaimingTheEpochIsPassedOnAndLeavesNoClaim() {
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        faulty.failWhen(call -> call.operation() == Operation.PUT_IF_ABSENT
                && call.key().equals(ChainLayout.epochKey(5)), 1, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class,
                () -> reconciler(faulty, 7, Keyring.empty()).reconcile(new Local(60, 4)));
        assertFalse(store.exists(ChainLayout.epochKey(5)));

        assertEquals(5, reconciler(faulty, 7, Keyring.empty()).reconcile(new Local(60, 4)).epoch());
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
        assertEquals(4,
                reconciler(store, 7, ChainBuilder.keyring()).reconcile(new Local(chain.lastLsn(), 3)).epoch());
    }

    @Test
    void writersOpeningTheSameBucketTogetherAlwaysGetDistinctEpochs() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 40; round++) {
                MemoryObjectStore shared = new MemoryObjectStore();
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Long>> attempts = new ArrayList<>();
                for (int writer = 0; writer < 2; writer++) {
                    long nonce = writer + 1;
                    attempts.add(pool.submit(() -> {
                        go.await();
                        return reconciler(shared, nonce, Keyring.empty()).reconcile(new Local(60, 4)).epoch();
                    }));
                }
                go.countDown();
                Set<Long> epochs = new HashSet<>();
                for (Future<Long> attempt : attempts) {
                    epochs.add(attempt.get(30, TimeUnit.SECONDS));
                }

                assertEquals(Set.of(5L, 6L), epochs, "round " + round);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
