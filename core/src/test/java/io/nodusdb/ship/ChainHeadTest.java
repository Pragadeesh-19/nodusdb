package io.nodusdb.ship;

import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainFormatException;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainHeadTest {

    private final MemoryObjectStore store = new MemoryObjectStore();

    private Optional<ChainHead.Found> find(Keyring keyring) {
        return new ChainHead(store, keyring).find();
    }

    @Test
    void anEmptyBucketHasNoHead() {
        assertEquals(Optional.empty(), find(Keyring.empty()));
    }

    @Test
    void aChainWithOnlyASnapshotReferenceEndsAtTheSnapshotsLsn() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        ChainObject ref = chain.snapshotRef(100, 1);

        ChainCursor cursor = find(Keyring.empty()).orElseThrow().cursor();

        assertEquals(1, cursor.seq());
        assertEquals(ref.digest(), cursor.digest());
        assertEquals(2, cursor.epoch());
        assertEquals(100, cursor.lastLsn());
    }

    @Test
    void theHeadIsTheNewestObjectAndItsLsnIsTheLastRecordsLsn() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.records(3);
        ChainObject last = chain.records(2);

        ChainCursor cursor = find(Keyring.empty()).orElseThrow().cursor();

        assertEquals(3, cursor.seq());
        assertEquals(last.digest(), cursor.digest());
        assertEquals(107, cursor.lastLsn());
    }

    @Test
    void aSnapshotReferenceAtTheHeadTakesItsLsnFromTheRecordsBeforeIt() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.records(3);
        ChainObject ref = chain.snapshotRef(102, 2);

        ChainCursor cursor = find(Keyring.empty()).orElseThrow().cursor();

        assertEquals(3, cursor.seq());
        assertEquals(ref.digest(), cursor.digest());
        assertEquals(104, cursor.lastLsn());
    }

    @Test
    void severalReferencesInARowLookBackThroughAllOfThem() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.records(1);
        chain.snapshotRef(101, 2);
        chain.snapshotRef(101, 3);

        assertEquals(102, find(Keyring.empty()).orElseThrow().cursor().lastLsn());
    }

    @Test
    void aChainOfOnlyReferencesEndsAtTheFirstReferencesLsn() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.snapshotRef(90, 2);

        assertEquals(100, find(Keyring.empty()).orElseThrow().cursor().lastLsn());
    }

    @Test
    void theSnapshotHintLimitsTheListingToObjectsSinceTheLastCheckpoint() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.many(1_200, 1);
        long checkpointSeq = chain.seq() + 1;
        chain.snapshotRef(5_000, checkpointSeq);
        chain.many(10, 1);
        FaultyObjectStore counting = new FaultyObjectStore(store);

        ChainCursor cursor = new ChainHead(counting, Keyring.empty()).find().orElseThrow().cursor();

        assertEquals(checkpointSeq + 10, cursor.seq());
        long chainListings = counting.calls().stream()
                .filter(call -> call.operation() == Operation.LIST && call.key().equals(ChainLayout.CHAIN_PREFIX))
                .count();
        assertEquals(1, chainListings, "ten objects after the checkpoint need one page, not two");
    }

    @Test
    void withoutAHintTheWholeChainIsListedAndStillFound() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.many(1_200, 1);
        FaultyObjectStore counting = new FaultyObjectStore(store);
        store.delete(ChainLayout.snapshotKey(100));

        ChainCursor cursor = new ChainHead(counting, Keyring.empty()).find().orElseThrow().cursor();

        assertEquals(1_201, cursor.seq());
        long chainListings = counting.calls().stream()
                .filter(call -> call.operation() == Operation.LIST && call.key().equals(ChainLayout.CHAIN_PREFIX))
                .count();
        assertEquals(2, chainListings);
    }

    @Test
    void aHintThatPointsPastTheRealHeadFallsBackToAFullListing() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.many(5, 1);
        store.putIfAbsent(ChainLayout.snapshotKey(999), "lying".getBytes(StandardCharsets.UTF_8),
                Map.of(ChainHead.FLOOR_METADATA, "9999"));

        assertEquals(6, find(Keyring.empty()).orElseThrow().cursor().seq());
    }

    @Test
    void anUnreadableHintIsIgnored() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.many(5, 1);
        store.putIfAbsent(ChainLayout.snapshotKey(200), "x".getBytes(StandardCharsets.UTF_8),
                Map.of(ChainHead.FLOOR_METADATA, "not-a-number"));
        store.putIfAbsent(ChainLayout.snapshotKey(300), "x".getBytes(StandardCharsets.UTF_8),
                Map.of(ChainHead.FLOOR_METADATA, "-4"));

        assertEquals(6, find(Keyring.empty()).orElseThrow().cursor().seq());
    }

    @Test
    void foreignKeysInTheChainFolderAreIgnored() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.many(3, 1);
        store.put("_nodus/chain/notes.txt", "x".getBytes(StandardCharsets.UTF_8));
        store.put("_nodus/chain/99999999999999999999.obj", "x".getBytes(StandardCharsets.UTF_8));

        assertEquals(4, find(Keyring.empty()).orElseThrow().cursor().seq());
    }

    @Test
    void aSignatureIsCheckedWhenTheKeyIsKnownAndSkippedWhenItIsNot() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.records(2);

        find(ChainBuilder.keyring());
        find(Keyring.empty());
        find(Keyring.single(ChainBuilder.KEY_ID + 1, KeyFiles.generate().getPublic()));
        assertThrows(ChainTrustException.class,
                () -> find(Keyring.single(ChainBuilder.KEY_ID, KeyFiles.generate().getPublic())));
    }

    @Test
    void aCorruptHeadObjectIsAnError() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.records(2);
        byte[] bytes = store.get(ChainLayout.chainKey(2)).orElseThrow();
        bytes[bytes.length / 2] ^= 0x01;
        store.put(ChainLayout.chainKey(2), bytes);

        assertThrows(ChainFormatException.class, () -> find(Keyring.empty()));
    }

    @Test
    void aListedObjectThatCannotBeReadIsAnError() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.records(2);
        FaultyObjectStore faulty = new FaultyObjectStore(store);
        store.delete(ChainLayout.chainKey(2));
        store.put(ChainLayout.chainKey(3), store.get(ChainLayout.chainKey(1)).orElseThrow());

        assertThrows(ChainTrustException.class, () -> new ChainHead(faulty, Keyring.empty()).find());
    }

    @Test
    void anObjectStoredUnderTheWrongSequenceNumberIsAnError() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        chain.records(2);
        store.put(ChainLayout.chainKey(7), store.get(ChainLayout.chainKey(2)).orElseThrow());

        ChainTrustException refused = assertThrows(ChainTrustException.class, () -> find(Keyring.empty()));

        assertTrue(refused.getMessage().contains("carries sequence number 2"), refused.getMessage());
    }

    @Test
    void aStoreFailureWhileFindingTheHeadIsPassedOn() {
        ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
        chain.snapshotRef(100, 1);
        FaultyObjectStore faulty = new FaultyObjectStore(store)
                .failNext(Operation.LIST, FaultyObjectStore.Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> new ChainHead(faulty, Keyring.empty()).find());
    }
}
