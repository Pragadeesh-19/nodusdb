package io.nodusdb.replica;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.ship.ChainHead;
import io.nodusdb.ship.ChainRetention;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnchorFinderTest {

    private static final long DAY = 86_400_000L;

    private final AtomicLong clock = new AtomicLong(1_000 * DAY);
    private final MemoryObjectStore store = new MemoryObjectStore(clock::get);
    private final ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);

    private AnchorFinder finder() {
        return new AnchorFinder(store, new ChainFetch(store, ChainBuilder.keyring(), Trust.REQUIRED));
    }

    private List<Anchor> all() {
        List<Anchor> anchors = new ArrayList<>();
        finder().candidates().forEachRemaining(anchors::add);
        return anchors;
    }

    private static List<String> described(List<Anchor> anchors) {
        List<String> lines = new ArrayList<>();
        for (Anchor anchor : anchors) {
            lines.add("L" + anchor.snapshotLsn() + " ref" + anchor.reference().seq() + " from" + anchor.firstSeq());
        }
        return lines;
    }

    @Test
    void anEmptyBucketHasNoAnchor() {
        assertEquals(List.of(), all());
    }

    @Test
    void theFirstReferenceOfAChainStartsTheReplayAtItself() {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);

        assertEquals(List.of("L10 ref1 from1"), described(all()));
    }

    @Test
    void aSnapshotOlderThanTheObjectsBeforeItsReferenceStartsAtTheObjectHoldingTheNextLsn() {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(3, 1);
        chain.snapshotRef(snapshotLsn, 5);
        chain.many(2, 1);

        assertEquals(List.of("L" + snapshotLsn + " ref8 from5", "L10 ref1 from1"), described(all()));
    }

    @Test
    void aSnapshotTakenAtTheChainHeadStartsAtTheLastRecordsObjectBeforeItsReference() {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        long head = chain.lastLsn();
        chain.snapshotRef(head, 5);

        assertEquals(List.of("L" + head + " ref5 from4", "L10 ref1 from1"), described(all()));
    }

    @Test
    void aSnapshotAheadOfTheRecordsShippedWhenItsReferenceWasCommittedStartsAtTheLastEarlierObject() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        chain.snapshotRef(20, 3);
        chain.many(2, 1);

        assertEquals(List.of("L20 ref3 from2", "L10 ref1 from1"), described(all()));
    }

    @Test
    void aReferenceCommittedBeforeTheRecordsItCoversStartsAtTheFirstObjectOfTheChain() {
        chain.snapshotRef(0, 1);
        chain.snapshotRef(2, 2);
        chain.many(1, 1);

        assertEquals(List.of("L2 ref2 from1", "L0 ref1 from1"), described(all()));
    }

    @Test
    void consecutiveReferencesWithNoRecordsBetweenThemStartAtTheLaterReference() {
        chain.snapshotRef(10, 1);
        chain.snapshotRef(10, 2);

        assertEquals(List.of("L10 ref1 from1"), described(all()));
    }

    @Test
    void aHoleInTheRunTheSnapshotNeedsSkipsThatCandidateAndYieldsTheOlderOne() {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(3, 1);
        chain.snapshotRef(snapshotLsn, 5);
        store.delete(ChainLayout.chainKey(6));

        assertEquals(List.of("L10 ref1 from1"), described(all()));
    }

    @Test
    void aSnapshotObjectThatWasDeletedIsSkipped() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        chain.snapshotRef(chain.lastLsn(), 4);
        long newest = chain.lastLsn();
        store.delete(ChainLayout.snapshotKey(newest));

        assertEquals(List.of("L10 ref1 from1"), described(all()));
    }

    @Test
    void aSnapshotThatWasUploadedButNeverReferencedIsSkipped() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        store.put(ChainLayout.snapshotKey(999), new byte[]{1});

        assertEquals(List.of("L10 ref1 from1"), described(all()));
    }

    @Test
    void aFloorHintBeyondTheReferenceSkipsThatCandidateInsteadOfFailing() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        chain.snapshotRef(chain.lastLsn(), 999);

        assertEquals(List.of("L10 ref1 from1"), described(all()));
    }

    @Test
    void aMissingFloorHintScansFromTheOldestObject() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        long head = chain.lastLsn();
        byte[] content = "snapshot".getBytes();
        store.putIfAbsent(ChainLayout.snapshotKey(head), content, Map.of());
        ChainObject reference = ChainCodec.seal(new ChainHeader(ChainKind.SNAPSHOT_REF, chain.seq() + 1, 2, 1,
                chain.digest(), ChainBuilder.KEY_ID), new ChainBody.SnapshotRef(ChainLayout.snapshotKey(head),
                ChainHash.sha256(content), head), ChainBuilder.signingKey());
        store.put(ChainLayout.chainKey(chain.seq() + 1), reference.encoded());

        assertEquals(List.of("L" + head + " ref4 from3", "L10 ref1 from1"), described(all()));
    }

    @Test
    void aReferenceSignedByAnotherKeyStopsTheSearchInsteadOfFallingBackToAnOlderOne() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        ChainObject forged = ChainCodec.seal(new ChainHeader(ChainKind.SNAPSHOT_REF, 4, 2, 1,
                ChainHash.ZERO, ChainBuilder.KEY_ID), new ChainBody.SnapshotRef(ChainLayout.snapshotKey(50),
                ChainHash.ZERO, 50), new SigningKey(ChainBuilder.KEY_ID, KeyFiles.generate().getPrivate()));
        store.put(ChainLayout.chainKey(4), forged.encoded());
        store.putIfAbsent(ChainLayout.snapshotKey(50), new byte[]{1}, Map.of(ChainHead.FLOOR_METADATA, "4"));

        Iterator<Anchor> candidates = finder().candidates();

        assertThrows(ChainTrustException.class, candidates::hasNext);
    }

    @Test
    void aReferenceThatNamesASnapshotWhoseNameHoldsAnotherLsnIsRefused() {
        chain.snapshotRef(10, 1);
        ChainObject lying = ChainCodec.seal(new ChainHeader(ChainKind.SNAPSHOT_REF, 2, 2, 1,
                store.get(ChainLayout.chainKey(1)).map(bytes -> ChainCodec.decode(bytes).digest()).orElseThrow(),
                ChainBuilder.KEY_ID), new ChainBody.SnapshotRef(ChainLayout.snapshotKey(30), ChainHash.ZERO, 31),
                ChainBuilder.signingKey());
        store.put(ChainLayout.chainKey(2), lying.encoded());
        store.putIfAbsent(ChainLayout.snapshotKey(30), new byte[]{1}, Map.of(ChainHead.FLOOR_METADATA, "2"));

        Iterator<Anchor> candidates = finder().candidates();

        assertThrows(ChainTrustException.class, candidates::hasNext);
    }

    @Test
    void onlyTheNewestEightSnapshotsAreExamined() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        for (int lsn = 100; lsn < 108; lsn++) {
            store.put(ChainLayout.snapshotKey(lsn), new byte[]{1});
        }

        assertEquals(List.of(), all());
    }

    @Test
    void theIteratorDoesTheWorkForOneCandidateAtATime() {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        long snapshotLsn = chain.lastLsn();
        chain.snapshotRef(snapshotLsn, 4);

        Iterator<Anchor> candidates = finder().candidates();

        assertTrue(candidates.hasNext());
        assertEquals(snapshotLsn, candidates.next().snapshotLsn());
        assertTrue(candidates.hasNext());
        assertEquals(10, candidates.next().snapshotLsn());
        assertFalse(candidates.hasNext());
    }

    @Test
    void aSweepNeverRemovesWhatTheNewestAnchorNeeds() {
        chain.snapshotRef(10, 1);
        chain.many(5, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(4, 1);
        chain.snapshotRef(snapshotLsn, 11);
        chain.many(2, 1);
        clock.addAndGet(30 * DAY);

        new ChainRetention(store, java.time.Duration.ofDays(7), clock::get)
                .sweep(new ChainRetention.Reference(11, snapshotLsn), ChainRetention.NO_PROJECTOR);

        Anchor newest = all().get(0);
        assertEquals(snapshotLsn, newest.snapshotLsn());
        assertEquals(7, newest.firstSeq());
        for (long seq = newest.firstSeq(); seq <= 13; seq++) {
            assertTrue(store.exists(ChainLayout.chainKey(seq)), "object " + seq + " must survive");
        }
    }
}
