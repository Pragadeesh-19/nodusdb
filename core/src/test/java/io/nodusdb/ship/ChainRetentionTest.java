package io.nodusdb.ship;

import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainFormatException;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.ForwardingObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.TransientStoreException;
import io.nodusdb.ship.ChainRetention.Reference;
import io.nodusdb.ship.ChainRetention.Result;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainRetentionTest {

    private static final long HOUR = 3_600_000L;
    private static final long DAY = 24 * HOUR;
    private static final Duration WEEK = Duration.ofDays(7);

    private final AtomicLong clock = new AtomicLong(1_000 * DAY);
    private final MemoryObjectStore memory = new MemoryObjectStore(clock::get);
    private final FaultyObjectStore store = new FaultyObjectStore(memory);
    private final ChainBuilder chain = new ChainBuilder(memory, 1).epoch(1);

    private ChainRetention retention() {
        return new ChainRetention(store, WEEK, clock::get);
    }

    private List<Long> remaining() {
        List<Long> seqs = new ArrayList<>();
        for (String key : ChainAudit.chainKeys(memory)) {
            ChainLayout.chainSeq(key).ifPresent(seqs::add);
        }
        return seqs;
    }

    private static List<Long> range(long from, long to) {
        List<Long> seqs = new ArrayList<>();
        for (long seq = from; seq <= to; seq++) {
            seqs.add(seq);
        }
        return seqs;
    }

    private long standardChain() {
        chain.snapshotRef(10, 1);
        for (int i = 2; i <= 10; i++) {
            clock.addAndGet(HOUR);
            chain.records(1);
        }
        clock.addAndGet(HOUR);
        chain.snapshotRef(chain.lastLsn(), 11);
        long referenceLsn = chain.lastLsn();
        for (int i = 12; i <= 15; i++) {
            clock.addAndGet(HOUR);
            chain.records(1);
        }
        return referenceLsn;
    }

    @Test
    void objectsOlderThanTheRetentionAndBeforeTheLastRecordsAheadOfTheNewestReferenceAreDeleted() {
        long referenceLsn = standardChain();
        clock.addAndGet(8 * DAY);

        Result result = retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(9, result.chainObjects());
        assertEquals(range(10, 15), remaining());
    }

    @Test
    void nothingYoungerThanTheRetentionIsDeleted() {
        long referenceLsn = standardChain();
        clock.addAndGet(3 * DAY);

        Result result = retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(0, result.chainObjects());
        assertEquals(range(1, 15), remaining());
    }

    @Test
    void onlyThePrefixOlderThanTheRetentionIsDeleted() {
        chain.snapshotRef(10, 1);
        chain.many(4, 1);
        clock.addAndGet(6 * DAY);
        chain.many(5, 1);
        chain.snapshotRef(chain.lastLsn(), 11);
        long referenceLsn = chain.lastLsn();
        chain.many(4, 1);
        clock.addAndGet(2 * DAY);

        Result result = retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(5, result.chainObjects());
        assertEquals(range(6, 15), remaining());
    }

    @Test
    void anObjectExactlyAtTheCutoffIsKept() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        clock.addAndGet(HOUR);
        chain.many(1, 1);
        chain.snapshotRef(chain.lastLsn(), 5);
        long referenceLsn = chain.lastLsn();
        chain.many(1, 1);
        clock.set(1_000 * DAY + WEEK.toMillis());

        Result result = retention().sweep(new Reference(5, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(0, result.chainObjects());
        clock.addAndGet(1);
        assertEquals(3, retention().sweep(new Reference(5, referenceLsn), ChainRetention.NO_PROJECTOR).chainObjects());
        assertEquals(range(4, 6), remaining());
    }

    @Test
    void theLastProjectedObjectAndEverythingAfterItAreKeptAndNoProjectorMeansNoBound() {
        long referenceLsn = standardChain();
        clock.addAndGet(8 * DAY);

        assertEquals(0, retention().sweep(new Reference(11, referenceLsn), 0).chainObjects());
        assertEquals(3, retention().sweep(new Reference(11, referenceLsn), 4).chainObjects());
        assertEquals(range(4, 15), remaining());
        assertEquals(6, retention().sweep(new Reference(11, referenceLsn), 100).chainObjects());
        assertEquals(range(10, 15), remaining());
    }

    @Test
    void aSnapshotOlderThanTheObjectsBeforeItsReferenceKeepsEveryObjectAfterTheSnapshot() {
        chain.snapshotRef(10, 1);
        chain.many(5, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(4, 1);
        chain.snapshotRef(snapshotLsn, 11);
        chain.many(2, 1);
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(11, snapshotLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(6, result.chainObjects());
        assertEquals(range(7, 13), remaining());
    }

    @Test
    void theObjectHoldingTheFirstRecordAfterTheSnapshotIsTheOneKept() {
        chain.snapshotRef(10, 1);
        chain.many(6, 1);
        chain.snapshotRef(10, 8);
        chain.many(1, 1);
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(8, 10), ChainRetention.NO_PROJECTOR);

        assertEquals(1, result.chainObjects());
        assertEquals(range(2, 9), remaining());
    }

    @Test
    void theProjectorBoundStillWinsOverTheSnapshotAnchor() {
        chain.snapshotRef(10, 1);
        chain.many(5, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(4, 1);
        chain.snapshotRef(snapshotLsn, 11);
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(11, snapshotLsn), 3);

        assertEquals(2, result.chainObjects());
        assertEquals(range(3, 11), remaining());
    }

    @Test
    void aGapInsideTheNeededRunStopsTheWalkAboveTheGap() {
        chain.snapshotRef(10, 1);
        chain.many(5, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(4, 1);
        chain.snapshotRef(snapshotLsn, 11);
        memory.delete(ChainLayout.chainKey(8));
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(11, snapshotLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(7, result.chainObjects());
        assertEquals(range(9, 11), remaining());
    }

    @Test
    void aNewestReferenceAtTheStartOfTheChainDeletesNothing() {
        chain.snapshotRef(10, 1);
        chain.many(5, 1);
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(1, 10), ChainRetention.NO_PROJECTOR);

        assertEquals(0, result.chainObjects());
        assertEquals(range(1, 6), remaining());
    }

    @Test
    void consecutiveReferencesAreWalkedBackToTheLastRecordsObject() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        chain.snapshotRef(chain.lastLsn(), 4);
        chain.snapshotRef(chain.lastLsn(), 5);
        long referenceLsn = chain.lastLsn();
        chain.many(1, 1);
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(5, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(2, result.chainObjects());
        assertEquals(range(3, 6), remaining());
    }

    @Test
    void theWalkBackStopsAtAnObjectThatIsAlreadyGone() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        chain.snapshotRef(chain.lastLsn(), 4);
        chain.snapshotRef(chain.lastLsn(), 5);
        long referenceLsn = chain.lastLsn();
        chain.many(1, 1);
        memory.delete(ChainLayout.chainKey(1));
        memory.delete(ChainLayout.chainKey(2));
        memory.delete(ChainLayout.chainKey(3));
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(5, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(0, result.chainObjects());
        assertEquals(range(4, 6), remaining());
    }

    @Test
    void headDiscoveryStillWorksAfterASweepWhenTheHeadIsAReference() {
        chain.snapshotRef(10, 1);
        chain.many(5, 1);
        long recordsEnd = chain.lastLsn();
        chain.snapshotRef(recordsEnd, 7);
        clock.addAndGet(30 * DAY);

        retention().sweep(new Reference(7, recordsEnd), ChainRetention.NO_PROJECTOR);

        assertEquals(range(6, 7), remaining());
        ChainCursor cursor = new ChainHead(memory, Keyring.empty()).find().orElseThrow().cursor();
        assertEquals(7, cursor.seq());
        assertEquals(recordsEnd, cursor.lastLsn());
    }

    @Test
    void aSecondSweepFindsNothingMoreToDelete() {
        long referenceLsn = standardChain();
        clock.addAndGet(8 * DAY);
        retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        Result again = retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(new Result(0, 0, 0), again);
    }

    @Test
    void foreignKeysInTheChainFolderAreLeftAlone() {
        long referenceLsn = standardChain();
        memory.put("_nodus/chain/notes.txt", "x".getBytes(StandardCharsets.UTF_8));
        clock.addAndGet(8 * DAY);

        retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertTrue(memory.exists("_nodus/chain/notes.txt"));
        assertEquals(range(10, 15), remaining());
    }

    @Test
    void oldSnapshotsBelowTheNewestReferenceAreDeletedIncludingOrphans() {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        for (long lsn : new long[]{20, 30, 40}) {
            memory.put(ChainLayout.snapshotKey(lsn), new byte[]{(byte) lsn});
        }
        clock.addAndGet(5 * DAY);
        memory.put(ChainLayout.snapshotKey(25), new byte[]{25});
        chain.snapshotRef(30, 5);
        clock.addAndGet(4 * DAY);

        Result result = retention().sweep(new Reference(5, 30), ChainRetention.NO_PROJECTOR);

        assertEquals(2, result.snapshots());
        assertTrue(!memory.exists(ChainLayout.snapshotKey(10)), "an old snapshot below the newest is deleted");
        assertTrue(!memory.exists(ChainLayout.snapshotKey(20)), "an old orphan below the newest is deleted");
        assertTrue(memory.exists(ChainLayout.snapshotKey(25)), "a young snapshot is kept");
        assertTrue(memory.exists(ChainLayout.snapshotKey(30)), "the newest reference's snapshot is kept");
        assertTrue(memory.exists(ChainLayout.snapshotKey(40)), "a snapshot above the newest reference is kept");
    }

    @Test
    void theNewestReferencesSnapshotIsKeptHoweverOldItIs() {
        memory.put(ChainLayout.snapshotKey(30), new byte[]{30});
        memory.put(ChainLayout.snapshotKey(10), new byte[]{10});
        chain.snapshotRef(30, 1);
        clock.addAndGet(100 * DAY);

        Result result = retention().sweep(new Reference(1, 30), ChainRetention.NO_PROJECTOR);

        assertEquals(1, result.snapshots());
        assertTrue(memory.exists(ChainLayout.snapshotKey(30)));
        assertTrue(!memory.exists(ChainLayout.snapshotKey(10)));
    }

    @Test
    void aVeryLongPrefixIsDeletedInSeveralPages() {
        chain.snapshotRef(10, 1);
        chain.many(2_300, 1);
        chain.snapshotRef(chain.lastLsn(), 2_302);
        long referenceLsn = chain.lastLsn();
        chain.many(3, 1);
        clock.addAndGet(30 * DAY);

        Result result = retention().sweep(new Reference(2_302, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(2_300, result.chainObjects());
        assertEquals(range(2_301, 2_305), remaining());
        long chainListings = store.calls().stream().filter(call -> call.operation() == Operation.LIST
                && call.key().equals(ChainLayout.CHAIN_PREFIX)).count();
        assertTrue(chainListings >= 3, "listed " + chainListings + " pages");
    }

    @Test
    void theSweepStopsListingAtTheFirstObjectItMustKeep() {
        chain.snapshotRef(10, 1);
        chain.many(500, 1);
        clock.addAndGet(30 * DAY);
        chain.many(1_800, 1);
        chain.snapshotRef(chain.lastLsn(), 2_302);
        long referenceLsn = chain.lastLsn();
        chain.many(3, 1);

        Result result = retention().sweep(new Reference(2_302, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(501, result.chainObjects());
        long chainListings = store.calls().stream().filter(call -> call.operation() == Operation.LIST
                && call.key().equals(ChainLayout.CHAIN_PREFIX)).count();
        assertEquals(1, chainListings);
    }

    @Test
    void staleMultipartUploadsAreAbortedThroughTheStore() {
        long referenceLsn = standardChain();
        AtomicReference<String> prefix = new AtomicReference<>();
        AtomicReference<Duration> age = new AtomicReference<>();
        AtomicReference<Instant> at = new AtomicReference<>();
        ChainRetention withRecorder = new ChainRetention(new ForwardingObjectStore(memory) {
            @Override
            public int abortStaleUploads(String abortPrefix, Duration olderThan, Instant now) {
                prefix.set(abortPrefix);
                age.set(olderThan);
                at.set(now);
                return 3;
            }
        }, WEEK, clock::get);

        Result result = withRecorder.sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(3, result.abandonedUploads());
        assertEquals("_nodus/", prefix.get());
        assertEquals(Duration.ofDays(1), age.get());
        assertEquals(Instant.ofEpochMilli(clock.get()), at.get());
    }

    @Test
    void aStoreFailureMidSweepLeavesAValidSuffixAndTheNextSweepFinishes() {
        long referenceLsn = standardChain();
        clock.addAndGet(8 * DAY);
        store.failWhen(call -> call.operation() == Operation.DELETE && call.key().equals(ChainLayout.chainKey(5)), 1,
                Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class,
                () -> retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR));
        assertEquals(range(5, 15), remaining());
        assertEquals(15, new ChainHead(memory, Keyring.empty()).find().orElseThrow().cursor().seq());

        retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR);

        assertEquals(range(10, 15), remaining());
    }

    @Test
    void anUnreadableObjectInTheWalkBackStopsTheSweepBeforeAnythingIsDeleted() {
        long referenceLsn = standardChain();
        memory.put(ChainLayout.chainKey(10), "garbage".getBytes(StandardCharsets.UTF_8));
        clock.addAndGet(8 * DAY);

        assertThrows(ChainFormatException.class,
                () -> retention().sweep(new Reference(11, referenceLsn), ChainRetention.NO_PROJECTOR));

        assertEquals(range(1, 15), remaining());
    }
}
