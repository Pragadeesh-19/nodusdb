package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.error.ShipTimeoutException;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.ship.ShipState.Phase;
import io.nodusdb.ship.ShipperCore.Cadence;
import io.nodusdb.ship.ShipperCore.Next;
import io.nodusdb.ship.ShipperRig.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShipperCoreTest {

    private ShipperRig rig = new ShipperRig();

    @AfterEach
    void closeRig() {
        rig.close();
    }

    private ChainObject object(long seq) {
        return ChainCodec.decode(rig.memory.get(ChainLayout.chainKey(seq)).orElseThrow());
    }

    private ChainObject foreign(long seq, long epoch, ChainHash prev, long firstLsn) {
        return ChainCodec.seal(new ChainHeader(ChainKind.RECORDS, seq, epoch, 99, prev, ChainBuilder.KEY_ID),
                new ChainBody.Records(firstLsn, firstLsn + 1, ChainBuilder.transaction(firstLsn, 1)),
                ChainBuilder.signingKey());
    }

    private Session shippedUpToTheSnapshot() {
        rig.append(3);
        Session session = rig.open(1);
        assertEquals(Cadence.IDLE, rig.run(session).cadence(), session.state().snapshot().lastError());
        return session;
    }

    @Test
    void theFirstStepCommitsTheSnapshotReferenceAsObjectOne() {
        rig.append(3);
        Session session = rig.open(1);
        long snapshotLsn = rig.log.lastLsn();

        Next first = session.core().step();

        assertEquals(Cadence.CONTINUE, first.cadence());
        ChainObject one = object(1);
        assertEquals(ChainKind.SNAPSHOT_REF, one.header().kind());
        assertEquals(session.start().epoch(), one.epoch());
        assertEquals(ChainHash.ZERO, one.header().prev());
        assertEquals(snapshotLsn, ((ChainBody.SnapshotRef) one.body()).lsn());
        ShipState.Snapshot snapshot = session.state().snapshot();
        assertEquals(1, snapshot.chainSeq());
        assertEquals(snapshotLsn, snapshot.shippedLsn());
        assertEquals(1, snapshot.objectsShipped());
        assertEquals(Phase.ACTIVE, snapshot.phase());
    }

    @Test
    void anIdleShipperLeavesTheStoreAlone() {
        Session session = shippedUpToTheSnapshot();
        int calls = rig.store.callCount();

        Next idle = session.core().step();

        assertEquals(Cadence.IDLE, idle.cadence());
        assertEquals(rig.settings.interval().toNanos(), idle.nanos());
        assertEquals(calls, rig.store.callCount());
        assertEquals(Phase.ACTIVE, session.state().phase());
        assertEquals(0, session.state().snapshot().backlogBytes());
    }

    @Test
    void newTransactionsAreShippedInOrderAndProgressIsReported() {
        Session session = shippedUpToTheSnapshot();

        for (int tuples = 1; tuples <= 6; tuples++) {
            rig.append(tuples);
            assertEquals(Cadence.IDLE, rig.run(session).cadence());
            assertEquals(rig.log.lastLsn(), session.state().shippedLsn());
        }

        ChainAudit.Result audit = ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
        assertEquals(7, audit.objects());
        ShipState.Snapshot snapshot = session.state().snapshot();
        assertEquals(7, snapshot.chainSeq());
        assertEquals(7, snapshot.objectsShipped());
        assertEquals(0, snapshot.backlogBytes());
    }

    @Test
    void transactionsAppendedBetweenStepsAreBatchedIntoOneObject() {
        Session session = shippedUpToTheSnapshot();

        rig.append(1);
        rig.append(2);
        rig.append(3);
        rig.run(session);

        assertEquals(2, ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn()).objects());
    }

    @Test
    void aLargeBacklogSplitsAtTransactionBoundariesAndLosesNothing() {
        Session session = shippedUpToTheSnapshot();
        for (int i = 0; i < 60; i++) {
            rig.append(1_000);
        }

        assertEquals(Cadence.IDLE, rig.run(session).cadence());

        ChainAudit.Result audit = ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
        assertTrue(audit.objects() >= 3, "expected the backlog to need several objects, got " + audit.objects());
        for (long seq = 2; seq <= audit.objects(); seq++) {
            assertTrue(object(seq).encoded().length < rig.settings.maxObjectBytes() + (2 << 20));
        }
    }

    @Test
    void aTransientFailureRetriesTheSameBytesWithGrowingBackoff() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        rig.store.failNext(Operation.PUT_IF_ABSENT, 3, Fault.FAIL_BEFORE);

        for (int attempt = 1; attempt <= 3; attempt++) {
            Next next = session.core().step();
            assertEquals(Cadence.BACKOFF, next.cadence());
            assertEquals(rig.settings.backoffNanos(attempt), next.nanos());
            assertEquals(Phase.RETRYING, session.state().phase());
            assertFalse(rig.memory.exists(ChainLayout.chainKey(2)));
        }
        assertEquals(3, session.state().snapshot().consecutiveFailures());

        assertEquals(Cadence.IDLE, rig.run(session).cadence());

        assertEquals(Phase.ACTIVE, session.state().phase());
        assertEquals(0, session.state().snapshot().consecutiveFailures());
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
        assertEquals(4, rig.store.calls().stream().filter(call -> call.operation() == Operation.PUT_IF_ABSENT
                && call.key().equals(ChainLayout.chainKey(2))).count());
    }

    @Test
    void backoffRestartsAfterASuccess() {
        Session session = shippedUpToTheSnapshot();
        rig.append(1);
        rig.store.failNext(Operation.PUT_IF_ABSENT, 2, Fault.FAIL_BEFORE);
        session.core().step();
        session.core().step();
        rig.run(session);
        rig.append(1);
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);

        Next next = session.core().step();

        assertEquals(rig.settings.backoffNanos(1), next.nanos());
    }

    @Test
    void backoffRestartsAfterASuccessEvenWhenMoreDataIsAlreadyWaiting() {
        Session session = shippedUpToTheSnapshot();
        for (int i = 0; i < 60; i++) {
            rig.append(1_000);
        }
        rig.store.failNext(Operation.PUT_IF_ABSENT, 2, Fault.FAIL_BEFORE);
        session.core().step();
        session.core().step();

        Next recovered = session.core().step();
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);
        Next failedAgain = session.core().step();

        assertEquals(Cadence.CONTINUE, recovered.cadence());
        assertEquals(rig.settings.backoffNanos(1), failedAgain.nanos());
    }

    @Test
    void aWriteThatLandedBeforeTheErrorIsRecognisedAndNotDuplicated() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_AFTER);

        Next failed = session.core().step();

        assertEquals(Cadence.BACKOFF, failed.cadence());
        assertTrue(rig.memory.exists(ChainLayout.chainKey(2)));
        assertEquals(Cadence.IDLE, rig.run(session).cadence());
        assertEquals(2, ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn()).objects());
        assertEquals(Phase.ACTIVE, session.state().phase());
        assertEquals(2, session.state().snapshot().chainSeq());
    }

    @Test
    void aConflictIsRetriedWithBackoffAndThenSucceeds() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        rig.store.failNext(Operation.PUT_IF_ABSENT, 2, Fault.CONFLICT);

        Next first = session.core().step();
        Next second = session.core().step();

        assertEquals(rig.settings.backoffNanos(1), first.nanos());
        assertEquals(rig.settings.backoffNanos(2), second.nanos());
        assertTrue(session.state().snapshot().lastError().contains("concurrent write"));
        assertEquals(Cadence.IDLE, rig.run(session).cadence());
        assertEquals(2, ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn()).objects());
    }

    @Test
    void aConflictThatNeverClearsFallsBackToReadingTheObjectAndKeepsRetryingWhenItIsAbsent() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        int retries = rig.settings.conflictRetries();
        rig.store.failNext(Operation.PUT_IF_ABSENT, retries + 1, Fault.CONFLICT);

        for (int i = 0; i < retries; i++) {
            assertEquals(Cadence.BACKOFF, session.core().step().cadence());
        }
        Next exhausted = session.core().step();

        assertEquals(Cadence.BACKOFF, exhausted.cadence());
        assertEquals(Phase.RETRYING, session.state().phase());
        assertTrue(session.state().snapshot().lastError().contains("cannot return it"));
        assertEquals(Cadence.IDLE, rig.run(session).cadence());
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    @Test
    void aConflictThatTurnsOutToBeOurOwnEarlierWriteCountsAsCommitted() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        int retries = rig.settings.conflictRetries();
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_AFTER);
        rig.store.failNext(Operation.PUT_IF_ABSENT, retries + 1, Fault.CONFLICT);

        for (int i = 0; i < retries + 1; i++) {
            assertEquals(Cadence.BACKOFF, session.core().step().cadence());
        }
        Next settled = session.core().step();

        assertEquals(Cadence.CONTINUE, settled.cadence());
        assertEquals(Phase.ACTIVE, session.state().phase());
        assertEquals(2, ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn()).objects());
    }

    @Test
    void anObjectFromANewerEpochFencesTheWriterAndIsLeftUntouched() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        ChainObject theirs = foreign(2, session.start().epoch() + 5, object(1).digest(), rig.log.lastLsn() - 2);
        rig.memory.put(ChainLayout.chainKey(2), theirs.encoded());

        Next next = session.core().step();

        assertEquals(Cadence.STOP, next.cadence());
        assertEquals(Phase.FENCED, session.state().phase());
        String reason = session.state().snapshot().lastError();
        assertTrue(reason.contains("another writer in epoch " + (session.start().epoch() + 5)), reason);
        assertEquals(List.of(theirs.encoded().length), List.of(object(2).encoded().length));
        assertEquals(Cadence.STOP, session.core().step().cadence());
    }

    @Test
    void anObjectFromAnOlderEpochFencesTheWriterToo() {
        rig.append(1);
        Session first = rig.open(1);
        rig.run(first);
        rig.append(1);
        Session second = rig.open(2);
        rig.append(2);
        ChainObject theirs = foreign(2, 0, object(1).digest(), rig.log.lastLsn() - 2);
        rig.memory.put(ChainLayout.chainKey(2), theirs.encoded());

        Next next = second.core().step();

        assertEquals(Cadence.STOP, next.cadence());
        assertEquals(Phase.FENCED, second.state().phase());
        assertTrue(second.state().snapshot().lastError().contains("in epoch 0"));
    }

    @Test
    void anUnreadableObjectInOurSlotFencesTheWriterWithoutAnEpoch() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        rig.memory.put(ChainLayout.chainKey(2), "garbage".getBytes(StandardCharsets.UTF_8));

        Next next = session.core().step();

        assertEquals(Cadence.STOP, next.cadence());
        String reason = session.state().snapshot().lastError();
        assertTrue(reason.contains("was written by another writer; this writer is in epoch"), reason);
    }

    @Test
    void aNewerEpochClaimFencesTheWriterBeforeItWritesAnything() {
        rig.append(2);
        Session session = rig.open(1);
        rig.memory.put(ChainLayout.epochKey(session.start().epoch() + 1), "{}".getBytes(StandardCharsets.UTF_8));
        int writes = rig.memory.size();

        Next next = session.core().step();

        assertEquals(Cadence.STOP, next.cadence());
        assertEquals(Phase.FENCED, session.state().phase());
        assertTrue(session.state().snapshot().lastError().contains("superseded"));
        assertEquals(writes, rig.memory.size());
        assertFalse(rig.memory.exists(ChainLayout.chainKey(1)));
    }

    @Test
    void aFencedWriterTouchesNothingOnLaterSteps() {
        rig.append(2);
        Session session = rig.open(1);
        rig.memory.put(ChainLayout.epochKey(session.start().epoch() + 1), "{}".getBytes(StandardCharsets.UTF_8));
        session.core().step();
        int calls = rig.store.callCount();

        for (int i = 0; i < 5; i++) {
            assertEquals(Cadence.STOP, session.core().step().cadence());
        }

        assertEquals(calls, rig.store.callCount());
    }

    @Test
    void aRejectedWriteFailsTheShipperSlowlyAndItRecoversWhenTheStoreDoes() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        long lsn = rig.log.lastLsn();
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FATAL);

        Next next = session.core().step();

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(rig.settings.failedRetry().toNanos(), next.nanos());
        assertEquals(Phase.FAILED, session.state().phase());
        ShipTimeoutException refused = assertThrows(ShipTimeoutException.class,
                () -> session.state().awaitShipped(session.start().epoch(), lsn, 5_000_000_000L));
        assertTrue(refused.getMessage().contains("shipping has failed"), refused.getMessage());

        assertEquals(Cadence.IDLE, rig.run(session).cadence());
        assertEquals(Phase.ACTIVE, session.state().phase());
        session.state().awaitShipped(session.start().epoch(), lsn, 5_000_000_000L);
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    @Test
    void expiredCredentialsFailTheShipperLikeAnyRejection() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.EXPIRED);

        Next next = session.core().step();

        assertEquals(rig.settings.failedRetry().toNanos(), next.nanos());
        assertEquals(Phase.FAILED, session.state().phase());
        assertEquals(Cadence.IDLE, rig.run(session).cadence());
    }

    @Test
    void aFailingFenceCheckIsRetriedWithoutWritingAnything() {
        rig.append(2);
        Session session = rig.open(1);
        rig.store.failNext(Operation.HEAD, Fault.FAIL_BEFORE);

        Next next = session.core().step();

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(Phase.RETRYING, session.state().phase());
        assertFalse(rig.memory.exists(ChainLayout.chainKey(1)));
        assertEquals(Cadence.IDLE, rig.run(session).cadence());
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    @Test
    void theUnshippedBacklogIncludesTheObjectThatIsStillBeingRetried() {
        Session session = shippedUpToTheSnapshot();
        rig.append(50);
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);

        session.core().step();

        long backlog = session.state().snapshot().backlogBytes();
        assertTrue(backlog >= 50 * 20, "backlog was " + backlog);
        rig.run(session);
        assertEquals(0, session.state().snapshot().backlogBytes());
    }

    @Test
    void aBacklogPastTheCapRefusesWritesAndResumesOnceShippingCatchesUp() {
        rig.close();
        rig = new ShipperRig(new MemoryObjectStore(), ShipSettings.defaults().withBacklogCapBytes(1 << 20),
                SyncMode.SYNC);
        Session session = shippedUpToTheSnapshot();
        rig.store.failNext(Operation.PUT_IF_ABSENT, 3, Fault.FAIL_BEFORE);
        for (int i = 0; i < 40; i++) {
            rig.append(1_000);
        }

        session.core().step();

        assertTrue(session.state().refusal() != null, "the cap must refuse writes");
        assertEquals(Cadence.IDLE, rig.runPastBackoffs(session, 5).cadence());
        assertEquals(null, session.state().refusal());
        assertTrue(session.state().drainEvents().stream().anyMatch(event -> event.contains("writes are refused")));
    }

    @Test
    void aLogBatchThatDoesNotFollowTheChainFailsTheShipperInsteadOfShippingAGap() {
        rig.append(3);
        Session session = rig.openSkipping(1, 3);
        rig.run(session);
        rig.append(2);
        rig.append(2);

        Next next = session.core().step();

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(rig.settings.failedRetry().toNanos(), next.nanos());
        assertEquals(Phase.FAILED, session.state().phase());
        String reason = session.state().snapshot().lastError();
        assertTrue(reason.contains("the chain ends at LSN"), reason);
        assertEquals(1, ChainAudit.chainKeys(rig.memory).size());
    }

    @Test
    void aDamagedLogFailsTheShipperWithoutTouchingTheChain() {
        Session session = shippedUpToTheSnapshot();
        rig.append(2);
        String last = rig.disk.list().stream().filter(name -> name.endsWith(".nlog")).sorted()
                .reduce((older, newer) -> newer).orElseThrow();
        byte[] content = rig.disk.contentOf(last);
        content[content.length - 1] ^= 0x5A;
        rig.disk.replaceContent(last, content);

        Next next = session.core().step();

        assertEquals(Cadence.BACKOFF, next.cadence());
        assertEquals(Phase.FAILED, session.state().phase());
        assertTrue(session.state().snapshot().lastError().startsWith("reading the local log failed"));
        assertEquals(1, ChainAudit.chainKeys(rig.memory).size());
    }

    @Test
    void drainShipsEverythingAndReportsWhetherItReachedAnIdleLog() {
        Session session = shippedUpToTheSnapshot();
        rig.append(4);
        rig.append(4);

        assertTrue(session.core().drain());
        assertEquals(rig.log.lastLsn(), session.state().shippedLsn());

        rig.append(2);
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);
        assertFalse(session.core().drain());
        assertTrue(session.core().drain());
    }

    @Test
    void everyCommittedObjectIsKeptInTheRingForTheProjector() {
        Session session = shippedUpToTheSnapshot();
        rig.append(3);
        rig.append(2);
        rig.run(session);

        ChainRing ring = session.state().ring();

        assertEquals(2, ring.size());
        assertEquals(object(1).digest(), ring.get(1).orElseThrow().digest());
        assertEquals(object(2).digest(), ring.get(2).orElseThrow().digest());
        assertEquals(ChainKind.SNAPSHOT_REF, ring.get(1).orElseThrow().header().kind());
    }

    @Test
    void anObjectThatFailedToCommitIsNotInTheRing() {
        Session session = shippedUpToTheSnapshot();
        rig.append(3);
        rig.store.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);

        session.core().step();

        assertEquals(1, session.state().ring().size());
        assertTrue(session.state().ring().get(2).isEmpty());
    }

    @Test
    void aReferenceOfferedWhileIdleIsCommittedAsTheNextObject() {
        Session session = shippedUpToTheSnapshot();
        long shipped = session.state().shippedLsn();
        ChainBody.SnapshotRef reference = rig.referenceAt(shipped);

        session.state().offerReference(reference);
        Next next = rig.run(session);

        assertEquals(Cadence.IDLE, next.cadence());
        ChainObject committed = object(2);
        assertEquals(ChainKind.SNAPSHOT_REF, committed.header().kind());
        assertEquals(reference, committed.body());
        assertEquals(object(1).digest(), committed.header().prev());
        assertEquals(shipped, session.state().shippedLsn());
        ShipState.ReferenceStatus status = session.state().snapshot().references();
        assertEquals(2, status.seq());
        assertEquals(shipped, status.lsn());
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    @Test
    void theStartingReferenceIsReportedAsTheNewestCommittedOne() {
        rig.append(3);
        Session session = rig.open(1);

        rig.run(session);

        ShipState.ReferenceStatus status = session.state().snapshot().references();
        assertEquals(1, status.seq());
        assertEquals(session.start().firstReference().lsn(), status.lsn());
    }

    @Test
    void aWaitingReferenceIsCommittedBeforeRecordsThatArriveWithIt() {
        Session session = shippedUpToTheSnapshot();
        long snapshotLsn = rig.log.lastLsn();
        rig.append(4);

        session.state().offerReference(rig.referenceAt(snapshotLsn));
        rig.run(session);

        assertEquals(ChainKind.SNAPSHOT_REF, object(2).header().kind());
        assertEquals(ChainKind.RECORDS, object(3).header().kind());
        assertEquals(rig.log.lastLsn(), session.state().shippedLsn());
        ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
    }

    @Test
    void aReferenceAheadOfTheShippedLsnDoesNotMakeThoseRecordsLookShipped() {
        Session session = shippedUpToTheSnapshot();
        long shipped = session.state().shippedLsn();
        long ahead = rig.append(3);
        session.state().offerReference(rig.referenceAt(ahead));

        Next next = session.core().step();

        assertEquals(Cadence.CONTINUE, next.cadence());
        assertEquals(ChainKind.SNAPSHOT_REF, object(2).header().kind());
        assertEquals(shipped, session.state().shippedLsn());
        assertEquals(ahead, session.state().snapshot().references().lsn());
        rig.run(session);
        assertEquals(ahead, session.state().shippedLsn());
    }

    @Test
    void aReferenceThatFailsToCommitIsRetriedWithTheSameBytes() {
        Session session = shippedUpToTheSnapshot();
        ChainBody.SnapshotRef reference = rig.referenceAt(rig.log.lastLsn());
        session.state().offerReference(reference);
        rig.store.failNext(Operation.PUT_IF_ABSENT, 2, Fault.FAIL_BEFORE);

        assertEquals(Cadence.BACKOFF, session.core().step().cadence());
        assertEquals(Cadence.BACKOFF, session.core().step().cadence());
        assertEquals(null, session.state().takeReference());
        rig.run(session);

        assertEquals(reference, object(2).body());
        assertEquals(2, session.state().snapshot().references().seq());
    }

    @Test
    void aNewerOfferedReferenceReplacesAnOlderOneThatWasNotYetTaken() {
        Session session = shippedUpToTheSnapshot();
        long lsn = rig.log.lastLsn();
        rig.append(2);
        session.state().offerReference(rig.referenceAt(lsn));
        session.state().offerReference(rig.referenceAt(lsn + 3));

        rig.run(session);

        assertEquals(lsn + 3, ((ChainBody.SnapshotRef) object(2).body()).lsn());
        assertEquals(3, ChainAudit.chainKeys(rig.memory).size());
    }

    @Test
    void aShipperThatOpensOnAnExistingChainContinuesItInANewEpoch() {
        Session first = shippedUpToTheSnapshot();
        rig.append(2);
        rig.run(first);
        rig.append(3);

        Session second = rig.open(2);
        rig.run(second);

        assertFalse(second.start().startsChain());
        assertEquals(first.start().epoch() + 1, second.start().epoch());
        ChainAudit.Result audit = ChainAudit.verify(rig.memory, rig.disk, rig.log.lastLsn());
        assertEquals(List.of(first.start().epoch(), second.start().epoch()), audit.epochs());
    }

    @Test
    void anAsyncLogIsForcedBeforeItsRecordsAreShipped() {
        rig.close();
        rig = new ShipperRig(new MemoryObjectStore(), ShipSettings.defaults(), SyncMode.ASYNC);
        Session session = shippedUpToTheSnapshot();

        long lsn = rig.append(5);
        assertTrue(rig.log.durableLsn() < lsn || rig.log.durableLsn() >= lsn);
        rig.run(session);

        assertTrue(rig.log.durableLsn() >= lsn);
        assertEquals(lsn, session.state().shippedLsn());
        ChainAudit.verify(rig.memory, rig.disk, lsn);
    }

    @Test
    void twoWritersStartedTogetherLeaveOnlyTheNewestEpochShipping() throws Exception {
        rig.close();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 30; round++) {
                MemoryObjectStore shared = new MemoryObjectStore();
                ShipperRig a = new ShipperRig(shared, ShipSettings.defaults(), SyncMode.SYNC);
                ShipperRig b = new ShipperRig(shared, ShipSettings.defaults(), SyncMode.SYNC);
                try {
                    Session sa = a.open(1);
                    Session sb = b.open(2);
                    CountDownLatch go = new CountDownLatch(1);
                    List<Future<Next>> runs = new ArrayList<>();
                    runs.add(pool.submit(() -> {
                        go.await();
                        return a.run(sa);
                    }));
                    runs.add(pool.submit(() -> {
                        go.await();
                        return b.run(sb);
                    }));
                    go.countDown();
                    for (Future<Next> run : runs) {
                        run.get(30, TimeUnit.SECONDS);
                    }

                    Session loser = sa.start().epoch() < sb.start().epoch() ? sa : sb;
                    Session winner = loser == sa ? sb : sa;
                    assertEquals(Phase.FENCED, loser.state().phase(), "round " + round);
                    assertEquals(Phase.ACTIVE, winner.state().phase(), "round " + round);
                    assertEquals(1, ChainAudit.chainKeys(shared).size(), "round " + round);
                } finally {
                    a.close();
                    b.close();
                }
            }
        } finally {
            pool.shutdownNow();
            rig = new ShipperRig();
        }
    }
}
