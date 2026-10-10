package io.nodusdb.replica;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.replica.FollowerState.Phase;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.Pace;
import io.nodusdb.ship.Pace.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FollowerCoreTest {

    private static final long DAY = 86_400_000L;
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final Duration POLL = Duration.ofMillis(100);

    @TempDir
    Path root;

    private final AtomicLong storeClock = new AtomicLong(1_000 * DAY);
    private final AtomicLong nanos = new AtomicLong(10 * SECOND);
    private final MemoryObjectStore bucket = new MemoryObjectStore(storeClock::get);
    private final FaultyObjectStore store = new FaultyObjectStore(bucket);
    private final ChainBuilder chain = new ChainBuilder(bucket, 1).epoch(2);
    private final RecordingSink sink = new RecordingSink();
    private final FollowerState state = new FollowerState();
    private AntiRollbackMarker marker = AntiRollbackMarker.disabled();

    private FollowerCore core() {
        FollowerConfig.Follow follow = new FollowerConfig.Follow(POLL, Duration.ofSeconds(1), null, 2, null);
        return new FollowerCore(store, ChainBuilder.keyring(), sink, marker, state, follow, root, nanos::get);
    }

    private static void assertPace(Kind kind, Pace pace) {
        assertEquals(kind, pace.kind(), pace.toString());
    }

    private static Pace runUntilIdle(FollowerCore core) {
        for (int i = 0; i < 1_000; i++) {
            Pace pace = core.step();
            if (pace.kind() != Kind.CONTINUE) {
                return pace;
            }
        }
        throw new AssertionError("the follower never went idle");
    }

    @Test
    void anEmptyBucketKeepsTheFollowerWaitingAndIdle() {
        FollowerCore core = core();

        Pace pace = core.step();

        assertPace(Kind.IDLE, pace);
        assertEquals(POLL.toNanos(), pace.nanos());
        assertEquals(Phase.BOOTSTRAPPING, state.phase());
        assertEquals(1, state.recentEvents().size());
    }

    @Test
    void whenTheWriterShipsASnapshotTheFollowerBootstrapsAndThenDrainsTheChain() {
        FollowerCore core = core();
        core.step();
        chain.snapshotRef(10, 1);
        chain.many(3, 1);

        Pace first = core.step();

        assertPace(Kind.CONTINUE, first);
        assertEquals(Phase.CATCHING_UP, state.phase());
        assertEquals(List.of(10L), sink.loaded);

        Pace last = runUntilIdle(core);

        assertPace(Kind.IDLE, last);
        FollowerState.Snapshot snapshot = state.snapshot(nanos.get());
        assertEquals(Phase.CURRENT, snapshot.phase());
        assertEquals(chain.lastLsn(), snapshot.appliedLsn());
        assertEquals(chain.seq(), snapshot.chainSeq());
        assertEquals(10, snapshot.snapshotLsn());
        assertEquals(3, snapshot.objectsApplied());
        assertEquals(0, snapshot.nanosSinceConfirmed());
        assertEquals(chain.lastLsn(), sink.lastAppliedLsn());
    }

    @Test
    void aNewObjectAfterTheFollowerIsCurrentIsAppliedAndTheFollowerSettlesAgain() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        assertEquals(Phase.CURRENT, state.phase());

        chain.many(2, 1);
        nanos.addAndGet(SECOND / 2);
        Pace pace = core.step();

        assertPace(Kind.CONTINUE, pace);
        assertEquals(Phase.CATCHING_UP, state.phase());
        runUntilIdle(core);
        assertEquals(Phase.CURRENT, state.phase());
        assertEquals(chain.lastLsn(), state.snapshot(nanos.get()).appliedLsn());
    }

    @Test
    void anIdleFollowerReportsHowLongAgoItConfirmedTheHead() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);

        nanos.addAndGet(4 * SECOND);
        core.step();

        assertEquals(0, state.snapshot(nanos.get()).nanosSinceConfirmed());
        nanos.addAndGet(3 * SECOND);
        assertEquals(3 * SECOND, state.snapshot(nanos.get()).nanosSinceConfirmed());
    }

    @Test
    void aTransientStoreFailureWhileFollowingMakesTheFollowerLaggingWithGrowingBackoff() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        chain.many(1, 1);
        store.failNext(Operation.GET, 3, Fault.FAIL_BEFORE);

        Pace first = core.step();
        Pace second = core.step();
        Pace third = core.step();

        assertPace(Kind.BACKOFF, first);
        assertEquals(Phase.LAGGING, state.phase());
        assertTrue(second.nanos() > first.nanos(), second + " after " + first);
        assertTrue(third.nanos() > second.nanos(), third + " after " + second);
        assertEquals(3, state.snapshot(nanos.get()).consecutiveFailures());
    }

    @Test
    void theFollowerRecoversFromLaggingAndKeepsItsPosition() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        long before = state.snapshot(nanos.get()).appliedLsn();
        chain.many(1, 1);
        store.failNext(Operation.GET, 2, Fault.FAIL_BEFORE);
        core.step();
        core.step();
        assertEquals(Phase.LAGGING, state.phase());

        runUntilIdle(core);

        FollowerState.Snapshot snapshot = state.snapshot(nanos.get());
        assertEquals(Phase.CURRENT, snapshot.phase());
        assertEquals(0, snapshot.consecutiveFailures());
        assertTrue(snapshot.appliedLsn() > before);
    }

    @Test
    void aTransientFailureWhileBootstrappingKeepsTheFollowerBootstrappingAndRetries() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        store.failNext(Operation.LIST, 2, Fault.FAIL_BEFORE);
        FollowerCore core = core();

        Pace pace = core.step();

        assertPace(Kind.BACKOFF, pace);
        assertEquals(Phase.BOOTSTRAPPING, state.phase());
        assertEquals(1, state.snapshot(nanos.get()).consecutiveFailures());
        for (int i = 0; i < 50 && state.phase() != Phase.CURRENT; i++) {
            core.step();
        }
        assertEquals(Phase.CURRENT, state.phase());
    }

    @Test
    void aForgedObjectStallsTheFollowerForGoodWithTheReasonAndStopsTouchingTheStore() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        long applied = state.snapshot(nanos.get()).appliedLsn();
        ChainHash previous = chain.digest();
        long seq = chain.seq() + 1;
        SigningKey stranger = new SigningKey(ChainBuilder.KEY_ID, KeyFiles.generate().getPrivate());
        ChainBody.Records body = new ChainBody.Records(applied + 1, applied + 2,
                ChainBuilder.transaction(applied + 1, 1));
        ChainObject forged = ChainCodec.seal(new ChainHeader(ChainKind.RECORDS, seq, 2, 1, previous,
                ChainBuilder.KEY_ID), body, stranger);
        bucket.put(ChainLayout.chainKey(seq), forged.encoded());

        Pace pace = core.step();

        assertPace(Kind.STOP, pace);
        FollowerState.Snapshot snapshot = state.snapshot(nanos.get());
        assertEquals(Phase.STALLED, snapshot.phase());
        assertFalse(snapshot.stallReason().isEmpty());
        assertEquals(applied, snapshot.appliedLsn());
        int calls = store.callCount();
        assertPace(Kind.STOP, core.step());
        assertEquals(calls, store.callCount());
    }

    @Test
    void anObjectThatCannotBeDecodedStallsTheFollower() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        bucket.put(ChainLayout.chainKey(chain.seq() + 1), new byte[] {1, 2, 3});

        assertPace(Kind.STOP, core.step());

        assertEquals(Phase.STALLED, state.phase());
    }

    @Test
    void aBucketBehindTheRememberedPositionIsARollbackRefusedBeforeAnythingIsLoaded() throws IOException {
        Path stateDirectory = root.resolve("state");
        MarkerFile.write(stateDirectory.resolve(AntiRollbackMarker.FILE_NAME),
                new MarkerPosition(50, ChainHash.sha256(new byte[] {1}), 2, 5_000));
        marker = AntiRollbackMarker.in(stateDirectory);
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        FollowerCore core = core();

        assertPace(Kind.STOP, core.step());

        assertEquals(Phase.STALLED, state.phase());
        assertTrue(state.snapshot(nanos.get()).stallReason().contains("rolled back"));
        assertEquals(List.of(), sink.loaded);
    }

    @Test
    void anEmptiedBucketIsARollbackOnceSomethingWasRemembered() throws IOException {
        Path stateDirectory = root.resolve("state");
        MarkerFile.write(stateDirectory.resolve(AntiRollbackMarker.FILE_NAME),
                new MarkerPosition(5, ChainHash.sha256(new byte[] {1}), 2, 500));
        marker = AntiRollbackMarker.in(stateDirectory);
        FollowerCore core = core();

        assertPace(Kind.STOP, core.step());

        assertEquals(Phase.STALLED, state.phase());
    }

    @Test
    void anObjectAtTheRememberedSeqWithAnotherDigestIsAForkRefusedBeforeItIsApplied() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        Path stateDirectory = root.resolve("state");
        MarkerFile.write(stateDirectory.resolve(AntiRollbackMarker.FILE_NAME),
                new MarkerPosition(3, ChainHash.sha256(new byte[] {7}), 2, 12));
        marker = AntiRollbackMarker.in(stateDirectory);
        FollowerCore core = core();

        runUntilStopped(core);

        FollowerState.Snapshot snapshot = state.snapshot(nanos.get());
        assertEquals(Phase.STALLED, snapshot.phase());
        assertTrue(snapshot.stallReason().contains("fork"), snapshot.stallReason());
        assertTrue(sink.lastAppliedLsn() < chain.lastLsn());
    }

    @Test
    void theMarkerFollowsTheAppliedPositionAndIsFlushedOnDemand() throws IOException {
        Path stateDirectory = root.resolve("state");
        marker = AntiRollbackMarker.in(stateDirectory);
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        FollowerCore core = core();

        runUntilIdle(core);
        marker.flush();

        MarkerPosition remembered = AntiRollbackMarker.in(stateDirectory).remembered().orElseThrow();
        assertEquals(chain.seq(), remembered.seq());
        assertEquals(chain.digest(), remembered.digest());
        assertEquals(chain.lastLsn(), remembered.lsn());
    }

    @Test
    void anObjectDeletedByRetentionWhileLaterOnesExistStallsTheFollowerOnTheNextGapCheck() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        long firstMissing = chain.seq() + 1;
        chain.many(3, 1);
        bucket.delete(ChainLayout.chainKey(firstMissing));

        nanos.addAndGet(2 * SECOND);
        Pace pace = core.step();

        assertPace(Kind.STOP, pace);
        FollowerState.Snapshot snapshot = state.snapshot(nanos.get());
        assertEquals(Phase.STALLED, snapshot.phase());
        assertTrue(snapshot.stallReason().contains("fell behind retention"), snapshot.stallReason());
        assertTrue(snapshot.stallReason().contains("restart"), snapshot.stallReason());
    }

    @Test
    void theGapCheckListsTheStoreAtMostOncePerSecondWhileIdle() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        long listsBefore = store.count(Operation.LIST);

        for (int i = 0; i < 8; i++) {
            nanos.addAndGet(POLL.toNanos());
            core.step();
        }

        assertEquals(0, store.count(Operation.LIST) - listsBefore);
        nanos.addAndGet(SECOND);
        core.step();
        assertEquals(1, store.count(Operation.LIST) - listsBefore);
    }

    @Test
    void anUnexpectedFailureWhileApplyingStallsTheFollowerWithTheCause() {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        sink.failOnApply = new IllegalStateException("the graph state is inconsistent");
        FollowerCore core = core();

        runUntilStopped(core);

        FollowerState.Snapshot snapshot = state.snapshot(nanos.get());
        assertEquals(Phase.STALLED, snapshot.phase());
        assertTrue(snapshot.stallReason().contains("inconsistent"), snapshot.stallReason());
    }

    @Test
    void fatalStoreErrorsAndExpiredCredentialsStallTheFollower() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        chain.many(1, 1);
        store.failNext(Operation.GET, Fault.EXPIRED);

        assertPace(Kind.STOP, core.step());

        assertEquals(Phase.STALLED, state.phase());
    }

    @Test
    void aClosedFollowerDoesNothing() {
        state.closed();
        FollowerCore core = core();

        assertPace(Kind.STOP, core.step());
        assertEquals(0, store.callCount());
    }

    @Test
    void anIdleFollowerKeepsItsPositionAcrossPolls() {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        FollowerCore core = core();
        runUntilIdle(core);
        FollowerState.Snapshot before = state.snapshot(nanos.get());

        for (int i = 0; i < 5; i++) {
            nanos.addAndGet(POLL.toNanos());
            core.step();
        }

        FollowerState.Snapshot after = state.snapshot(nanos.get());
        assertEquals(before.appliedLsn(), after.appliedLsn());
        assertEquals(before.chainSeq(), after.chainSeq());
        assertEquals(before.objectsApplied(), after.objectsApplied());
    }

    private static void runUntilStopped(FollowerCore core) {
        for (int i = 0; i < 1_000; i++) {
            if (core.step().kind() == Kind.STOP) {
                return;
            }
        }
        throw new AssertionError("the follower never stopped");
    }
}
