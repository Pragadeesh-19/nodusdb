package io.nodusdb.replica;

import io.nodusdb.chain.ChainFormatException;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.Keyring;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.TransientStoreException;
import io.nodusdb.replica.FollowerState.Phase;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.ship.ChainHead;
import io.nodusdb.ship.Pace;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public final class FollowerCore {

    static final String FELL_BEHIND = "fell behind retention; restart to rebuild";

    private static final long GAP_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long MAX_BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int MAX_BACKOFF_SHIFT = 20;

    private final ObjectStore store;
    private final Keyring keyring;
    private final ChainReplay replay;
    private final ReplicaSink sink;
    private final AntiRollbackMarker marker;
    private final FollowerState state;
    private final long pollNanos;
    private final LongSupplier nanoClock;
    private ReplayPosition position;
    private int transientFailures;
    private boolean gapChecked;
    private long lastGapCheckNanos;

    public FollowerCore(ObjectStore store, Keyring keyring, ReplicaSink sink, AntiRollbackMarker marker,
                        FollowerState state, FollowerConfig.Follow follow, Path scratch, LongSupplier nanoClock) {
        this.store = store;
        this.keyring = keyring;
        this.sink = sink;
        this.marker = marker;
        this.state = state;
        this.pollNanos = follow.pollInterval().toNanos();
        this.nanoClock = nanoClock;
        ChainFetch fetch = new ChainFetch(store, keyring, Trust.REQUIRED);
        this.replay = new ChainReplay(store, fetch, new SnapshotDownloader(store, follow.downloadParallelism()),
                scratch);
    }

    public Pace step() {
        Phase phase = state.phase();
        if (phase == Phase.STALLED || phase == Phase.CLOSED) {
            return Pace.stop();
        }
        try {
            Pace pace = phase == Phase.BOOTSTRAPPING ? bootstrap() : follow();
            transientFailures = 0;
            return pace;
        } catch (ChainTrustException | ChainFormatException refused) {
            return stall(refused.getMessage());
        } catch (ChainGapException gap) {
            return stall(FELL_BEHIND + " (" + gap.getMessage() + ")");
        } catch (MemoryLimitExceededException limit) {
            return stall("the memory limit was reached: " + limit.getMessage());
        } catch (TransientStoreException failure) {
            return retry(failure.getMessage());
        } catch (ObjectStoreException failure) {
            return stall("the object store refused the request: " + failure.getMessage());
        } catch (IOException | UncheckedIOException failure) {
            return retry("local disk or network failure: " + failure.getMessage());
        } catch (RuntimeException unexpected) {
            return stall("unexpected failure: " + unexpected);
        }
    }

    private Pace bootstrap() throws IOException {
        requireHeadNotBehindMarker();
        Optional<ReplayPosition> started = replay.bootstrap(sink);
        if (started.isEmpty()) {
            state.waitingForSnapshot();
            return Pace.idle(pollNanos);
        }
        position = started.get();
        state.bootstrapped(position.snapshotLsn(), position.appliedLsn(), position.cursor().seq(),
                position.cursor().epoch());
        observeMarker();
        return Pace.immediately();
    }

    private void requireHeadNotBehindMarker() {
        if (marker.remembered().isEmpty()) {
            return;
        }
        Optional<ChainHead.Found> head = new ChainHead(store, keyring, Trust.REQUIRED).find();
        if (head.isEmpty()) {
            marker.requireBucketMayBeEmpty();
            return;
        }
        marker.requireHead(head.get().object().seq(), head.get().object().digest());
    }

    private Pace follow() {
        Optional<ChainObject> next = replay.advance(position, sink, this::guard);
        if (next.isPresent()) {
            ChainObject object = next.get();
            state.applied(object.seq(), position.appliedLsn(), object.epoch(), object.encoded().length);
            observeMarker();
            return Pace.immediately();
        }
        requireNoGapIfDue();
        state.current(nanoClock.getAsLong());
        observeMarker();
        return Pace.idle(pollNanos);
    }

    private void guard(ChainObject object) {
        marker.requireObject(object.seq(), object.digest());
    }

    private void requireNoGapIfDue() {
        long now = nanoClock.getAsLong();
        if (gapChecked && now - lastGapCheckNanos < GAP_CHECK_INTERVAL_NANOS) {
            return;
        }
        replay.requireNoGap(position);
        gapChecked = true;
        lastGapCheckNanos = now;
    }

    private void observeMarker() {
        try {
            marker.observe(new MarkerPosition(position.cursor().seq(), position.cursor().digest(),
                    position.cursor().epoch(), position.appliedLsn()), nanoClock.getAsLong());
        } catch (UncheckedIOException failure) {
            state.note("could not write the rollback marker: " + failure.getMessage());
        }
    }

    private Pace retry(String reason) {
        transientFailures++;
        state.lagging(reason);
        int shift = Math.min(transientFailures - 1, MAX_BACKOFF_SHIFT);
        long scaled = pollNanos << shift;
        return Pace.backoff(scaled < 0 || scaled > MAX_BACKOFF_NANOS ? MAX_BACKOFF_NANOS : scaled);
    }

    private Pace stall(String reason) {
        state.stalled(reason);
        return Pace.stop();
    }
}
