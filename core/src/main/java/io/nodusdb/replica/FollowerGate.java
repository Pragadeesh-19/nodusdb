package io.nodusdb.replica;

import io.nodusdb.error.StaleReadException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.replica.FollowerState.Phase;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public final class FollowerGate {

    private static final long NO_BOUND = -1;
    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

    private final GraphKernel kernel;
    private final FollowerState state;
    private final long readWaitNanos;
    private final long maxStalenessNanos;
    private final LongSupplier nanoClock;

    public FollowerGate(GraphKernel kernel, FollowerState state, FollowerConfig.Follow follow,
                        LongSupplier nanoClock) {
        this.kernel = kernel;
        this.state = state;
        this.readWaitNanos = follow.readWait().toNanos();
        this.maxStalenessNanos = follow.hasStalenessBound() ? follow.maxStaleness().toNanos() : NO_BOUND;
        this.nanoClock = nanoClock;
    }

    public void requireReadable() {
        FollowerState.Snapshot snapshot = state.snapshot(nanoClock.getAsLong());
        if (snapshot.phase() == Phase.CLOSED) {
            throw new IllegalStateException("the follower is closed");
        }
        if (snapshot.phase() == Phase.BOOTSTRAPPING) {
            throw new StaleReadException("the follower has not loaded a snapshot yet"
                    + (snapshot.lastError().isEmpty() ? "" : "; the last attempt failed: " + snapshot.lastError()));
        }
        if (maxStalenessNanos == NO_BOUND) {
            return;
        }
        long since = snapshot.nanosSinceConfirmed();
        if (since == FollowerState.NEVER_CONFIRMED) {
            throw new StaleReadException("the follower has not yet confirmed the chain head, and the staleness "
                    + "bound is " + maxStalenessNanos / NANOS_PER_MILLI + " ms");
        }
        if (since > maxStalenessNanos) {
            throw new StaleReadException("the follower last confirmed the chain head " + since / NANOS_PER_MILLI
                    + " ms ago, beyond the staleness bound of " + maxStalenessNanos / NANOS_PER_MILLI + " ms");
        }
    }

    public void awaitToken(Token token) {
        long deadline = System.nanoTime() + readWaitNanos;
        long seen = state.version();
        while (!reached(token) && !state.terminal()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            seen = state.awaitChange(seen, remaining);
        }
    }

    private boolean reached(Token token) {
        long epoch = kernel.epoch();
        if (token.epoch() < epoch) {
            return true;
        }
        return token.epoch() == epoch && token.lsn() <= kernel.appliedLsn();
    }
}
