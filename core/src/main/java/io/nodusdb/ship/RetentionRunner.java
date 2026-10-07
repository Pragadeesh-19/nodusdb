package io.nodusdb.ship;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public final class RetentionRunner implements AutoCloseable {

    private static final long JOIN_MILLIS = 10_000;
    private static final long INTERRUPTED_JOIN_MILLIS = 2_000;

    private final ChainRetention retention;
    private final ShipState state;
    private final LongSupplier projectedSeq;
    private final long intervalNanos;
    private final Object lock = new Object();
    private final Thread thread;
    private boolean closing;

    private RetentionRunner(ChainRetention retention, ShipState state, LongSupplier projectedSeq, Duration interval,
                            String name) {
        this.retention = retention;
        this.state = state;
        this.projectedSeq = projectedSeq;
        this.intervalNanos = interval.toNanos();
        this.thread = new Thread(this::run, name);
        this.thread.setDaemon(true);
    }

    public static RetentionRunner start(ChainRetention retention, ShipState state, LongSupplier projectedSeq,
                                        Duration interval, String name) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("the retention interval must be positive");
        }
        RetentionRunner runner = new RetentionRunner(retention, state, projectedSeq, interval, name);
        runner.thread.start();
        return runner;
    }

    @Override
    public void close() {
        synchronized (lock) {
            closing = true;
            lock.notifyAll();
        }
        join(JOIN_MILLIS);
        if (thread.isAlive()) {
            thread.interrupt();
            join(INTERRUPTED_JOIN_MILLIS);
        }
    }

    private void join(long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        while (true) {
            sweepOnce();
            if (!sleep()) {
                return;
            }
        }
    }

    private void sweepOnce() {
        ShipState.ReferenceStatus newest = state.snapshot().references();
        if (newest.seq() == 0) {
            return;
        }
        try {
            ChainRetention.Result result = retention.sweep(new ChainRetention.Reference(newest.seq(), newest.lsn()),
                    projectedSeq.getAsLong());
            state.retentionSwept(result.chainObjects(), result.snapshots());
        } catch (RuntimeException failure) {
            state.retentionFailed(failure.toString());
        }
    }

    private boolean sleep() {
        long deadline = System.nanoTime() + intervalNanos;
        synchronized (lock) {
            try {
                while (!closing) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return true;
                    }
                    TimeUnit.NANOSECONDS.timedWait(lock, remaining);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
