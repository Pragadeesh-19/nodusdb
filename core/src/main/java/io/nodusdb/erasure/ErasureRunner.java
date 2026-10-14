package io.nodusdb.erasure;

import io.nodusdb.error.ErasurePendingException;
import io.nodusdb.kernel.ErasureProgress;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

public final class ErasureRunner implements ErasureProgress {

    static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(200);
    static final long MAX_BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(30);

    private static final long NOT_PENDING = -1L;
    private static final long MAX_WAIT_NANOS = TimeUnit.DAYS.toNanos(3_650);
    private static final String THREAD_NAME = "nodus-erasure";

    private final GraphKernel kernel;
    private final List<ErasureLayer> layers;
    private final long delayNanos;
    private final LongSupplier nanoClock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private Thread worker;
    private boolean urgent;
    private boolean closed;
    private long pendingSince = NOT_PENDING;
    private int failures;
    private volatile String lastError = "";

    ErasureRunner(GraphKernel kernel, List<ErasureLayer> layers, Duration checkpointDelay, LongSupplier nanoClock) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.layers = List.copyOf(layers);
        if (checkpointDelay.isNegative()) {
            throw new IllegalArgumentException("the checkpoint delay must not be negative: " + checkpointDelay);
        }
        this.delayNanos = checkpointDelay.toNanos();
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    public static ErasureRunner durable(GraphKernel kernel, Duration checkpointDelay) {
        ErasureRunner runner = new ErasureRunner(kernel, List.of(new LocalErasure(kernel)), checkpointDelay,
                System::nanoTime);
        runner.wake();
        return runner;
    }

    @Override
    public void wake() {
        lock.lock();
        try {
            if (!closed && !pendingLayers(kernel.newestEraseLsn()).isEmpty()) {
                ensureWorker();
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void awaitErased(Token token, Duration timeout) {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("the erasure wait must not be negative: " + timeout);
        }
        long remaining = waitNanos(timeout);
        lock.lock();
        try {
            while (true) {
                List<String> pending = pendingNames(Math.min(token.lsn(), kernel.newestEraseLsn()));
                if (pending.isEmpty()) {
                    return;
                }
                if (closed) {
                    throw new IllegalStateException("graph is closed");
                }
                if (remaining <= 0) {
                    throw new ErasurePendingException(token.epoch(), token.lsn(), pending);
                }
                urgent = true;
                ensureWorker();
                changed.signalAll();
                remaining = changed.awaitNanos(remaining);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for an erasure", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Layer> layers() {
        long newest = kernel.newestEraseLsn();
        int erased = kernel.erasedSymbols();
        List<Layer> statuses = new ArrayList<>(layers.size());
        for (ErasureLayer layer : layers) {
            statuses.add(new Layer(layer.name(), layer.coveredLsn(), !layer.covers(newest, erased)));
        }
        return statuses;
    }

    @Override
    public String lastError() {
        return lastError;
    }

    @Override
    public void close() {
        Thread running;
        lock.lock();
        try {
            closed = true;
            changed.signalAll();
            running = worker;
        } finally {
            lock.unlock();
        }
        if (running != null && running != Thread.currentThread()) {
            joinUninterruptibly(running);
        }
    }

    boolean working() {
        lock.lock();
        try {
            return worker != null;
        } finally {
            lock.unlock();
        }
    }

    private void ensureWorker() {
        if (worker == null) {
            worker = new Thread(this::run, THREAD_NAME);
            worker.setDaemon(true);
            worker.start();
        }
    }

    private void run() {
        while (true) {
            long target;
            boolean due;
            List<ErasureLayer> pending;
            lock.lock();
            try {
                target = kernel.newestEraseLsn();
                pending = closed || kernel.isClosed() ? List.of() : pendingLayers(target);
                if (pending.isEmpty()) {
                    finish();
                    return;
                }
                long now = nanoClock.getAsLong();
                if (pendingSince == NOT_PENDING) {
                    pendingSince = now;
                }
                due = urgent || now - pendingSince >= delayNanos;
            } finally {
                lock.unlock();
            }
            boolean failed = advance(pending, target, due);
            lock.lock();
            try {
                failures = failed ? failures + 1 : 0;
                changed.signalAll();
                if (!closed) {
                    changed.awaitNanos(pause());
                }
            } catch (InterruptedException e) {
                finish();
                return;
            } finally {
                lock.unlock();
            }
        }
    }

    private void finish() {
        worker = null;
        urgent = false;
        pendingSince = NOT_PENDING;
        failures = 0;
        changed.signalAll();
    }

    private boolean advance(List<ErasureLayer> pending, long target, boolean due) {
        boolean failed = false;
        for (ErasureLayer layer : pending) {
            try {
                layer.advance(target, due);
            } catch (RuntimeException e) {
                failed = true;
                lastError = layer.name() + ": " + e.getMessage();
            }
        }
        if (!failed) {
            lastError = "";
        }
        return failed;
    }

    private long pause() {
        if (failures == 0) {
            return TICK_NANOS;
        }
        int shift = Math.min(failures, 8);
        return Math.min(TICK_NANOS << shift, MAX_BACKOFF_NANOS);
    }

    private List<ErasureLayer> pendingLayers(long requiredLsn) {
        int erased = kernel.erasedSymbols();
        List<ErasureLayer> pending = new ArrayList<>(layers.size());
        for (ErasureLayer layer : layers) {
            if (!layer.covers(requiredLsn, erased)) {
                pending.add(layer);
            }
        }
        return pending;
    }

    private List<String> pendingNames(long requiredLsn) {
        List<String> names = new ArrayList<>();
        for (ErasureLayer layer : pendingLayers(requiredLsn)) {
            names.add(layer.name());
        }
        return names;
    }

    private static long waitNanos(Duration timeout) {
        try {
            return Math.min(timeout.toNanos(), MAX_WAIT_NANOS);
        } catch (ArithmeticException tooLong) {
            return MAX_WAIT_NANOS;
        }
    }

    private static void joinUninterruptibly(Thread thread) {
        boolean interrupted = false;
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
