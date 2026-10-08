package io.nodusdb.projection;

import io.nodusdb.ship.ShipState;

import java.util.concurrent.TimeUnit;

public final class ProjectorRunner implements AutoCloseable {

    private static final long JOIN_MILLIS = 15_000;
    private static final long INTERRUPTED_JOIN_MILLIS = 2_000;

    private final EdgeLogProjector projector;
    private final ShipState state;
    private final long failedRetryNanos;
    private final Object lock = new Object();
    private final Thread thread;
    private boolean closing;

    private ProjectorRunner(EdgeLogProjector projector, ShipState state, long failedRetryNanos, String name) {
        this.projector = projector;
        this.state = state;
        this.failedRetryNanos = failedRetryNanos;
        this.thread = new Thread(this::run, name);
        this.thread.setDaemon(true);
    }

    public static ProjectorRunner start(EdgeLogProjector projector, ShipState state, ProjectionSettings settings,
                                        String name) {
        ProjectorRunner runner = new ProjectorRunner(projector, state, settings.failedRetry().toNanos(), name);
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
        state.projectionClosed();
    }

    private void join(long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        loop();
        if (isClosing() && !state.projectionTerminal()) {
            drain();
        }
    }

    private void loop() {
        while (!isClosing()) {
            EdgeLogProjector.Next next = step();
            switch (next.cadence()) {
                case CONTINUE -> {
                }
                case IDLE, BACKOFF -> {
                    if (!sleep(next.nanos())) {
                        return;
                    }
                }
                case STOP -> {
                    return;
                }
            }
        }
    }

    private EdgeLogProjector.Next step() {
        try {
            return projector.step();
        } catch (RuntimeException unexpected) {
            state.projectionFailed("unexpected failure: " + unexpected);
            return EdgeLogProjector.Next.backoff(failedRetryNanos);
        } catch (Error fatal) {
            state.projectionFailed("the projector stopped on " + fatal);
            throw fatal;
        }
    }

    private void drain() {
        try {
            projector.drain();
        } catch (RuntimeException unexpected) {
            state.projectionFailed("unexpected failure while closing: " + unexpected);
        }
    }

    private boolean isClosing() {
        synchronized (lock) {
            return closing;
        }
    }

    private boolean sleep(long nanos) {
        long deadline = System.nanoTime() + nanos;
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
