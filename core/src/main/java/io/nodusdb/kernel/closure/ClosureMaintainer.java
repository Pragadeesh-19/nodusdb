package io.nodusdb.kernel.closure;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class ClosureMaintainer {

    public enum Outcome { MORE, BUSY, IDLE, CLOSED }

    public interface SliceRunner {

        Outcome runSlice();

        boolean hasWork();
    }

    private static final long BACKOFF_NANOS = TimeUnit.MICROSECONDS.toNanos(200);
    private static final long STOP_WAIT_MILLIS = 5_000;

    private final SliceRunner runner;
    private final String name;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile boolean stopped;
    private volatile Thread thread;

    public ClosureMaintainer(SliceRunner runner, String name) {
        this.runner = Objects.requireNonNull(runner, "runner");
        this.name = Objects.requireNonNull(name, "name");
    }

    public void request() {
        if (stopped || running.get() || !running.compareAndSet(false, true)) {
            return;
        }
        Thread started = new Thread(this::run, name);
        started.setDaemon(true);
        thread = started;
        started.start();
    }

    public boolean isRunning() {
        return running.get();
    }

    public void stop() {
        stopped = true;
        Thread current = thread;
        if (current == null || current == Thread.currentThread()) {
            return;
        }
        current.interrupt();
        try {
            current.join(STOP_WAIT_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        while (true) {
            Outcome outcome = stopped ? Outcome.CLOSED : runner.runSlice();
            if (outcome == Outcome.MORE) {
                continue;
            }
            if (outcome == Outcome.BUSY) {
                LockSupport.parkNanos(BACKOFF_NANOS);
                continue;
            }
            running.set(false);
            if (stopped || !runner.hasWork() || !running.compareAndSet(false, true)) {
                return;
            }
        }
    }
}
