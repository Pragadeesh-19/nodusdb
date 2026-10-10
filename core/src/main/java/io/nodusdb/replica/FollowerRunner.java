package io.nodusdb.replica;

import io.nodusdb.ship.Pace;

import java.io.UncheckedIOException;

final class FollowerRunner implements AutoCloseable {

    private static final long JOIN_MILLIS = 10_000;
    private static final long INTERRUPTED_JOIN_MILLIS = 2_000;

    private final FollowerCore core;
    private final FollowerState state;
    private final AntiRollbackMarker marker;
    private final Thread thread;

    private FollowerRunner(FollowerCore core, FollowerState state, AntiRollbackMarker marker, String name) {
        this.core = core;
        this.state = state;
        this.marker = marker;
        this.thread = new Thread(this::loop, name);
        this.thread.setDaemon(true);
    }

    static FollowerRunner start(FollowerCore core, FollowerState state, AntiRollbackMarker marker, String name) {
        FollowerRunner runner = new FollowerRunner(core, state, marker, name);
        runner.thread.start();
        return runner;
    }

    @Override
    public void close() {
        state.requestStop();
        join(JOIN_MILLIS);
        if (thread.isAlive()) {
            thread.interrupt();
            join(INTERRUPTED_JOIN_MILLIS);
        }
        flushMarker();
        state.closed();
    }

    private void loop() {
        while (!state.stopRequested()) {
            Pace pace = step();
            switch (pace.kind()) {
                case CONTINUE -> {
                }
                case IDLE, BACKOFF -> state.pause(pace.nanos());
                case STOP -> {
                    return;
                }
            }
        }
    }

    private Pace step() {
        try {
            return core.step();
        } catch (Error fatal) {
            state.stalled("the follower stopped on " + fatal);
            throw fatal;
        }
    }

    private void flushMarker() {
        try {
            marker.flush();
        } catch (UncheckedIOException failure) {
            state.note("could not write the rollback marker: " + failure.getMessage());
        }
    }

    private void join(long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
