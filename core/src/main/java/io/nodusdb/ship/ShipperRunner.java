package io.nodusdb.ship;

import java.io.IOException;

public final class ShipperRunner implements AutoCloseable {

    private static final long JOIN_MILLIS = 10_000;
    private static final long INTERRUPTED_JOIN_MILLIS = 2_000;

    private final ShipperCore core;
    private final ShipState state;
    private final LogFeed feed;
    private final ShipSettings settings;
    private final Thread thread;

    private ShipperRunner(ShipperCore core, ShipState state, LogFeed feed, ShipSettings settings, String name) {
        this.core = core;
        this.state = state;
        this.feed = feed;
        this.settings = settings;
        this.thread = new Thread(this::run, name);
        this.thread.setDaemon(true);
    }

    public static ShipperRunner start(ShipperCore core, ShipState state, LogFeed feed, ShipSettings settings,
                                      String name) {
        ShipperRunner runner = new ShipperRunner(core, state, feed, settings, name);
        runner.thread.start();
        return runner;
    }

    public ShipState state() {
        return state;
    }

    @Override
    public void close() {
        state.requestStop();
        join(JOIN_MILLIS);
        if (thread.isAlive()) {
            thread.interrupt();
            join(INTERRUPTED_JOIN_MILLIS);
        }
        state.closed();
    }

    private void join(long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        try {
            loop();
            if (state.stopRequested() && !state.terminal()) {
                drain();
            }
        } finally {
            closeFeed();
        }
    }

    private void loop() {
        while (!state.stopRequested()) {
            ShipperCore.Next next = step();
            switch (next.cadence()) {
                case CONTINUE -> {
                }
                case IDLE -> state.awaitWork(next.nanos());
                case BACKOFF -> state.pause(next.nanos());
                case STOP -> {
                    return;
                }
            }
        }
    }

    private ShipperCore.Next step() {
        try {
            return core.step();
        } catch (RuntimeException unexpected) {
            return unexpectedFailure(unexpected);
        } catch (Error fatal) {
            state.failed("shipping stopped on " + fatal);
            throw fatal;
        }
    }

    private ShipperCore.Next unexpectedFailure(RuntimeException unexpected) {
        state.failed("unexpected failure: " + unexpected);
        return ShipperCore.Next.backoff(settings.failedRetry().toNanos());
    }

    private void drain() {
        try {
            core.drain();
        } catch (RuntimeException unexpected) {
            state.failed("unexpected failure while closing: " + unexpected);
        }
    }

    private void closeFeed() {
        try {
            feed.close();
        } catch (IOException ignored) {
            return;
        }
    }
}
