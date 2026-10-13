package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.closure.ClosureMaintainer.Outcome;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClosureMaintainerTest {

    private static final long DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(20);

    private static final class CountingRunner implements ClosureMaintainer.SliceRunner {

        private final AtomicInteger remaining = new AtomicInteger();
        private final AtomicInteger busy = new AtomicInteger();
        private final AtomicInteger slices = new AtomicInteger();

        @Override
        public Outcome runSlice() {
            if (busy.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                return Outcome.BUSY;
            }
            slices.incrementAndGet();
            return remaining.updateAndGet(value -> Math.max(0, value - 1)) > 0 ? Outcome.MORE : Outcome.IDLE;
        }

        @Override
        public boolean hasWork() {
            return remaining.get() > 0;
        }
    }

    private static void waitUntil(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + DEADLINE_NANOS;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting until " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void aRequestRunsSlicesUntilThereIsNoWorkAndTheThreadEnds() {
        CountingRunner runner = new CountingRunner();
        runner.remaining.set(50);
        ClosureMaintainer maintainer = new ClosureMaintainer(runner, "test-closure");

        maintainer.request();

        waitUntil(() -> !maintainer.isRunning(), "the maintainer finished");
        assertEquals(0, runner.remaining.get());
        assertEquals(50, runner.slices.get());
    }

    @Test
    void aLaterRequestStartsANewThread() {
        CountingRunner runner = new CountingRunner();
        ClosureMaintainer maintainer = new ClosureMaintainer(runner, "test-closure");
        runner.remaining.set(3);
        maintainer.request();
        waitUntil(() -> !maintainer.isRunning(), "the first run finished");

        runner.remaining.set(4);
        maintainer.request();

        waitUntil(() -> runner.remaining.get() == 0 && !maintainer.isRunning(), "the second run finished");
        assertEquals(7, runner.slices.get());
    }

    @Test
    void busySlicesAreRetriedInsteadOfEndingTheRun() {
        CountingRunner runner = new CountingRunner();
        runner.remaining.set(2);
        runner.busy.set(5);
        ClosureMaintainer maintainer = new ClosureMaintainer(runner, "test-closure");

        maintainer.request();

        waitUntil(() -> !maintainer.isRunning(), "the maintainer finished");
        assertEquals(0, runner.remaining.get());
        assertEquals(0, runner.busy.get());
    }

    @Test
    void aStoppedMaintainerIgnoresRequestsAndEndsItsThread() {
        CountingRunner runner = new CountingRunner();
        runner.remaining.set(Integer.MAX_VALUE);
        ClosureMaintainer maintainer = new ClosureMaintainer(runner, "test-closure");
        maintainer.request();
        waitUntil(() -> runner.slices.get() > 10, "the maintainer started");

        maintainer.stop();

        assertFalse(maintainer.isRunning());
        int slices = runner.slices.get();
        maintainer.request();
        assertFalse(maintainer.isRunning());
        assertEquals(slices, runner.slices.get());
    }

    @Test
    void stoppingBeforeAnyRequestIsHarmless() {
        ClosureMaintainer maintainer = new ClosureMaintainer(new CountingRunner(), "test-closure");

        maintainer.stop();
        maintainer.request();

        assertFalse(maintainer.isRunning());
    }

    @Test
    void manyConcurrentRequestsStartOneThreadAtATime() throws InterruptedException {
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        AtomicInteger remaining = new AtomicInteger(200);
        ClosureMaintainer maintainer = new ClosureMaintainer(new ClosureMaintainer.SliceRunner() {
            @Override
            public Outcome runSlice() {
                maximum.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
                concurrent.decrementAndGet();
                return remaining.updateAndGet(value -> Math.max(0, value - 1)) > 0 ? Outcome.MORE : Outcome.IDLE;
            }

            @Override
            public boolean hasWork() {
                return remaining.get() > 0;
            }
        }, "test-closure");
        Thread[] requesters = new Thread[8];
        for (int i = 0; i < requesters.length; i++) {
            requesters[i] = new Thread(() -> {
                for (int j = 0; j < 1_000; j++) {
                    maintainer.request();
                }
            });
            requesters[i].start();
        }
        for (Thread requester : requesters) {
            requester.join();
        }

        waitUntil(() -> remaining.get() == 0 && !maintainer.isRunning(), "the work finished");
        assertTrue(maximum.get() == 1, "slices overlapped: " + maximum.get());
        maintainer.stop();
    }
}
