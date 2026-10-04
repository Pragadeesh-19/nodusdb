package io.nodusdb.kernel;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphConcurrencyTest {

    private static final int TARGETS = 2_000;
    private static final long HUB = 0L;
    private static final long SHARED = TARGETS + 1L;
    private static final int WRITES = 200_000;
    private static final int READERS = 8;
    private static final int WARMUP_ROUNDS = 2_000;
    private static final int MEASURED_ROUNDS = 5_000;
    private static final long JOIN_TIMEOUT_SECONDS = 120L;

    @Test
    void readersSeeConsistentTraversalsWhileTheWriterChurnsSwapAndPop() throws InterruptedException {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported());
        GraphKernel kernel = new GraphKernel();
        for (long target = 1; target <= TARGETS; target++) {
            kernel.addEdge(HUB, target);
            kernel.addEdge(SHARED, target);
        }
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicLong steadyStateBytes = new AtomicLong();
        AtomicLong completedQueries = new AtomicLong();
        CountDownLatch warmed = new CountDownLatch(READERS);
        List<Thread> readers = new ArrayList<>(READERS);
        for (int r = 0; r < READERS; r++) {
            Thread reader = new Thread(() -> readerLoop(kernel, bean, writing, failure, steadyStateBytes,
                    completedQueries, warmed), "reader-" + r);
            readers.add(reader);
            reader.start();
        }

        assertTrue(warmed.await(JOIN_TIMEOUT_SECONDS, TimeUnit.SECONDS), "readers did not warm up");
        boolean[] present = new boolean[TARGETS + 1];
        for (int target = 1; target <= TARGETS; target++) {
            present[target] = true;
        }
        Random random = new Random(7L);
        try {
            for (int op = 0; op < WRITES; op++) {
                int target = 1 + random.nextInt(TARGETS);
                if (present[target]) {
                    assertTrue(kernel.removeEdge(HUB, target), "remove of present edge " + target + " at op " + op);
                    present[target] = false;
                } else {
                    assertTrue(kernel.addEdge(HUB, target), "add of absent edge " + target + " at op " + op);
                    present[target] = true;
                }
            }
        } finally {
            writing.set(false);
        }

        for (Thread reader : readers) {
            reader.join(TimeUnit.SECONDS.toMillis(JOIN_TIMEOUT_SECONDS));
            assertFalse(reader.isAlive(), reader.getName() + " did not finish: possible deadlock");
        }
        assertNull(failure.get(), () -> "reader failed: " + failure.get());
        assertEquals(0L, steadyStateBytes.get(), "bytes allocated by readers in a quiescent steady-state window");
        assertTrue(completedQueries.get() > 0L, "readers never ran a query");
        int expectedDegree = 0;
        for (int target = 1; target <= TARGETS; target++) {
            if (present[target]) {
                expectedDegree++;
            }
        }
        assertEquals(expectedDegree, kernel.getDegree(HUB));
        assertEquals(TARGETS, kernel.getDegree(SHARED));
    }

    private static void readerLoop(GraphKernel kernel, com.sun.management.ThreadMXBean bean,
                                   AtomicBoolean writing, AtomicReference<Throwable> failure,
                                   AtomicLong steadyStateBytes, AtomicLong completedQueries,
                                   CountDownLatch warmed) {
        long[] out = new long[TARGETS + 1];
        int[] seen = new int[TARGETS + 1];
        int generation = 0;
        long threadId = Thread.currentThread().threadId();
        try {
            for (int round = 0; round < WARMUP_ROUNDS; round++) {
                generation = checkRound(kernel, out, seen, generation);
            }
            warmed.countDown();
            while (writing.get()) {
                generation = checkRound(kernel, out, seen, generation);
                completedQueries.incrementAndGet();
            }
            long before = bean.getThreadAllocatedBytes(threadId);
            for (int round = 0; round < MEASURED_ROUNDS; round++) {
                generation = checkRound(kernel, out, seen, generation);
            }
            long allocated = bean.getThreadAllocatedBytes(threadId) - before;
            steadyStateBytes.accumulateAndGet(allocated, Math::max);
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
            warmed.countDown();
        }
    }

    private static int checkRound(GraphKernel kernel, long[] out, int[] seen, int generation) {
        int next = generation + 1;
        int count = kernel.kHop(HUB, 1, out);
        requireDistinctTargets(out, count, seen, next, "kHop");
        next++;
        int common = kernel.commonNeighbors(HUB, SHARED, out);
        requireDistinctTargets(out, common, seen, next, "commonNeighbors");
        return next;
    }

    private static void requireDistinctTargets(long[] out, int count, int[] seen, int generation, String query) {
        if (count < 0 || count > TARGETS) {
            throw new IllegalStateException(query + " returned an impossible count " + count);
        }
        for (int i = 0; i < count; i++) {
            long value = out[i];
            if (value < 1 || value > TARGETS) {
                throw new IllegalStateException(query + " returned a torn target " + value);
            }
            int index = (int) value;
            if (seen[index] == generation) {
                throw new IllegalStateException(query + " returned target " + value + " twice");
            }
            seen[index] = generation;
        }
    }
}
