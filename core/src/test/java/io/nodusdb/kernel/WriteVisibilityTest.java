package io.nodusdb.kernel;

import io.nodusdb.error.IndeterminateOutcomeException;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.record.RecordBatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteVisibilityTest {

    private static final long TIMEOUT_SECONDS = 10;

    private static final class GatedLog implements LogStore {

        private final CountDownLatch waiting = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final boolean failOnRelease;
        private long lastLsn;

        GatedLog(boolean failOnRelease) {
            this.failOnRelease = failOnRelease;
        }

        @Override
        public long epoch() {
            return 1;
        }

        @Override
        public synchronized long lastLsn() {
            return lastLsn;
        }

        @Override
        public long durableLsn() {
            return 0;
        }

        @Override
        public long lastCommitMicros() {
            return 0;
        }

        @Override
        public synchronized long append(RecordBatch batch) {
            lastLsn += batch.count();
            return lastLsn;
        }

        @Override
        public void awaitDurable(long lsn) {
            waiting.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            if (failOnRelease) {
                throw new IndeterminateOutcomeException("the log could not be forced");
            }
        }

        @Override
        public void force() {
        }

        @Override
        public long rollSegment() {
            return lastLsn();
        }

        @Override
        public void trim(long throughLsn) {
        }

        @Override
        public void close() {
        }
    }

    private static GraphKernel attached(GatedLog log) {
        GraphKernel kernel = new GraphKernel();
        kernel.attachLog(log, new DurableStorage() {
            @Override
            public void checkpoint(GraphKernel graph, LogStore store) {
            }

            @Override
            public void close() {
            }
        });
        return kernel;
    }

    @Test
    void readersAreNotBlockedWhileAWriterWaitsForDurabilityAndSeeTheWriteOnlyAfterwards() throws Exception {
        GatedLog log = new GatedLog(false);
        GraphKernel kernel = attached(log);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> writer = pool.submit(() -> kernel.addEdge(1, 2));
            assertTrue(log.waiting.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            Future<Integer> reads = pool.submit(() -> {
                int completed = 0;
                for (int i = 0; i < 20_000; i++) {
                    assertFalse(kernel.hasEdge(1, 2), "a write was visible before it was durable");
                    assertEquals(0, kernel.getDegree(1));
                    completed++;
                }
                return completed;
            });
            assertEquals(20_000, reads.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(0, kernel.appliedLsn());

            log.release.countDown();
            assertTrue(writer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(kernel.hasEdge(1, 2));
            assertEquals(1, kernel.appliedLsn());
        } finally {
            log.release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void aWriteWhoseDurabilityFailsIsNeverMadeVisible() throws Exception {
        GatedLog log = new GatedLog(true);
        GraphKernel kernel = attached(log);
        log.release.countDown();

        assertThrows(IndeterminateOutcomeException.class, () -> kernel.addEdge(1, 2));

        assertFalse(kernel.hasEdge(1, 2));
        assertEquals(0, kernel.appliedLsn());
    }
}
