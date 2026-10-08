package io.nodusdb.ship;

import io.nodusdb.log.LogConfig;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.SegmentedLog;
import io.nodusdb.log.SyncMode;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.simulation.LogHarness;
import io.nodusdb.log.simulation.SimulatedDisk;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LogFeedTest {

    private static final long NEVER_MILLIS = 3_600_000L;

    private static final class CountingLog implements LogStore {

        private final LogStore delegate;
        int forces;

        CountingLog(LogStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public long epoch() {
            return delegate.epoch();
        }

        @Override
        public long lastLsn() {
            return delegate.lastLsn();
        }

        @Override
        public long durableLsn() {
            return delegate.durableLsn();
        }

        @Override
        public long lastCommitMicros() {
            return delegate.lastCommitMicros();
        }

        @Override
        public long append(RecordBatch batch) {
            return delegate.append(batch);
        }

        @Override
        public void awaitDurable(long lsn) {
            delegate.awaitDurable(lsn);
        }

        @Override
        public void force() {
            forces++;
            delegate.force();
        }

        @Override
        public long rollSegment() {
            return delegate.rollSegment();
        }

        @Override
        public void trim(long throughLsn) {
            delegate.trim(throughLsn);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private final SimulatedDisk disk = new SimulatedDisk();
    private SegmentedLog real;
    private CountingLog log;

    @BeforeEach
    void open() throws IOException {
        LogConfig lazy = new LogConfig(SyncMode.ASYNC, NEVER_MILLIS, 2 << 20, 64L << 20, NEVER_MILLIS);
        real = LogHarness.open(disk, lazy, 0).log();
        log = new CountingLog(real);
    }

    @AfterEach
    void close() {
        real.close();
    }

    private LogFeed feed(AtomicBoolean wanted) {
        return new LogFeed(log, new LogTailReader(disk, 0), wanted::get);
    }

    private void writeTuples() {
        real.append(LogHarness.tuples(1, 3));
        assertEquals(true, real.durableLsn() < real.lastLsn(), "the writes must still be in the buffer");
    }

    @Test
    void theLogIsNotForcedWhileNobodyWaitsForShipping() throws IOException {
        writeTuples();
        LogFeed feed = feed(new AtomicBoolean(false));

        feed.next(1 << 20);

        assertEquals(0, log.forces);
        assertEquals(true, real.durableLsn() < real.lastLsn());
    }

    @Test
    void theLogIsForcedWhenAWaiterNeedsTheTailToBeDurable() throws IOException {
        writeTuples();
        AtomicBoolean wanted = new AtomicBoolean(false);
        LogFeed feed = feed(wanted);
        feed.next(1 << 20);

        wanted.set(true);
        LogTailReader.Batch batch = feed.next(1 << 20);

        assertEquals(1, log.forces);
        assertNotNull(batch);
        assertEquals(real.lastLsn(), batch.lsnLast());
    }

    @Test
    void anAlreadyDurableLogIsNeverForcedAgain() throws IOException {
        writeTuples();
        real.force();
        LogFeed feed = feed(new AtomicBoolean(true));

        feed.next(1 << 20);
        feed.next(1 << 20);

        assertEquals(0, log.forces);
    }

    @Test
    void theDefaultFeedForcesWheneverTheTailIsNotDurable() throws IOException {
        writeTuples();
        LogFeed feed = new LogFeed(log, new LogTailReader(disk, 0));

        feed.next(1 << 20);

        assertEquals(1, log.forces);
    }

    @Test
    void theStatePredicateWantsDurabilityOnlyForWaitersAndTheFinalDrain() throws InterruptedException {
        ShipState state = new ShipState(1, 0, 0, 1_000);
        assertEquals(false, state.durabilityWanted());

        Thread waiter = new Thread(() -> {
            try {
                state.awaitShipped(1, 99, 5_000_000_000L);
            } catch (RuntimeException expected) {
                return;
            }
        });
        waiter.start();
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!state.hasWaiters() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }

        assertEquals(true, state.durabilityWanted());
        state.closed();
        waiter.join(5_000);
        assertEquals(false, state.durabilityWanted());
        state.requestStop();
        assertEquals(true, state.durabilityWanted());
    }
}
