package io.nodusdb.ship;

import io.nodusdb.error.LogBacklogException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.record.RecordBatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShippingLogStoreTest {

    private static final class RecordingLog implements LogStore {

        final List<String> calls = new ArrayList<>();

        @Override
        public long epoch() {
            calls.add("epoch");
            return 4;
        }

        @Override
        public long lastLsn() {
            calls.add("lastLsn");
            return 90;
        }

        @Override
        public long durableLsn() {
            calls.add("durableLsn");
            return 80;
        }

        @Override
        public long lastCommitMicros() {
            calls.add("lastCommitMicros");
            return 1_234;
        }

        @Override
        public long append(RecordBatch batch) {
            calls.add("append");
            return 91;
        }

        @Override
        public void awaitDurable(long lsn) {
            calls.add("awaitDurable " + lsn);
        }

        @Override
        public void force() {
            calls.add("force");
        }

        @Override
        public long rollSegment() {
            calls.add("rollSegment");
            return 85;
        }

        @Override
        public void trim(long throughLsn) {
            calls.add("trim " + throughLsn);
        }

        @Override
        public void close() {
            calls.add("close");
        }
    }

    private static final class FixedGate implements ShippingLogStore.Gate {

        long shipped;
        String refusal;

        @Override
        public long shippedLsn() {
            return shipped;
        }

        @Override
        public String refusal() {
            return refusal;
        }
    }

    @Test
    void trimNeverGoesPastWhatHasBeenShipped() {
        RecordingLog log = new RecordingLog();
        FixedGate gate = new FixedGate();
        ShippingLogStore store = new ShippingLogStore(log, gate);

        gate.shipped = 50;
        store.trim(80);
        store.trim(30);
        gate.shipped = -1;
        store.trim(80);
        gate.shipped = 1_000;
        store.trim(80);

        assertEquals(List.of("trim 50", "trim 30", "trim -1", "trim 80"), log.calls);
    }

    @Test
    void aWriteIsRefusedWithTheGatesReasonWhileTheBacklogIsAtItsCap() {
        RecordingLog log = new RecordingLog();
        FixedGate gate = new FixedGate();
        gate.refusal = "the backlog is full";
        ShippingLogStore store = new ShippingLogStore(log, gate);

        LogBacklogException refused = assertThrows(LogBacklogException.class, () -> store.append(new RecordBatch()));

        assertEquals("the backlog is full", refused.getMessage());
        assertEquals(List.of(), log.calls);
    }

    @Test
    void writesFlowAgainOnceTheGateOpens() {
        RecordingLog log = new RecordingLog();
        FixedGate gate = new FixedGate();
        gate.refusal = "full";
        ShippingLogStore store = new ShippingLogStore(log, gate);
        assertThrows(LogBacklogException.class, () -> store.append(new RecordBatch()));

        gate.refusal = null;

        assertEquals(91, store.append(new RecordBatch()));
        assertEquals(List.of("append"), log.calls);
    }

    @Test
    void everythingElseIsPassedStraightThrough() {
        RecordingLog log = new RecordingLog();
        ShippingLogStore store = new ShippingLogStore(log, new FixedGate());

        assertEquals(4, store.epoch());
        assertEquals(90, store.lastLsn());
        assertEquals(80, store.durableLsn());
        assertEquals(1_234, store.lastCommitMicros());
        store.awaitDurable(77);
        store.force();
        assertEquals(85, store.rollSegment());
        store.close();

        assertEquals(List.of("epoch", "lastLsn", "durableLsn", "lastCommitMicros", "awaitDurable 77", "force",
                "rollSegment", "close"), log.calls);
    }

    @Test
    void aFencedWriterRefusesEveryWriteBeforeLookingAtTheBacklog() {
        RecordingLog log = new RecordingLog();
        ShipState state = new ShipState(4, 40, 3, 1_000);
        ShippingLogStore store = new ShippingLogStore(log, state);
        state.backlog(1_000);

        state.fenced("epoch 4 was superseded");

        WriterFencedException refused = assertThrows(WriterFencedException.class,
                () -> store.append(new RecordBatch()));
        assertEquals("epoch 4 was superseded", refused.getMessage());
        assertEquals(List.of(), log.calls);
    }

    @Test
    void aStateThatIsNotFencedHasNoFencedReason() {
        ShipState state = new ShipState(4, 40, 3, 1_000);

        assertEquals(null, state.fencedReason());
        state.failed("the store is down");
        assertEquals(null, state.fencedReason());
        state.fenced("lost");
        assertEquals("lost", state.fencedReason());
    }

    @Test
    void theHookRunsBeforeTheLogIsClosedAndTheLogClosesEvenIfItFails() {
        RecordingLog log = new RecordingLog();
        List<String> order = new ArrayList<>();
        ShippingLogStore store = new ShippingLogStore(log, new FixedGate(), () -> {
            order.add("hook sees " + log.calls);
            throw new IllegalStateException("hook failed");
        });

        assertThrows(IllegalStateException.class, store::close);

        assertEquals(List.of("hook sees []"), order);
        assertEquals(List.of("close"), log.calls);
    }

    @Test
    void theBacklogStateDrivesTheGateEndToEnd() {
        RecordingLog log = new RecordingLog();
        ShipState state = new ShipState(4, 40, 3, 1_000);
        ShippingLogStore store = new ShippingLogStore(log, state);

        state.backlog(1_000);
        assertThrows(LogBacklogException.class, () -> store.append(new RecordBatch()));
        store.trim(100);
        state.shipped(60, 4, 10, 1);
        store.trim(100);
        state.backlog(10);
        store.append(new RecordBatch());

        assertEquals(List.of("trim 40", "trim 60", "append"), log.calls);
        assertTrue(state.refusal() == null);
    }
}
