package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EpochHistoryTest {

    private static EpochHistory threeTenures() {
        EpochHistory history = new EpochHistory();
        history.record(1, 1, 0);
        history.record(2, 10, 7);
        history.record(3, 20, 15);
        return history;
    }

    @Test
    void anEmptyHistoryHasNoEpoch() {
        EpochHistory history = new EpochHistory();

        assertEquals(0, history.size());
        assertEquals(0, history.latestEpoch());
        assertFalse(history.isLost(1, 5));
    }

    @Test
    void tenuresAreKeptInOrder() {
        EpochHistory history = threeTenures();

        assertEquals(3, history.size());
        assertEquals(3, history.latestEpoch());
        assertEquals(new EpochHistory.Tenure(2, 10, 7), history.tenureAt(1));
    }

    @Test
    void anEpochMustFollowTheOneBeforeIt() {
        EpochHistory history = threeTenures();

        assertThrows(IllegalStateException.class, () -> history.record(3, 30, 25));
        assertThrows(IllegalStateException.class, () -> history.record(2, 30, 25));
        assertEquals(3, history.size());
    }

    @Test
    void aWriteAfterTheNextTenureStartedItsHandoffIsLost() {
        EpochHistory history = threeTenures();

        assertFalse(history.isLost(1, 7), "the last durable write of the first tenure");
        assertTrue(history.isLost(1, 8), "acknowledged but never durable");
        assertFalse(history.isLost(2, 15));
        assertTrue(history.isLost(2, 16));
    }

    @Test
    void theCurrentTenureNeverLosesAToken() {
        EpochHistory history = threeTenures();

        assertFalse(history.isLost(3, 1_000_000));
        assertFalse(history.isLost(4, 1));
    }
}
