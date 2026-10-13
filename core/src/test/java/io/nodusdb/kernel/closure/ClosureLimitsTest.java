package io.nodusdb.kernel.closure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClosureLimitsTest {

    @Test
    void theDefaultsAreTheReviewedOnes() {
        assertTrue(ClosureLimits.DEFAULT.enabled());
        assertEquals(4_096, ClosureLimits.DEFAULT.workBudget());
        assertEquals(1L << 20, ClosureLimits.DEFAULT.minimumCap());
        assertEquals(4, ClosureLimits.DEFAULT.capPerTuple());
    }

    @Test
    void theSwitchTurnsTheIndexOffOnlyForOff() {
        assertFalse(ClosureLimits.forSwitch("off").enabled());
        assertFalse(ClosureLimits.forSwitch(" OFF ").enabled());
        assertTrue(ClosureLimits.forSwitch(null).enabled());
        assertTrue(ClosureLimits.forSwitch("").enabled());
        assertTrue(ClosureLimits.forSwitch("on").enabled());
        assertTrue(ClosureLimits.forSwitch("0").enabled());
    }

    @Test
    void theCapIsTheLargerOfTheMinimumAndTheScaledTupleCount() {
        assertEquals(1L << 20, ClosureLimits.DEFAULT.capFor(0));
        assertEquals(1L << 20, ClosureLimits.DEFAULT.capFor(1_000));
        assertEquals(40_000_000L, ClosureLimits.DEFAULT.capFor(10_000_000L));
        assertEquals(Long.MAX_VALUE, ClosureLimits.DEFAULT.capFor(Long.MAX_VALUE / 2));
        assertEquals(7, ClosureLimits.DEFAULT.withCap(7, 0).capFor(1_000_000));
    }

    @Test
    void invalidLimitsAreRefused() {
        assertThrows(IllegalArgumentException.class, () -> new ClosureLimits(true, 0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClosureLimits(true, 1, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClosureLimits(true, 1, 1, -1));
    }

    @Test
    void derivedLimitsKeepTheOtherFields() {
        ClosureLimits limits = ClosureLimits.DEFAULT.withWorkBudget(8).withCap(9, 2).disabled();

        assertFalse(limits.enabled());
        assertEquals(8, limits.workBudget());
        assertEquals(9, limits.minimumCap());
        assertEquals(2, limits.capPerTuple());
    }
}
