package io.nodusdb.kernel.memory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryBudgetTest {

    @Test
    void chargesAccumulateUpToTheLimit() {
        MemoryBudget budget = MemoryBudget.limitedTo(100);

        budget.charge(60);
        budget.charge(40);

        assertEquals(100, budget.used());
        assertTrue(budget.isLimited());
    }

    @Test
    void aChargePastTheLimitIsRefusedAndLeavesTheCountUnchanged() {
        MemoryBudget budget = MemoryBudget.limitedTo(100);
        budget.charge(70);

        assertThrows(MemoryLimitExceededException.class, () -> budget.charge(31));

        assertEquals(70, budget.used());
        budget.charge(30);
        assertEquals(100, budget.used());
    }

    @Test
    void requireChecksWithoutCharging() {
        MemoryBudget budget = MemoryBudget.limitedTo(100);
        budget.charge(90);

        budget.require(10);
        assertThrows(MemoryLimitExceededException.class, () -> budget.require(11));

        assertEquals(90, budget.used());
    }

    @Test
    void canChargeMatchesWhatChargeAccepts() {
        MemoryBudget budget = MemoryBudget.limitedTo(100);
        budget.charge(95);

        assertTrue(budget.canCharge(5));
        assertFalse(budget.canCharge(6));
    }

    @Test
    void unlimitedBudgetCountsButNeverRefuses() {
        MemoryBudget budget = MemoryBudget.unlimited();

        budget.charge(Long.MAX_VALUE / 2);
        budget.charge(1L << 40);

        assertFalse(budget.isLimited());
        assertTrue(budget.canCharge(1L << 50));
    }

    @Test
    void aRequestLargerThanTheRemainingLimitDoesNotOverflow() {
        MemoryBudget budget = MemoryBudget.limitedTo(Long.MAX_VALUE - 1);
        budget.charge(Long.MAX_VALUE - 2);

        assertThrows(MemoryLimitExceededException.class, () -> budget.charge(Long.MAX_VALUE - 1));
    }

    @Test
    void nonPositiveLimitIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> MemoryBudget.limitedTo(0));
        assertThrows(IllegalArgumentException.class, () -> MemoryBudget.limitedTo(-5));
    }

    @Test
    void theExceptionNamesTheLimitTheUsageAndTheRequest() {
        MemoryBudget budget = MemoryBudget.limitedTo(100);
        budget.charge(70);

        MemoryLimitExceededException error = assertThrows(MemoryLimitExceededException.class, () -> budget.charge(31));

        assertTrue(error.getMessage().contains("100"));
        assertTrue(error.getMessage().contains("70"));
        assertTrue(error.getMessage().contains("31"));
    }
}
