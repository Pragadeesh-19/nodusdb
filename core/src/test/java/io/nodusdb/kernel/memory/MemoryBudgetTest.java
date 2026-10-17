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
    void aBudgetDrawingOnAPoolIsRefusedWhenThePoolRunsOutAndKeepsItsOwnCount() {
        MemoryBudget pool = MemoryBudget.limitedTo(100);
        MemoryBudget first = MemoryBudget.drawingOn(pool, 80);
        MemoryBudget second = MemoryBudget.drawingOn(pool, Long.MAX_VALUE);
        first.charge(60);

        assertThrows(MemoryLimitExceededException.class, () -> second.charge(41));
        assertThrows(MemoryLimitExceededException.class, () -> first.charge(21));

        assertEquals(0, second.used());
        assertEquals(60, first.used());
        assertEquals(60, pool.used());
        assertFalse(second.canCharge(41));
        assertThrows(MemoryLimitExceededException.class, () -> second.require(41));
        second.charge(40);
        assertEquals(100, pool.used());
    }

    @Test
    void releasingABudgetReturnsItsChargesToThePool() {
        MemoryBudget pool = MemoryBudget.limitedTo(100);
        MemoryBudget view = MemoryBudget.drawingOn(pool, 100);
        view.charge(70);

        view.releaseAll();

        assertEquals(0, view.used());
        assertEquals(0, pool.used());
        MemoryBudget.drawingOn(pool, 100).charge(100);
        assertEquals(100, pool.used());
    }

    @Test
    void concurrentChargesNeverPassThePoolLimit() throws InterruptedException {
        MemoryBudget pool = MemoryBudget.limitedTo(10_000);
        Thread[] threads = new Thread[4];
        for (int t = 0; t < threads.length; t++) {
            MemoryBudget view = MemoryBudget.drawingOn(pool, Long.MAX_VALUE);
            threads[t] = new Thread(() -> {
                for (int i = 0; i < 10_000; i++) {
                    try {
                        view.charge(1);
                    } catch (MemoryLimitExceededException full) {
                        return;
                    }
                }
            });
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(10_000, pool.used());
    }

    @Test
    void aPoolIsRequiredAndALimitMustBePositive() {
        assertThrows(NullPointerException.class, () -> MemoryBudget.drawingOn(null, 10));
        assertThrows(IllegalArgumentException.class, () -> MemoryBudget.drawingOn(MemoryBudget.unlimited(), 0));
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
