package io.nodusdb.kernel.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAddressingTest {

    private static final int MASK = 7;

    @Test
    void holeOnTheProbePathAcrossTheWrapAcceptsTheEntry() {
        assertTrue(OpenAddressing.canMoveInto(1, 2, 6, MASK));
        assertTrue(OpenAddressing.canMoveInto(7, 2, 6, MASK));
    }

    @Test
    void holeBeforeTheHomeSlotRejectsTheEntry() {
        assertFalse(OpenAddressing.canMoveInto(5, 2, 6, MASK));
        assertFalse(OpenAddressing.canMoveInto(3, 2, 6, MASK));
    }

    @Test
    void homeOfAKeyIsInsideTheTable() {
        for (long key = -1_000; key < 1_000; key++) {
            int home = OpenAddressing.home(key, MASK);
            assertTrue(home >= 0 && home <= MASK, "home of " + key + " is " + home);
        }
    }
}
