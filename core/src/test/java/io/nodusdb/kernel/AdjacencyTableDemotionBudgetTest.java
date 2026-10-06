package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdjacencyTableDemotionBudgetTest {

    private static final int HUB = 0;
    private static final long FIRST_TARGET = 100;
    private static final int HUB_DEGREE = 16;

    @Test
    void demotionHappensWhenASlabBlockIsAvailable() {
        AdjacencyTable table = withFullSlabAndPromotedHub(MemoryBudget.unlimited());

        removeDownTo(table, DEMOTION_DEGREE());

        assertFalse(table.isHighDegree(HUB));
        assertEquals(DEMOTION_DEGREE(), table.degreeOf(HUB));
    }

    @Test
    void demotionIsSkippedWhenNoSlabBlockCanBeAllocated() {
        MemoryBudget probe = MemoryBudget.unlimited();
        withFullSlabAndPromotedHub(probe);
        AdjacencyTable table = withFullSlabAndPromotedHub(MemoryBudget.limitedTo(probe.used()));

        removeDownTo(table, DEMOTION_DEGREE());

        assertTrue(table.isHighDegree(HUB), "the node stays a set when demotion has no block");
        assertEquals(DEMOTION_DEGREE(), table.degreeOf(HUB));
        for (int i = 0; i < DEMOTION_DEGREE(); i++) {
            assertTrue(table.contains(HUB, FIRST_TARGET + i), "neighbor " + i + " lost");
        }
    }

    @Test
    void aSetKeptAfterASkippedDemotionCanBeEmptiedAndRefilled() {
        MemoryBudget probe = MemoryBudget.unlimited();
        withFullSlabAndPromotedHub(probe);
        AdjacencyTable table = withFullSlabAndPromotedHub(MemoryBudget.limitedTo(probe.used()));

        removeDownTo(table, 0);
        assertEquals(0, table.degreeOf(HUB));
        assertEquals(0, table.neighborsOf(HUB, new long[HUB_DEGREE]).length);
        for (int i = 0; i < HUB_DEGREE; i++) {
            table.reserveForAdd(HUB);
            assertTrue(table.add(HUB, FIRST_TARGET + i));
        }

        assertEquals(HUB_DEGREE, table.degreeOf(HUB));
        for (int i = 0; i < HUB_DEGREE; i++) {
            assertTrue(table.contains(HUB, FIRST_TARGET + i));
        }
    }

    private static int DEMOTION_DEGREE() {
        return AdjacencyTable.DEMOTION_DEGREE;
    }

    private static AdjacencyTable withFullSlabAndPromotedHub(MemoryBudget budget) {
        AdjacencyTable table = new AdjacencyTable(Arena.ofAuto(), budget);
        table.ensureCapacity(20);
        for (int i = 0; i < HUB_DEGREE; i++) {
            table.reserveForAdd(HUB);
            table.add(HUB, FIRST_TARGET + i);
        }
        for (int node = 1; node <= 16; node++) {
            table.reserveForAdd(node);
            table.add(node, 500 + node);
        }
        return table;
    }

    private static void removeDownTo(AdjacencyTable table, int degree) {
        for (int i = HUB_DEGREE - 1; table.degreeOf(HUB) > degree; i--) {
            assertTrue(table.remove(HUB, FIRST_TARGET + i));
        }
    }
}
