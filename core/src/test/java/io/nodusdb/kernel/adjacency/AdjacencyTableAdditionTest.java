package io.nodusdb.kernel.adjacency;

import io.nodusdb.kernel.memory.MemoryBudget;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdjacencyTableAdditionTest {

    private final AdjacencyTable table = new AdjacencyTable(Arena.ofAuto(), MemoryBudget.unlimited());

    private void reserveFor(long node) {
        Headroom headroom = new Headroom();
        table.accumulateHeadroom(node, 1, headroom);
        table.reserve(headroom);
    }

    @Test
    void addAbsentStoresTheNeighborWithoutAMembershipTest() {
        table.ensureCapacity(4);
        reserveFor(1);

        table.addAbsent(1, 7);

        assertTrue(table.contains(1, 7));
        assertEquals(1, table.degreeOf(1));
    }

    @Test
    void addAbsentAndAddBuildTheSameTableAcrossPromotion() {
        AdjacencyTable other = new AdjacencyTable(Arena.ofAuto(), MemoryBudget.unlimited());
        table.ensureCapacity(2);
        other.ensureCapacity(2);
        for (int neighbor = 100; neighbor < 140; neighbor++) {
            reserveFor(0);
            table.addAbsent(0, neighbor);
            Headroom headroom = new Headroom();
            other.accumulateHeadroom(0, 1, headroom);
            other.reserve(headroom);
            assertTrue(other.add(0, neighbor));
        }

        assertEquals(other.degreeOf(0), table.degreeOf(0));
        assertTrue(table.isHighDegree(0));
        for (int neighbor = 100; neighbor < 140; neighbor++) {
            assertTrue(table.contains(0, neighbor));
        }
    }

    @Test
    void addStillRefusesADuplicate() {
        table.ensureCapacity(2);
        reserveFor(0);
        assertTrue(table.add(0, 5));

        assertFalse(table.add(0, 5));
        assertEquals(1, table.degreeOf(0));
    }

    @Test
    void addAbsentOfAPresentNeighborIsCaughtWhenAssertionsAreOn() {
        table.ensureCapacity(2);
        reserveFor(0);
        table.addAbsent(0, 5);

        assertThrows(AssertionError.class, () -> table.addAbsent(0, 5));
    }

    @Test
    void headroomIsEmptyUntilSomethingIsAccumulatedAndClearsAgain() {
        table.ensureCapacity(2);
        Headroom headroom = new Headroom();
        assertTrue(headroom.isEmpty());

        table.accumulateHeadroom(0, 1, headroom);
        assertFalse(headroom.isEmpty());
        assertEquals(1, headroom.slabBlocks());

        headroom.clear();
        assertTrue(headroom.isEmpty());

        table.accumulateHeadroom(0, 20, headroom);
        assertFalse(headroom.isEmpty());
        headroom.clear();
        assertTrue(headroom.isEmpty());
    }
}
