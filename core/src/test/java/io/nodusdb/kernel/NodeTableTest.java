package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeTableTest {

    @Test
    void unwrittenNodesAreEmptyWithNoBlockAndNoSet() {
        NodeTable table = new NodeTable(Arena.ofAuto(), 16);
        table.ensureCapacity(200);

        for (int node : new int[] {0, 15, 16, 199, 500}) {
            long slot = table.read(node);
            assertEquals(0, NodeTable.degreeOf(slot), "degree of " + node);
            assertEquals(NodeTable.NO_BLOCK, NodeTable.blockOf(slot), "block of " + node);
            assertFalse(NodeTable.isSet(slot), "set flag of " + node);
        }
    }

    @Test
    void lowDegreeWritesRecoverDegreeAndBlock() {
        NodeTable table = new NodeTable(Arena.ofAuto(), 16);
        table.write(3, 15, 7);
        table.write(4, 0, NodeTable.NO_BLOCK);

        assertEquals(15, NodeTable.degreeOf(table.read(3)));
        assertEquals(7, NodeTable.blockOf(table.read(3)));
        assertFalse(NodeTable.isSet(table.read(3)));
        assertEquals(NodeTable.NO_BLOCK, NodeTable.blockOf(table.read(4)));
    }

    @Test
    void setWritesCarryTheFlagAndTheHandle() {
        NodeTable table = new NodeTable(Arena.ofAuto(), 16);
        table.writeSet(5, Integer.MAX_VALUE, 1_234_567);

        long slot = table.read(5);
        assertTrue(NodeTable.isSet(slot));
        assertEquals(Integer.MAX_VALUE, NodeTable.degreeOf(slot));
        assertEquals(1_234_567, NodeTable.handleOf(slot));
    }

    @Test
    void growthKeepsEarlierWrites() {
        NodeTable table = new NodeTable(Arena.ofAuto(), 16);
        table.write(9, 4, 2);
        table.writeSet(10, 20, 77);

        table.ensureCapacity(10_000);

        assertEquals(2, NodeTable.blockOf(table.read(9)));
        assertEquals(77, NodeTable.handleOf(table.read(10)));
        assertEquals(0, NodeTable.degreeOf(table.read(9_999)));
    }
}
