package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodeTableTest {

    @Test
    void unwrittenNodesAreEmptyWithNoBlock() {
        NodeTable table = new NodeTable(Arena.ofAuto(), 16);
        table.ensureCapacity(200);

        for (int node : new int[] {0, 15, 16, 199, -1, 500}) {
            long slot = table.read(node);
            assertEquals(0, NodeTable.degreeOf(slot), "degree of " + node);
            assertEquals(NodeTable.NO_BLOCK, NodeTable.blockOf(slot), "block of " + node);
        }
    }

    @Test
    void degreeAndBlockAreIndependentlyRecovered() {
        NodeTable table = new NodeTable(Arena.ofAuto(), 16);
        table.write(3, 15, 7);
        table.write(4, Integer.MAX_VALUE, NodeTable.NO_BLOCK);
        table.write(5, 0, Integer.MAX_VALUE);

        assertEquals(15, NodeTable.degreeOf(table.read(3)));
        assertEquals(7, NodeTable.blockOf(table.read(3)));
        assertEquals(Integer.MAX_VALUE, NodeTable.degreeOf(table.read(4)));
        assertEquals(NodeTable.NO_BLOCK, NodeTable.blockOf(table.read(4)));
        assertEquals(0, NodeTable.degreeOf(table.read(5)));
        assertEquals(Integer.MAX_VALUE, NodeTable.blockOf(table.read(5)));
    }

    @Test
    void growthKeepsEarlierWrites() {
        NodeTable table = new NodeTable(Arena.ofAuto(), 16);
        table.write(9, 4, 2);

        table.ensureCapacity(10_000);

        assertEquals(4, NodeTable.degreeOf(table.read(9)));
        assertEquals(2, NodeTable.blockOf(table.read(9)));
        assertEquals(0, NodeTable.degreeOf(table.read(9_999)));
    }
}
