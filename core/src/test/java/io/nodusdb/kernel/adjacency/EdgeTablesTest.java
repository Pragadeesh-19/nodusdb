package io.nodusdb.kernel.adjacency;

import io.nodusdb.kernel.memory.MemoryBudget;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeTablesTest {

    private static final int RELATION = 3;

    private final MemoryBudget budget = MemoryBudget.unlimited();
    private final EdgeTables tables = new EdgeTables(Arena.ofAuto(), budget);

    private void reserveFor(long object, long subject) {
        Headroom out = new Headroom();
        Headroom in = new Headroom();
        tables.accumulateOutgoing(object, 1, out);
        tables.accumulateIncoming(subject, 1, in);
        tables.reserve(out, in);
    }

    private boolean add(int object, int subject) {
        reserveFor(object, subject);
        return tables.add(object, EdgeKey.pack(RELATION, 0, subject), subject, EdgeKey.pack(RELATION, 0, object));
    }

    private boolean remove(int object, int subject) {
        return tables.remove(object, EdgeKey.pack(RELATION, 0, subject), subject,
                EdgeKey.pack(RELATION, 0, object));
    }

    @Test
    void anAddIsVisibleInBothDirections() {
        tables.ensureCapacity(10);

        assertTrue(add(1, 2));

        assertTrue(tables.contains(1, EdgeKey.pack(RELATION, 0, 2)));
        assertTrue(tables.containsIncoming(2, EdgeKey.pack(RELATION, 0, 1)));
        assertEquals(1, tables.degree(1));
        assertEquals(1, tables.inDegree(2));
        assertEquals(EdgeKey.pack(RELATION, 0, 2), tables.outgoingKeyAt(1, 0));
        assertEquals(EdgeKey.pack(RELATION, 0, 1), tables.incomingKeyAt(2, 0));
    }

    @Test
    void aDuplicateAddAndAMissingRemoveChangeNothing() {
        tables.ensureCapacity(10);
        add(1, 2);

        assertFalse(add(1, 2));
        assertFalse(remove(1, 3));

        assertEquals(1, tables.degree(1));
        assertEquals(1, tables.inDegree(2));
    }

    @Test
    void aRemoveClearsBothDirections() {
        tables.ensureCapacity(10);
        add(1, 2);

        assertTrue(remove(1, 2));

        assertEquals(0, tables.degree(1));
        assertEquals(0, tables.inDegree(2));
        assertFalse(tables.contains(1, EdgeKey.pack(RELATION, 0, 2)));
    }

    @Test
    void aHubSurvivesPromotionAndStaysMirrored() {
        tables.ensureCapacity(100);
        for (int subject = 1; subject <= 60; subject++) {
            add(0, subject);
        }

        assertTrue(tables.isHighDegree(0));
        assertEquals(60, tables.degree(0));
        for (int subject = 1; subject <= 60; subject++) {
            assertEquals(1, tables.inDegree(subject));
        }
    }

    @Test
    void bulkLoadedNodesMatchTheirDeclaredDegrees() {
        tables.ensureCapacity(4);
        tables.prepareBulkLoad(new int[] {2, 0, 0, 0}, new int[] {0, 1, 1, 0});
        long first = EdgeKey.pack(0, 0, 1);
        long second = EdgeKey.pack(0, 0, 2);

        tables.fillBulkNode(true, 0, new long[] {first, second}, 2);
        tables.fillBulkNode(false, 1, new long[] {EdgeKey.pack(0, 0, 0)}, 1);
        tables.fillBulkNode(false, 2, new long[] {EdgeKey.pack(0, 0, 0)}, 1);

        assertEquals(2, tables.degree(0));
        assertTrue(tables.contains(0, first));
        assertTrue(tables.containsIncoming(2, EdgeKey.pack(0, 0, 0)));
    }
}
