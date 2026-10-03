package io.nodusdb.capi;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionTest {

    @Test
    void khopResultsGrowWithoutLimit() {
        GraphSession session = new GraphSession();
        int leaves = 5_000;
        for (long v = 1; v <= leaves; v++) {
            session.addEdge(0L, v);
        }

        int total = session.khop(0L, 1);

        assertEquals(leaves, total);
        Set<Long> reached = new HashSet<>();
        for (int i = 0; i < total; i++) {
            reached.add(session.result(i));
        }
        assertEquals(leaves, reached.size());
    }

    @Test
    void commonNeighborsResultsGrowWithoutLimit() {
        GraphSession session = new GraphSession();
        int shared = 1_000;
        for (long v = 0; v < shared; v++) {
            session.addEdge(0L, v);
            session.addEdge(1L, v);
        }

        int total = session.commonNeighbors(0L, 1L);

        assertEquals(shared, total);
        Set<Long> common = new HashSet<>();
        for (int i = 0; i < total; i++) {
            common.add(session.result(i));
        }
        assertEquals(shared, common.size());
    }

    @Test
    void invalidNodeIdsSurfaceAsExceptionsForTheBoundaryToCatch() {
        GraphSession session = new GraphSession();

        assertThrows(IllegalArgumentException.class, () -> session.addEdge(-1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> session.khop(-1L, 2));
        assertThrows(IllegalArgumentException.class, () -> session.commonNeighbors(0L, -5L));
    }

    @Test
    void degreesAndDirectionAreReportedThroughTheSession() {
        GraphSession session = new GraphSession();
        session.addEdge(3L, 4L);
        session.addEdge(3L, 5L);

        assertEquals(2, session.degree(3L));
        assertEquals(1, session.inDegree(4L));
        assertTrue(session.removeEdge(3L, 4L));
        assertEquals(1, session.degree(3L));
    }
}
