package io.nodusdb.capi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionsTest {

    @Test
    void openedHandlesAreNonZeroAndDistinct() {
        GraphSessions sessions = new GraphSessions();

        long first = sessions.open();
        long second = sessions.open();

        assertNotEquals(0L, first);
        assertNotEquals(first, second);
        assertNotNull(sessions.get(first));
        assertNotNull(sessions.get(second));
    }

    @Test
    void handleFromOneSessionCarriesItsOwnGraph() {
        GraphSessions sessions = new GraphSessions();
        long a = sessions.open();
        long b = sessions.open();
        sessions.get(a).addEdge(1L, 2L);

        assertTrue(sessions.get(a).hasEdge(1L, 2L));
        assertFalse(sessions.get(b).hasEdge(1L, 2L));
    }

    @Test
    void closedHandleIsRejected() {
        GraphSessions sessions = new GraphSessions();
        long handle = sessions.open();
        sessions.close(handle);

        assertThrows(IllegalArgumentException.class, () -> sessions.get(handle));
    }

    @Test
    void staleHandleIsRejectedAfterSlotIsReused() {
        GraphSessions sessions = new GraphSessions();
        long stale = sessions.open();
        sessions.close(stale);

        long fresh = sessions.open();

        assertEquals(stale & 0xFFFF_FFFFL, fresh & 0xFFFF_FFFFL, "slot should be reused");
        assertNotEquals(stale, fresh);
        assertThrows(IllegalArgumentException.class, () -> sessions.get(stale));
        assertNotNull(sessions.get(fresh));
    }

    @Test
    void doubleCloseIsRejected() {
        GraphSessions sessions = new GraphSessions();
        long handle = sessions.open();
        sessions.close(handle);

        assertThrows(IllegalArgumentException.class, () -> sessions.close(handle));
    }

    @Test
    void zeroAndForeignHandlesAreRejected() {
        GraphSessions sessions = new GraphSessions();
        sessions.open();

        assertThrows(IllegalArgumentException.class, () -> sessions.get(0L));
        assertThrows(IllegalArgumentException.class, () -> sessions.get(99L));
        assertThrows(IllegalArgumentException.class, () -> sessions.close(-1L));
    }

    @Test
    void registryGrowsBeyondInitialSlots() {
        GraphSessions sessions = new GraphSessions();
        long[] handles = new long[100];
        for (int i = 0; i < handles.length; i++) {
            handles[i] = sessions.open();
            sessions.get(handles[i]).addEdge(i, i + 1L);
        }

        for (int i = 0; i < handles.length; i++) {
            assertTrue(sessions.get(handles[i]).hasEdge(i, i + 1L));
        }
    }
}
