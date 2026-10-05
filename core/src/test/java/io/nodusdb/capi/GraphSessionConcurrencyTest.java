package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionConcurrencyTest {

    private static final int WRITERS = 4;
    private static final int EDGES_PER_WRITER = 2_000;

    @Test
    void concurrentWritersOnOneSessionLeaveTheExactState() throws InterruptedException {
        GraphSession session = new GraphSession(new GraphKernel());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> writers = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
            long base = w * 1_000_000L;
            Thread writer = new Thread(() -> {
                try {
                    for (int i = 0; i < EDGES_PER_WRITER; i++) {
                        assertTrue(session.addEdge(0, base + i));
                    }
                    for (int i = 0; i < EDGES_PER_WRITER; i += 2) {
                        assertTrue(session.removeEdge(0, base + i));
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            writers.add(writer);
            writer.start();
        }
        for (Thread writer : writers) {
            writer.join();
        }

        assertNull(failure.get(), () -> "writer failed: " + failure.get());
        int kept = WRITERS * (EDGES_PER_WRITER / 2);
        assertEquals(kept, session.degree(0));
        for (int w = 0; w < WRITERS; w++) {
            long base = w * 1_000_000L;
            for (int i = 0; i < EDGES_PER_WRITER; i++) {
                assertEquals(i % 2 == 1, session.hasEdge(0, base + i), "writer " + w + " edge " + i);
            }
        }
        session.close();
    }

    @Test
    void closeIsIdempotentAndLaterCallsAreRejected() {
        GraphSession session = new GraphSession(new GraphKernel());
        assertTrue(session.addEdge(1, 2));
        session.close();
        session.close();

        assertThrows(IllegalStateException.class, () -> session.addEdge(1, 3));
        assertThrows(IllegalStateException.class, () -> session.degree(1));
        assertThrows(IllegalStateException.class, () -> session.khop(1, 1));
        assertThrows(IllegalStateException.class, () -> session.commonNeighbors(1, 2));
        assertThrows(IllegalStateException.class, session::checkpoint);
    }
}
