package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelConcurrencyTest {

    private static final int HUB_EDGES = 20_000;
    private static final int READERS = 3;

    @Test
    void readersSeeMonotoneDegreeAndReachWhileOneWriterAddsEdges() throws InterruptedException {
        GraphKernel graph = new GraphKernel();
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < READERS; r++) {
            Thread reader = new Thread(() -> {
                try {
                    long[] reach = new long[HUB_EDGES + 1];
                    int lastDegree = 0;
                    int lastReach = 0;
                    while (writing.get()) {
                        int degree = graph.getDegree(0);
                        int count = graph.kHop(0, 1, reach);
                        assertTrue(degree >= lastDegree, "degree went backwards: " + degree + " < " + lastDegree);
                        assertTrue(count >= lastReach, "reach went backwards: " + count + " < " + lastReach);
                        lastDegree = degree;
                        lastReach = count;
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            readers.add(reader);
            reader.start();
        }

        for (long v = 1; v <= HUB_EDGES; v++) {
            graph.addEdge(0, v);
        }
        writing.set(false);
        for (Thread reader : readers) {
            reader.join();
        }

        assertNull(failure.get(), () -> "reader failed: " + failure.get());
        assertEquals(HUB_EDGES, graph.getDegree(0));
        assertEquals(1, graph.getInDegree(HUB_EDGES));
    }

    @Test
    void readersSurviveConcurrentRemovalsWithoutFailing() throws InterruptedException {
        GraphKernel graph = new GraphKernel();
        for (long v = 1; v <= HUB_EDGES; v++) {
            graph.addEdge(0, v);
        }
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                long[] reach = new long[HUB_EDGES + 1];
                while (writing.get()) {
                    int degree = graph.getDegree(0);
                    assertTrue(degree >= 0 && degree <= HUB_EDGES, "degree out of range: " + degree);
                    int count = graph.kHop(0, 1, reach);
                    assertTrue(count >= 0 && count <= HUB_EDGES, "reach out of range: " + count);
                    graph.hasEdge(0, 1);
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });
        reader.start();

        for (long v = 1; v <= HUB_EDGES; v++) {
            graph.removeEdge(0, v);
        }
        writing.set(false);
        reader.join();

        assertNull(failure.get(), () -> "reader failed: " + failure.get());
        assertEquals(0, graph.getDegree(0));
    }
}
