package io.nodusdb.capi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HandleTableTest {

    private static final int READERS = 6;
    private static final long TIMEOUT_SECONDS = 30;

    @Test
    void aHandleReadsBackTheValueItWasOpenedWith() {
        HandleTable<String> table = new HandleTable<>();
        String value = "graph";

        long handle = table.open(value);

        assertSame(value, table.get(handle));
    }

    @Test
    void closingReturnsTheValueAndInvalidatesTheHandle() {
        HandleTable<String> table = new HandleTable<>();
        long handle = table.open("graph");

        assertEquals("graph", table.close(handle));

        assertThrows(IllegalArgumentException.class, () -> table.get(handle));
        assertThrows(IllegalArgumentException.class, () -> table.close(handle));
    }

    @Test
    void readersNeverSeeAWrongValueWhileHandlesAreOpenedClosedAndTheTableGrows() throws Exception {
        HandleTable<Long> table = new HandleTable<>();
        long[] stable = new long[8];
        for (int i = 0; i < stable.length; i++) {
            stable[i] = table.open((long) i);
        }
        AtomicBoolean stop = new AtomicBoolean();
        ExecutorService pool = Executors.newFixedThreadPool(READERS + 1);
        try {
            List<Future<Long>> readers = new ArrayList<>();
            for (int r = 0; r < READERS; r++) {
                readers.add(pool.submit(() -> {
                    long reads = 0;
                    while (!stop.get()) {
                        for (int i = 0; i < stable.length; i++) {
                            assertEquals((long) i, table.get(stable[i]));
                            reads++;
                        }
                    }
                    return reads;
                }));
            }
            Future<?> churn = pool.submit(() -> {
                for (int round = 0; round < 20_000; round++) {
                    long handle = table.open(1_000L + round);
                    assertEquals(1_000L + round, table.get(handle));
                    table.close(handle);
                }
            });
            churn.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            stop.set(true);
            for (Future<Long> reader : readers) {
                assertEquals(true, reader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS) > 0);
            }
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }
    }

    @Test
    void manyOpenHandlesGrowTheTableWithoutLosingAny() {
        HandleTable<Integer> table = new HandleTable<>();
        long[] handles = new long[1_000];
        for (int i = 0; i < handles.length; i++) {
            handles[i] = table.open(i);
        }

        for (int i = 0; i < handles.length; i++) {
            assertEquals(i, table.get(handles[i]));
        }
    }
}
