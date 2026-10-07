package io.nodusdb.capi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HandleTableTest {

    private static final int READERS = 6;
    private static final int HELD_HANDLES = 1 << 17;
    private static final int HELD_CHECK_STRIDE = 97;
    private static final int CHURN_ROUNDS = 200;
    private static final int CHURN_BATCH = 64;
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
        CountDownLatch readersRunning = new CountDownLatch(READERS);
        ExecutorService pool = Executors.newFixedThreadPool(READERS + 1);
        try {
            List<Future<Long>> readers = new ArrayList<>();
            for (int r = 0; r < READERS; r++) {
                readers.add(pool.submit(() -> {
                    long reads = readAll(table, stable);
                    readersRunning.countDown();
                    while (!stop.get()) {
                        reads += readAll(table, stable);
                    }
                    return reads;
                }));
            }
            Future<?> churn = pool.submit(() -> {
                assertTrue(readersRunning.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "the readers never started");
                holdManyHandlesThenRelease(table);
                for (int round = 0; round < CHURN_ROUNDS; round++) {
                    openVerifyAndCloseBatch(table, round);
                }
                return null;
            });
            churn.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            stop.set(true);
            for (Future<Long> reader : readers) {
                assertTrue(reader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS) >= stable.length);
            }
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }
    }

    private static long readAll(HandleTable<Long> table, long[] handles) {
        for (int i = 0; i < handles.length; i++) {
            assertEquals((long) i, table.get(handles[i]));
        }
        return handles.length;
    }

    private static void holdManyHandlesThenRelease(HandleTable<Long> table) {
        long[] held = new long[HELD_HANDLES];
        for (int i = 0; i < held.length; i++) {
            held[i] = table.open(-1L - i);
        }
        for (int i = 0; i < held.length; i += HELD_CHECK_STRIDE) {
            assertEquals(-1L - i, table.get(held[i]));
        }
        for (long handle : held) {
            table.close(handle);
        }
    }

    private static void openVerifyAndCloseBatch(HandleTable<Long> table, int round) {
        long[] opened = new long[CHURN_BATCH];
        for (int i = 0; i < opened.length; i++) {
            long value = 1_000L + (long) round * CHURN_BATCH + i;
            opened[i] = table.open(value);
            assertEquals(value, table.get(opened[i]));
        }
        for (long handle : opened) {
            table.close(handle);
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
