package io.nodusdb.authz;

import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.closure.ClosureLimits;
import io.nodusdb.kernel.closure.ClosureStats;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReachabilityConcurrencyTest {

    private static final int READERS = 4;
    private static final int CHAIN = 12;
    private static final int SPARES = 30;
    private static final int TOGGLES = 20_000;
    private static final int UNBOUNDED_DEPTH = 10_000;
    private static final long TIMEOUT_SECONDS = 120;

    @Test
    void readersSeeStableAnswersWhileNestingChurnsAndTheIndexRebuilds() throws Exception {
        TupleStore store = TupleStore.open(new GraphKernel(GraphKernel.NO_MEMORY_LIMIT,
                ClosureLimits.DEFAULT.withWorkBudget(2)), UNBOUNDED_DEPTH);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        TupleTransaction setup = new TupleTransaction();
        for (int i = 0; i < CHAIN; i++) {
            setup.add("group:c" + i, "member", "group:c" + (i + 1), "member");
        }
        setup.add("group:c" + CHAIN, "member", "user:alice");
        setup.add("group:empty", "member", "user:bob");
        for (int i = 0; i < SPARES; i++) {
            setup.add("group:s" + i, "member", "group:t" + i, "member");
        }
        store.write(setup);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong reads = new AtomicLong();
        CountDownLatch started = new CountDownLatch(READERS);
        ExecutorService pool = Executors.newFixedThreadPool(READERS);
        try {
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < READERS; r++) {
                readers.add(pool.submit(() -> {
                    started.countDown();
                    while (!stop.get()) {
                        int top = (int) (reads.get() % CHAIN);
                        assertTrue(store.check("group:c" + top, "member", "user:alice"), "alice lost c" + top);
                        assertTrue(!store.check("group:c" + top, "member", "user:bob"), "bob gained c" + top);
                        reads.incrementAndGet();
                    }
                    return null;
                }));
            }
            assertTrue(started.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            for (int i = 0; i < TOGGLES; i++) {
                String parent = "group:c" + (i % CHAIN);
                String spare = "group:s" + (i % SPARES);
                if ((i / SPARES) % 2 == 0) {
                    store.add(parent, "member", spare, "member");
                } else {
                    store.remove(parent, "member", spare, "member");
                }
            }
            stop.set(true);
            for (Future<?> reader : readers) {
                reader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }

        ClosureStats stats = store.kernel().closureStats();
        assertTrue(reads.get() > READERS, "reads=" + reads.get());
        assertTrue(stats.dirtyMarks() > 0, String.valueOf(stats));
        assertEquals("", stats.failure());
        store.kernel().close();
    }
}
