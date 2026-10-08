package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainObject;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainRingTest {

    private static ChainObject object(long seq, int tuples) {
        long first = 1 + seq * 100;
        return ChainCodec.seal(new ChainHeader(ChainKind.RECORDS, seq, 1, 1, ChainHash.ZERO, ChainBuilder.KEY_ID),
                new ChainBody.Records(first, first + tuples, ChainBuilder.transaction(first, tuples)),
                ChainBuilder.signingKey());
    }

    private static long cost(ChainObject object) {
        return object.encoded().length * 2L;
    }

    @Test
    void anObjectIsFoundBySequenceNumberAndAnAbsentOneIsNot() {
        ChainRing ring = new ChainRing(1 << 20);
        ChainObject two = object(2, 1);

        ring.put(two);

        assertSame(two, ring.get(2).orElseThrow());
        assertEquals(Optional.empty(), ring.get(1));
        assertEquals(Optional.empty(), ring.get(3));
        assertEquals(1, ring.size());
    }

    @Test
    void theOldestObjectsAreEvictedWhenTheBudgetIsExceeded() {
        ChainObject a = object(1, 5);
        ChainObject b = object(2, 5);
        ChainObject c = object(3, 5);
        ChainRing ring = new ChainRing(cost(a) + cost(b) + cost(c) / 2);

        ring.put(a);
        ring.put(b);
        ring.put(c);

        assertEquals(Optional.empty(), ring.get(1));
        assertSame(b, ring.get(2).orElseThrow());
        assertSame(c, ring.get(3).orElseThrow());
        assertEquals(cost(b) + cost(c), ring.bytes());
    }

    @Test
    void theNewestObjectIsKeptEvenWhenItAloneExceedsTheBudget() {
        ChainObject big = object(1, 50);
        ChainRing ring = new ChainRing(10);

        ring.put(big);
        ring.put(object(2, 50));

        assertEquals(Optional.empty(), ring.get(1));
        assertEquals(1, ring.size());
        assertTrue(ring.get(2).isPresent());
    }

    @Test
    void replacingAnObjectDoesNotCountItsBytesTwice() {
        ChainObject first = object(1, 5);
        ChainObject again = object(1, 5);
        ChainRing ring = new ChainRing(1 << 20);

        ring.put(first);
        ring.put(again);

        assertEquals(1, ring.size());
        assertEquals(cost(again), ring.bytes());
        assertSame(again, ring.get(1).orElseThrow());
    }

    @Test
    void aNonPositiveBudgetIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new ChainRing(0));
        assertThrows(IllegalArgumentException.class, () -> new ChainRing(-1));
    }

    @Test
    void concurrentPutsAndGetsNeverFailAndStayWithinTheBudget() throws Exception {
        ChainObject sample = object(1, 5);
        long budget = cost(sample) * 20;
        ChainRing ring = new ChainRing(budget);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch go = new CountDownLatch(1);
            Future<?> writer = pool.submit(() -> {
                go.await();
                for (long seq = 1; seq <= 2_000; seq++) {
                    ring.put(object(seq, 5));
                }
                return null;
            });
            Future<?> reader = pool.submit(() -> {
                go.await();
                for (long seq = 1; seq <= 2_000; seq++) {
                    ring.get(seq);
                    assertTrue(ring.bytes() <= budget + cost(sample));
                }
                return null;
            });
            go.countDown();
            writer.get(60, TimeUnit.SECONDS);
            reader.get(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertTrue(ring.size() <= 21);
        assertTrue(ring.get(2_000).isPresent());
    }
}
