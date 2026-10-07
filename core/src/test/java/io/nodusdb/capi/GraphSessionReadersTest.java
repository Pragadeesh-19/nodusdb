package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionReadersTest {

    private static final int READERS = 6;
    private static final int CHAINS = READERS;
    private static final int CHAIN_LENGTH = 200;
    private static final long TIMEOUT_SECONDS = 60;

    private static long chainNode(int chain, int position) {
        return 10_000L * (chain + 1) + position;
    }

    @Test
    void parallelReadersEachGetTheirOwnResultsWhileAWriterKeepsWriting() throws Exception {
        GraphSession session = new GraphSession(new GraphKernel());
        for (int chain = 0; chain < CHAINS; chain++) {
            for (int position = 0; position < CHAIN_LENGTH; position++) {
                session.addEdge(chainNode(chain, position), chainNode(chain, position + 1));
            }
        }
        AtomicBoolean stop = new AtomicBoolean();
        ExecutorService pool = Executors.newFixedThreadPool(READERS + 1);
        try {
            Future<?> writer = pool.submit(() -> {
                for (long i = 0; !stop.get(); i++) {
                    session.addEdge(900_000L + (i % 500), 900_001L + (i % 500));
                    session.removeEdge(900_000L + (i % 500), 900_001L + (i % 500));
                }
            });
            List<Future<Integer>> readers = new ArrayList<>();
            for (int r = 0; r < READERS; r++) {
                int chain = r;
                readers.add(pool.submit(() -> {
                    long[] expected = new long[CHAIN_LENGTH];
                    for (int position = 0; position < CHAIN_LENGTH; position++) {
                        expected[position] = chainNode(chain, position + 1);
                    }
                    for (int round = 0; round < 100; round++) {
                        int count = session.khop(chainNode(chain, 0), CHAIN_LENGTH);
                        assertEquals(CHAIN_LENGTH, count);
                        long[] found = new long[count];
                        for (int i = 0; i < count; i++) {
                            found[i] = session.result(i);
                        }
                        Arrays.sort(found);
                        assertArrayEquals(expected, found, "reader " + chain + " saw another reader's results");
                    }
                    return CHAIN_LENGTH;
                }));
            }
            for (Future<Integer> reader : readers) {
                assertEquals(CHAIN_LENGTH, reader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            stop.set(true);
            writer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            stop.set(true);
            pool.shutdownNow();
            session.close();
        }
    }

    @Test
    void aReaderThatOutgrowsItsBufferGrowsOnlyItsOwn() {
        GraphSession session = new GraphSession(new GraphKernel());
        for (int target = 1; target <= 500; target++) {
            session.addEdge(0, target);
        }

        int count = session.khop(0, 1);

        assertEquals(500, count);
        assertTrue(session.result(499) > 0);
        session.close();
    }
}
