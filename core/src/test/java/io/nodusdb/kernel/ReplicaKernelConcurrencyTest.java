package io.nodusdb.kernel;

import io.nodusdb.log.record.RecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class ReplicaKernelConcurrencyTest {

    private static final int TRANSACTIONS = 3_000;
    private static final int PER_CHUNK = 100;
    private static final int READERS = 3;
    private static final int RECORDS_PER_TRANSACTION = 4;
    private static final int FIRST_TARGET = 10_000;
    private static final int SECOND_TARGET = 20_000;
    private static final int THIRD_TARGET = 30_000;
    private static final long MAX_READ_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);

    private static void threeEdgeTransaction(ReplicatedStream stream, int node) {
        stream.transaction(batch -> {
            batch.tuple(RecordType.TUPLE_ADD, node, 0, 0, FIRST_TARGET + node);
            batch.tuple(RecordType.TUPLE_ADD, node, 0, 0, SECOND_TARGET + node);
            batch.tuple(RecordType.TUPLE_ADD, node, 0, 0, THIRD_TARGET + node);
        });
    }

    @Test
    void everyTransactionIsOneWriteSection() {
        ReplicatedStream stream = new ReplicatedStream();
        for (int node = 0; node < 50; node++) {
            threeEdgeTransaction(stream, node);
        }
        ReplicatedStream.Chunk chunk = stream.drain();
        long before = replica.readStart();

        chunk.applyTo(replica);

        long after = replica.readStart();
        assertEquals(2L * 50, after - before);
    }

    @Test
    void aChunkOfAutocommitTuplesCostsOneWriteSectionPerTuple() {
        ReplicatedStream stream = new ReplicatedStream();
        for (int node = 0; node < 40; node++) {
            stream.add(node, node + 1);
        }
        ReplicatedStream.Chunk chunk = stream.drain();
        long before = replica.readStart();

        chunk.applyTo(replica);

        assertEquals(2L * 40, replica.readStart() - before);
    }

    @Test
    void aRejectedChunkEntersNoWriteSection() {
        ReplicatedStream stream = new ReplicatedStream();
        stream.add(1, 2);
        ReplicatedStream.Chunk chunk = stream.drain();
        byte[] damaged = chunk.records().clone();
        damaged[10] ^= 0x01;
        long before = replica.readStart();

        assertThrows(IllegalArgumentException.class, () -> chunk.withRecords(damaged).applyTo(replica));

        assertEquals(before, replica.readStart());
    }

    @Test
    void readersNeverSeeAHalfAppliedTransactionWhileChunksAreApplied() throws Exception {
        ReplicatedStream stream = new ReplicatedStream();
        List<ReplicatedStream.Chunk> chunks = new ArrayList<>();
        for (int node = 0; node < TRANSACTIONS; node++) {
            threeEdgeTransaction(stream, node);
            if ((node + 1) % PER_CHUNK == 0) {
                chunks.add(stream.drain());
            }
        }
        AtomicBoolean done = new AtomicBoolean();
        AtomicInteger violations = new AtomicInteger();
        AtomicLong validWindows = new AtomicLong();
        AtomicLong worstRead = new AtomicLong();
        CountDownLatch started = new CountDownLatch(READERS);
        ExecutorService pool = Executors.newFixedThreadPool(READERS);
        try {
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < READERS; r++) {
                long seed = r + 1;
                readers.add(pool.submit(() -> {
                    Random random = new Random(seed);
                    started.countDown();
                    while (!done.get()) {
                        int inFlight = (int) Math.min(replica.appliedLsn() / RECORDS_PER_TRANSACTION,
                                TRANSACTIONS - 1);
                        int node = random.nextInt(4) == 0 ? random.nextInt(TRANSACTIONS) : inFlight;
                        long begin = System.nanoTime();
                        long token = replica.readStart();
                        int out = replica.getDegree(node);
                        int first = replica.getInDegree(FIRST_TARGET + node);
                        int second = replica.getInDegree(SECOND_TARGET + node);
                        int third = replica.getInDegree(THIRD_TARGET + node);
                        boolean valid = replica.readStillValid(token);
                        worstRead.accumulateAndGet(System.nanoTime() - begin, Math::max);
                        if (valid) {
                            validWindows.incrementAndGet();
                            boolean whole = first == second && second == third && (first == 0 || first == 1)
                                    && out == 3 * first;
                            if (!whole) {
                                violations.incrementAndGet();
                            }
                        }
                    }
                }));
            }
            started.await();
            for (ReplicatedStream.Chunk chunk : chunks) {
                chunk.applyTo(replica);
            }
            done.set(true);
            for (Future<?> reader : readers) {
                reader.get(30, TimeUnit.SECONDS);
            }
        } finally {
            done.set(true);
            pool.shutdownNow();
        }

        assertEquals(0, violations.get());
        assertTrue(validWindows.get() > 0);
        assertTrue(worstRead.get() < MAX_READ_NANOS, "slowest read took " + worstRead.get() + " ns");
        assertEquals((long) RECORDS_PER_TRANSACTION * TRANSACTIONS, replica.appliedLsn());
        for (int node = 0; node < TRANSACTIONS; node += 97) {
            assertEquals(3, replica.getDegree(node));
        }
    }
}
