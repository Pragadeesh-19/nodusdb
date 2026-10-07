package io.nodusdb.ship;

import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EpochClaimsTest {

    private final MemoryObjectStore store = new MemoryObjectStore();
    private final EpochClaims claims = new EpochClaims(store, 0xABCDEF, 3, () -> 1_700_000_000_000_000L);

    @Test
    void theFirstFreeCandidateIsClaimedAndRecorded() {
        assertEquals(5, claims.claim(5, 10));

        JsonObject document = JsonParser.parseObject(store.get(ChainLayout.epochKey(5)).orElseThrow());
        assertEquals(5, document.requireLong("epoch"));
        assertEquals("abcdef", document.requireString("writer_nonce"));
        assertEquals(3, document.requireLong("key_id"));
        assertEquals(1_700_000_000_000_000L, document.requireLong("claimed_micros"));
    }

    @Test
    void claimsAlreadyHeldAreSkipped() {
        store.put(ChainLayout.epochKey(5), "{}".getBytes(StandardCharsets.UTF_8));
        store.put(ChainLayout.epochKey(6), "{}".getBytes(StandardCharsets.UTF_8));

        assertEquals(7, claims.claim(5, 10));
        assertEquals(8, claims.claim(5, 10));
    }

    @Test
    void whenEveryCandidateIsTakenTheWriterIsFenced() {
        for (int epoch = 5; epoch < 8; epoch++) {
            store.put(ChainLayout.epochKey(epoch), "{}".getBytes(StandardCharsets.UTF_8));
        }

        WriterFencedException fenced = assertThrows(WriterFencedException.class, () -> claims.claim(5, 3));

        assertTrue(fenced.getMessage().contains("5 to 7"), fenced.getMessage());
        assertEquals(4, claims.claim(4, 3));
    }

    @Test
    void aConflictAnswerCountsAsTakenAndTheNextEpochIsTried() {
        FaultyObjectStore faulty = new FaultyObjectStore(store).failNext(Operation.PUT_IF_ABSENT, Fault.CONFLICT);

        long epoch = new EpochClaims(faulty, 1, 1, () -> 1).claim(5, 10);

        assertEquals(6, epoch);
        assertFalse(store.exists(ChainLayout.epochKey(5)));
    }

    @Test
    void aStoreFailureIsPassedOnWithoutClaimingAnything() {
        FaultyObjectStore faulty = new FaultyObjectStore(store).failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> new EpochClaims(faulty, 1, 1, () -> 1).claim(5, 10));

        assertEquals(0, store.size());
    }

    @Test
    void aWriterIsSupersededOnlyWhenTheNextEpochIsClaimed() {
        store.put(ChainLayout.epochKey(5), "{}".getBytes(StandardCharsets.UTF_8));

        assertFalse(claims.supersededBy(5));
        assertFalse(claims.supersededBy(6));
        store.put(ChainLayout.epochKey(6), "{}".getBytes(StandardCharsets.UTF_8));

        assertTrue(claims.supersededBy(5));
        assertTrue(claims.supersededBy(4));
        assertFalse(claims.supersededBy(6));
    }

    @Test
    void concurrentClaimersAlwaysGetDistinctEpochs() throws Exception {
        int claimers = 12;
        ExecutorService pool = Executors.newFixedThreadPool(claimers);
        try {
            for (int round = 0; round < 30; round++) {
                MemoryObjectStore shared = new MemoryObjectStore();
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Long>> results = new ArrayList<>();
                for (int claimer = 0; claimer < claimers; claimer++) {
                    int id = claimer;
                    results.add(pool.submit(() -> {
                        go.await();
                        return new EpochClaims(shared, id, 1, () -> 1).claim(10, 64);
                    }));
                }
                go.countDown();
                Set<Long> epochs = new HashSet<>();
                for (Future<Long> result : results) {
                    assertTrue(epochs.add(result.get(30, TimeUnit.SECONDS)), "an epoch was handed out twice");
                }
                assertEquals(claimers, epochs.size());
                assertEquals(10, epochs.stream().mapToLong(Long::longValue).min().orElseThrow());
                assertEquals(10 + claimers - 1, epochs.stream().mapToLong(Long::longValue).max().orElseThrow());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
