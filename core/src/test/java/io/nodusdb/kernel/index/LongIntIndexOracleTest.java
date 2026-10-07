package io.nodusdb.kernel.index;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LongIntIndexOracleTest {

    private static final int OPERATIONS = 100_000;
    private static final int UNIFORM_KEYSPACE = 4096;
    private static final int UNIFORM_CHECK_INTERVAL = 1_000;
    private static final int CLUSTER_CAPACITY = 16;
    private static final int CLUSTER_MASK = CLUSTER_CAPACITY - 1;

    @ParameterizedTest
    @ValueSource(longs = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L})
    void uniformOperationsAgreeWithHashMap(long seed) {
        Random random = new Random(seed);
        LongIntIndex index = new LongIntIndex(4);
        Map<Long, Integer> oracle = new HashMap<>();
        long low = -(UNIFORM_KEYSPACE / 2L);

        for (int op = 0; op < OPERATIONS; op++) {
            long key = low + random.nextInt(UNIFORM_KEYSPACE);
            int roll = random.nextInt(100);
            String where = "seed=" + seed + " op=" + op + " key=" + key;

            if (roll < 50) {
                int value = random.nextInt(Integer.MAX_VALUE);
                index.put(key, value);
                oracle.put(key, value);
            } else if (roll < 85) {
                boolean expected = oracle.remove(key) != null;
                assertEquals(expected, index.remove(key), where);
            } else {
                assertEquals(oracle.getOrDefault(key, LongIntIndex.ABSENT), index.get(key), where);
            }

            assertEquals(oracle.size(), index.size(), where);
            if (op % UNIFORM_CHECK_INTERVAL == 0) {
                verifyRange(index, oracle, low, low + UNIFORM_KEYSPACE, "seed=" + seed + " op=" + op);
            }
        }
        verifyRange(index, oracle, low, low + UNIFORM_KEYSPACE, "seed=" + seed + " final");
    }

    @ParameterizedTest
    @ValueSource(longs = {11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L})
    void clusteredCollisionsWrappingTableAgreeWithHashMap(long seed) {
        long[] pool = collidingPool();
        Random random = new Random(seed);
        LongIntIndex index = new LongIntIndex(CLUSTER_CAPACITY);
        Map<Long, Integer> oracle = new HashMap<>();

        for (int op = 0; op < OPERATIONS; op++) {
            long key = pool[random.nextInt(pool.length)];
            int roll = random.nextInt(100);
            String where = "seed=" + seed + " op=" + op + " key=" + key;

            if (roll < 50) {
                int value = random.nextInt(Integer.MAX_VALUE);
                index.put(key, value);
                oracle.put(key, value);
            } else if (roll < 85) {
                boolean expected = oracle.remove(key) != null;
                assertEquals(expected, index.remove(key), where);
            } else {
                assertEquals(oracle.getOrDefault(key, LongIntIndex.ABSENT), index.get(key), where);
            }

            assertEquals(oracle.size(), index.size(), where);
            for (long candidate : pool) {
                assertEquals(oracle.getOrDefault(candidate, LongIntIndex.ABSENT), index.get(candidate),
                        "seed=" + seed + " op=" + op + " candidate=" + candidate);
            }
        }
    }

    private static long[] collidingPool() {
        int[] homes = {CLUSTER_MASK - 1, CLUSTER_MASK, 0};
        int[] counts = {3, 3, 2};
        long[] pool = new long[CLUSTER_CAPACITY / 2];
        int next = 0;
        for (int h = 0; h < homes.length; h++) {
            for (int n = 0; n < counts[h]; n++) {
                pool[next++] = KeyFixtures.nthKeyWithHome(homes[h], CLUSTER_MASK, n);
            }
        }
        return pool;
    }

    private static void verifyRange(LongIntIndex index, Map<Long, Integer> oracle, long from, long to, String where) {
        for (long key = from; key < to; key++) {
            assertEquals(oracle.getOrDefault(key, LongIntIndex.ABSENT), index.get(key), where + " key=" + key);
        }
    }
}
