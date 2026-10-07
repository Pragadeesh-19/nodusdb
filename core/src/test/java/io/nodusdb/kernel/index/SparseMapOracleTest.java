package io.nodusdb.kernel.index;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseMapOracleTest {

    private static final int OPERATIONS = 100_000;
    private static final int KEYSPACE = 512;
    private static final int SWEEP_INTERVAL = 1_000;

    @ParameterizedTest
    @ValueSource(longs = {301L, 302L, 303L, 304L, 305L, 306L, 307L, 308L, 309L, 310L})
    void randomOperationsAgreeWithHashMap(long seed) {
        Random random = new Random(seed);
        SparseMap map = new SparseMap(2);
        Map<Long, Long> oracle = new HashMap<>();
        long low = -(KEYSPACE / 2L);

        for (int op = 0; op < OPERATIONS; op++) {
            long key = low + random.nextInt(KEYSPACE);
            int roll = random.nextInt(100);
            String where = "seed=" + seed + " op=" + op + " key=" + key;

            if (roll < 50) {
                long value = random.nextLong();
                map.putLong(key, value);
                oracle.put(key, value);
            } else if (roll < 85) {
                assertEquals(oracle.remove(key) != null, map.remove(key), where);
            } else {
                assertEquals(oracle.containsKey(key), map.containsKey(key), where);
                if (oracle.containsKey(key)) {
                    assertEquals(oracle.get(key), map.getLong(key), where);
                }
            }

            assertEquals(oracle.size(), map.size(), where);
            if (op % SWEEP_INTERVAL == 0) {
                verifyAll(map, oracle, low, low + KEYSPACE, where);
            }
        }
        verifyAll(map, oracle, low, low + KEYSPACE, "seed=" + seed + " final");
    }

    private static void verifyAll(SparseMap map, Map<Long, Long> oracle, long from, long to, String where) {
        for (long key = from; key < to; key++) {
            if (oracle.containsKey(key)) {
                assertTrue(map.containsKey(key), where + " missing key=" + key);
                assertEquals(oracle.get(key), map.getLong(key), where + " key=" + key);
            } else {
                assertFalse(map.containsKey(key), where + " ghost key=" + key);
            }
        }
    }
}
