package io.nodusdb.kernel;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexedSparseSetOracleTest {

    private static final int OPERATIONS = 100_000;
    private static final int KEYSPACE = 512;
    private static final int SWEEP_INTERVAL = 1_000;

    @ParameterizedTest
    @ValueSource(longs = {101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L, 109L, 110L})
    void randomOperationsAgreeWithHashSet(long seed) {
        Random random = new Random(seed);
        IndexedSparseSet set = new IndexedSparseSet(2);
        Set<Long> oracle = new HashSet<>();
        long low = -(KEYSPACE / 2L);

        for (int op = 0; op < OPERATIONS; op++) {
            long key = low + random.nextInt(KEYSPACE);
            int roll = random.nextInt(100);
            String where = "seed=" + seed + " op=" + op + " key=" + key;

            if (roll < 50) {
                assertEquals(oracle.add(key), set.add(key), where);
            } else if (roll < 85) {
                assertEquals(oracle.remove(key), set.remove(key), where);
            } else {
                assertEquals(oracle.contains(key), set.contains(key), where);
            }

            assertEquals(oracle.size(), set.size(), where);
            set.assertInvariant();
            if (op % SWEEP_INTERVAL == 0) {
                verifyMembership(set, oracle, low, low + KEYSPACE, where);
            }
        }
        verifyMembership(set, oracle, low, low + KEYSPACE, "seed=" + seed + " final");
    }

    private static void verifyMembership(IndexedSparseSet set, Set<Long> oracle, long from, long to, String where) {
        for (long key = from; key < to; key++) {
            assertEquals(oracle.contains(key), set.contains(key), where + " key=" + key);
        }
        Set<Long> enumerated = new HashSet<>();
        for (int i = 0; i < set.size(); i++) {
            assertTrue(enumerated.add(set.get(i)), where + " duplicate in dense at " + i);
            assertTrue(oracle.contains(set.get(i)), where + " dense holds non-member " + set.get(i));
        }
        assertEquals(oracle.size(), enumerated.size(), where);
    }
}
