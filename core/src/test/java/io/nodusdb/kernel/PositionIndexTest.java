package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PositionIndexTest {

    @Test
    void emptyIndexFindsNothing() {
        PositionIndex index = new PositionIndex(8);
        long[] dense = new long[4];

        assertEquals(PositionIndex.ABSENT, index.find(dense, 0, 42L));
        assertEquals(0, index.size());
    }

    @Test
    void swapAndPopThroughRelocateKeepsTheIndexConsistent() {
        long[] keys = {
                KeyFixtures.nthKeyWithHome(6, 7, 0),
                KeyFixtures.nthKeyWithHome(6, 7, 1),
                KeyFixtures.nthKeyWithHome(6, 7, 2),
        };
        long[] dense = new long[8];
        PositionIndex index = new PositionIndex(8);
        for (int i = 0; i < keys.length; i++) {
            dense[i] = keys[i];
            index.insertAbsent(keys[i], i);
        }
        assertEquals(0, index.find(dense, 3, keys[0]));

        index.remove(dense, 3, keys[0]);
        index.relocate(dense, 3, keys[2], 0);
        dense[0] = keys[2];

        assertEquals(0, index.find(dense, 2, keys[2]));
        assertEquals(1, index.find(dense, 2, keys[1]));
        assertEquals(PositionIndex.ABSENT, index.find(dense, 2, keys[0]));
        assertEquals(2, index.size());
    }

    @Test
    void wrapAroundClusterSurvivesRemovalOfItsFirstEntry() {
        int mask = 7;
        long[] keys = new long[4];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = KeyFixtures.nthKeyWithHome(7, mask, i);
        }
        long[] dense = new long[8];
        PositionIndex index = new PositionIndex(8);
        for (int i = 0; i < keys.length; i++) {
            dense[i] = keys[i];
            index.insertAbsent(keys[i], i);
        }

        index.remove(dense, keys.length, keys[0]);

        for (int i = 1; i < keys.length; i++) {
            assertEquals(i, index.find(dense, keys.length, keys[i]), "key " + i + " lost after removal");
        }
        assertEquals(keys.length - 1, index.size());
    }

    @Test
    void rebuildKeepsEveryKeyReachable() {
        long[] dense = new long[16];
        PositionIndex index = new PositionIndex(32);
        for (int i = 0; i < 12; i++) {
            dense[i] = KeyFixtures.nthKeyWithHome(31, 31, i);
            index.insertAbsent(dense[i], i);
        }

        index.rebuild(dense, 12, 64);

        assertEquals(64, index.capacity());
        for (int i = 0; i < 12; i++) {
            assertEquals(i, index.find(dense, 12, dense[i]));
        }
    }

    @Test
    void randomChurnOverCollidingKeysMatchesASetOracle() {
        Random random = new Random(29L);
        IndexedSparseSet set = new IndexedSparseSet(4);
        Set<Long> oracle = new HashSet<>();
        long[] pool = new long[24];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = KeyFixtures.nthKeyWithHome(15, 15, i);
        }
        for (int op = 0; op < 40_000; op++) {
            long key = pool[random.nextInt(pool.length)];
            if (random.nextBoolean()) {
                assertEquals(oracle.add(key), set.add(key), "add at op " + op);
            } else {
                assertEquals(oracle.remove(key), set.remove(key), "remove at op " + op);
            }
            set.assertInvariant();
            for (long candidate : pool) {
                assertEquals(oracle.contains(candidate), set.contains(candidate), "contains at op " + op);
            }
        }
        assertEquals(oracle.size(), set.size());
    }
}
