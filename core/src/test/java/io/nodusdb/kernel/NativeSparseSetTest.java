package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeSparseSetTest {

    private final NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto(), MemoryBudget.unlimited());
    private final NativeSparseSet set = new NativeSparseSet(pool);

    @Test
    void emptySetContainsNothingAndAbsentRemoveIsRejected() {
        int handle = set.allocate(16);

        assertFalse(set.contains(handle, 0, 7L));
        assertThrows(IllegalStateException.class, () -> set.remove(handle, 0, 7L));
    }

    @Test
    void appendedKeysAreFoundAndRemovalShiftsTheLastKeyIntoTheHole() {
        int handle = set.allocate(16);
        for (int i = 0; i < 5; i++) {
            set.append(handle, i, 100L + i);
        }

        set.remove(handle, 5, 101L);

        assertFalse(set.contains(handle, 4, 101L));
        assertEquals(1, set.indexOf(handle, 4, 104L));
        for (long key : new long[] {100L, 102L, 103L}) {
            assertTrue(set.contains(handle, 4, key), "key " + key + " lost");
        }
        set.verify(handle, 4);
    }

    @Test
    void wrapAroundClusterSurvivesRemovalOfItsFirstEntry() {
        int capacity = 16;
        int mask = 2 * capacity - 1;
        long[] keys = new long[6];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = KeyFixtures.nthKeyWithHome(mask, mask, i);
        }
        int handle = set.allocate(capacity);
        for (int i = 0; i < keys.length; i++) {
            set.append(handle, i, keys[i]);
        }

        set.remove(handle, keys.length, keys[0]);

        for (int i = 1; i < keys.length; i++) {
            assertTrue(set.contains(handle, keys.length - 1, keys[i]), "key " + i + " unreachable");
        }
        set.verify(handle, keys.length - 1);
    }

    @Test
    void growthKeepsEveryKeyReachable() {
        int handle = set.allocate(4);
        for (int i = 0; i < 4; i++) {
            set.append(handle, i, 1_000L + i);
        }

        int grown = set.grow(handle, 4);
        set.append(grown, 4, 9_999L);

        assertEquals(8, set.capacityOf(grown));
        for (int i = 0; i < 4; i++) {
            assertTrue(set.contains(grown, 5, 1_000L + i));
        }
        assertTrue(set.contains(grown, 5, 9_999L));
        set.verify(grown, 5);
    }

    @Test
    void appendBeyondCapacityIsRejected() {
        int handle = set.allocate(2);
        int capacity = set.capacityOf(handle);
        for (int i = 0; i < capacity; i++) {
            set.append(handle, i, 1L + i);
        }

        assertThrows(IllegalStateException.class, () -> set.append(handle, capacity, 3L));
    }

    @Test
    void smallCapacitiesArePaddedToWholeCacheLines() {
        for (int requested : new int[] {1, 2, 4}) {
            int handle = set.allocate(requested);

            assertEquals(NativeSparseSet.MIN_CAPACITY, set.capacityOf(handle), "requested " + requested);
            assertEquals(0, handle % NativeBlockPool.LINE_WORDS, "handle of requested " + requested);
        }
    }

    @Test
    void largeBlocksKeepDenseAndTableOnCacheLines() {
        int handle = set.allocate(64);

        assertEquals(64, set.capacityOf(handle));
        assertEquals(0, handle % NativeBlockPool.LINE_WORDS);
        assertEquals(7, pool.logWordsOf(handle), "payload of 2 * 64 words");
    }

    @Test
    void randomChurnOverCollidingKeysMatchesASetOracle() {
        Random random = new Random(37L);
        Set<Long> oracle = new HashSet<>();
        long[] pool = new long[40];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = KeyFixtures.nthKeyWithHome(31, 31, i);
        }
        int handle = set.allocate(4);
        int degree = 0;
        for (int op = 0; op < 30_000; op++) {
            long key = pool[random.nextInt(pool.length)];
            if (random.nextBoolean()) {
                if (!oracle.contains(key)) {
                    if (degree == set.capacityOf(handle)) {
                        handle = set.grow(handle, degree);
                    }
                    set.append(handle, degree, key);
                    degree++;
                    oracle.add(key);
                }
            } else if (oracle.remove(key)) {
                set.remove(handle, degree, key);
                degree--;
            }
            set.verify(handle, degree);
            assertEquals(oracle.size(), degree);
            for (long candidate : pool) {
                assertEquals(oracle.contains(candidate), set.contains(handle, degree, candidate),
                        "contains " + candidate + " at op " + op);
            }
        }
    }
}
