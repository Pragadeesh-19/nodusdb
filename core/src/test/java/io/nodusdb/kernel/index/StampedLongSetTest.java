package io.nodusdb.kernel.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StampedLongSetTest {

    @Test
    void anEmptySetHoldsNothing() {
        StampedLongSet set = new StampedLongSet();

        assertEquals(0, set.size());
        assertFalse(set.contains(0));
        assertFalse(set.contains(-1));
    }

    @Test
    void addingTwiceKeepsOneCopy() {
        StampedLongSet set = new StampedLongSet();

        assertTrue(set.add(42));
        assertFalse(set.add(42));

        assertEquals(1, set.size());
        assertTrue(set.contains(42));
    }

    @Test
    void clearForgetsEveryKeyWithoutTouchingTheArrays() {
        StampedLongSet set = new StampedLongSet();
        for (long key = 0; key < 1_000; key++) {
            set.add(key);
        }

        set.clear();

        assertEquals(0, set.size());
        for (long key = 0; key < 1_000; key++) {
            assertFalse(set.contains(key), "key=" + key);
        }
        assertTrue(set.add(7));
    }

    @Test
    void extremeKeysAreDistinct() {
        StampedLongSet set = new StampedLongSet();

        assertTrue(set.add(Long.MIN_VALUE));
        assertTrue(set.add(Long.MAX_VALUE));
        assertTrue(set.add(0));

        assertEquals(3, set.size());
        assertTrue(set.contains(Long.MIN_VALUE));
        assertTrue(set.contains(Long.MAX_VALUE));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void agreesWithAHashSetAcrossGrowthAndClears(long seed) {
        Random random = new Random(seed);
        StampedLongSet set = new StampedLongSet();
        Set<Long> oracle = new HashSet<>();
        for (int op = 0; op < 50_000; op++) {
            if (random.nextInt(1_000) == 0) {
                set.clear();
                oracle.clear();
                continue;
            }
            long key = random.nextInt(4_096) - 2_048L;
            if (random.nextBoolean()) {
                assertEquals(oracle.add(key), set.add(key), "seed=" + seed + " op=" + op);
            } else {
                assertEquals(oracle.contains(key), set.contains(key), "seed=" + seed + " op=" + op);
            }
            assertEquals(oracle.size(), set.size(), "seed=" + seed + " op=" + op);
        }
    }
}
