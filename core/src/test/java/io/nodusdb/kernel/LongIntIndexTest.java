package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LongIntIndexTest {

    @Test
    void emptyIndexReportsAbsent() {
        LongIntIndex index = new LongIntIndex();

        assertEquals(0, index.size());
        assertEquals(LongIntIndex.ABSENT, index.get(42L));
        assertFalse(index.containsKey(42L));
    }

    @Test
    void putThenGetReturnsValue() {
        LongIntIndex index = new LongIntIndex();

        index.put(7L, 3);

        assertEquals(3, index.get(7L));
        assertTrue(index.containsKey(7L));
        assertEquals(1, index.size());
    }

    @Test
    void putOnExistingKeyOverwritesWithoutChangingSize() {
        LongIntIndex index = new LongIntIndex();
        index.put(7L, 3);

        index.put(7L, 9);

        assertEquals(9, index.get(7L));
        assertEquals(1, index.size());
    }

    @Test
    void negativeValueIsRejected() {
        LongIntIndex index = new LongIntIndex();

        assertThrows(IllegalArgumentException.class, () -> index.put(1L, -1));
        assertEquals(0, index.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {-8, 0, 1, 3, 6, 12, 100})
    void constructorRejectsCapacityThatIsNotPowerOfTwoAtLeastTwo(int capacity) {
        assertThrows(IllegalArgumentException.class, () -> new LongIntIndex(capacity));
    }

    @Test
    void boundaryKeysRoundTrip() {
        LongIntIndex index = new LongIntIndex();
        long[] keys = {0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE};

        for (int i = 0; i < keys.length; i++) {
            index.put(keys[i], i);
        }

        for (int i = 0; i < keys.length; i++) {
            assertEquals(i, index.get(keys[i]));
        }
        assertEquals(keys.length, index.size());
    }

    @Test
    void removeAbsentKeyReturnsFalseAndChangesNothing() {
        LongIntIndex index = new LongIntIndex();
        index.put(1L, 1);

        assertFalse(index.remove(2L));
        assertEquals(1, index.size());
        assertEquals(1, index.get(1L));
    }

    @Test
    void removeOnlyKeyEmptiesIndexAndAllowsReuse() {
        LongIntIndex index = new LongIntIndex();
        index.put(5L, 0);

        assertTrue(index.remove(5L));
        assertEquals(0, index.size());
        assertEquals(LongIntIndex.ABSENT, index.get(5L));

        index.put(5L, 2);
        assertEquals(2, index.get(5L));
    }

    @Test
    void removeLastInsertedKeyLeavesOthersReachable() {
        LongIntIndex index = new LongIntIndex();
        for (long key = 0; key < 6; key++) {
            index.put(key, (int) key + 10);
        }

        assertTrue(index.remove(5L));

        assertEquals(5, index.size());
        assertEquals(LongIntIndex.ABSENT, index.get(5L));
        for (long key = 0; key < 5; key++) {
            assertEquals((int) key + 10, index.get(key));
        }
    }

    @Test
    void growthPreservesEveryEntry() {
        LongIntIndex index = new LongIntIndex(2);
        int count = 100_000;

        for (int i = 0; i < count; i++) {
            index.put(i * 31L - 500_000L, i);
        }

        assertEquals(count, index.size());
        assertTrue(index.capacity() >= 2 * count);
        for (int i = 0; i < count; i++) {
            assertEquals(i, index.get(i * 31L - 500_000L));
        }
    }

    @Test
    void wrapAroundClusterSurvivesHoleAtTableEnd() {
        int mask = 15;
        LongIntIndex index = new LongIntIndex(16);
        long hole = KeyFixtures.nthKeyWithHome(15, mask, 0);
        long[] chain = new long[4];
        for (int i = 0; i < chain.length; i++) {
            chain[i] = KeyFixtures.nthKeyWithHome(15, mask, i + 1);
        }

        index.put(hole, 100);
        for (int i = 0; i < chain.length; i++) {
            index.put(chain[i], i + 1);
        }
        assertEquals(16, index.capacity());

        assertTrue(index.remove(hole));

        assertEquals(LongIntIndex.ABSENT, index.get(hole));
        for (int i = 0; i < chain.length; i++) {
            assertEquals(i + 1, index.get(chain[i]));
        }
    }

    @Test
    void entryWhoseHomeIsPastTheHoleStaysReachable() {
        int mask = 15;
        LongIntIndex index = new LongIntIndex(16);
        long hole = KeyFixtures.nthKeyWithHome(1, mask, 0);
        long ahead = KeyFixtures.nthKeyWithHome(2, mask, 0);

        index.put(hole, 100);
        index.put(ahead, 7);

        assertTrue(index.remove(hole));

        assertEquals(7, index.get(ahead));
        assertEquals(1, index.size());
    }

    @Test
    void removeEverythingFromClusterLeavesIndexEmpty() {
        int mask = 15;
        LongIntIndex index = new LongIntIndex(16);
        long[] keys = new long[8];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = KeyFixtures.nthKeyWithHome(15, mask, i);
            index.put(keys[i], i);
        }

        for (int i = 0; i < keys.length; i++) {
            assertTrue(index.remove(keys[i]));
            for (int j = i + 1; j < keys.length; j++) {
                assertEquals(j, index.get(keys[j]));
            }
        }

        assertEquals(0, index.size());
        for (long key : keys) {
            assertEquals(LongIntIndex.ABSENT, index.get(key));
        }
    }
}
