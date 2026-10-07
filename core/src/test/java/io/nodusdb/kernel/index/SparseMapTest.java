package io.nodusdb.kernel.index;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseMapTest {

    private static final long QUIET_NAN_WITH_PAYLOAD = 0x7ff8000000000123L;

    @Test
    void emptyMapHasNoEntries() {
        SparseMap map = new SparseMap();

        assertEquals(0, map.size());
        assertFalse(map.containsKey(1L));
        assertFalse(map.remove(1L));
        assertThrows(NoSuchElementException.class, () -> map.getLong(1L));
    }

    @Test
    void putLongThenGetLongAndOverwrite() {
        SparseMap map = new SparseMap();

        map.putLong(4L, 100L);
        assertEquals(100L, map.getLong(4L));
        assertEquals(1, map.size());

        map.putLong(4L, -7L);
        assertEquals(-7L, map.getLong(4L));
        assertEquals(1, map.size());
    }

    @Test
    void getAbsentKeyThrows() {
        SparseMap map = new SparseMap();
        map.putLong(1L, 1L);

        assertThrows(NoSuchElementException.class, () -> map.getLong(2L));
        assertThrows(NoSuchElementException.class, () -> map.getWeight(2L));
    }

    @Test
    void valueStaysWithKeyThroughRepeatedSwapAndPop() {
        SparseMap map = new SparseMap(2);
        int count = 200;
        for (long k = 0; k < count; k++) {
            map.putLong(k, k * 7L + 3L);
        }

        for (long k = 0; k < count; k += 3) {
            assertTrue(map.remove(k));
        }
        for (long k = 0; k < count; k++) {
            if (k % 3 == 0) {
                assertFalse(map.containsKey(k));
            } else {
                assertEquals(k * 7L + 3L, map.getLong(k));
            }
        }
    }

    @Test
    void removingLastInsertedKeyKeepsOthersIntact() {
        SparseMap map = new SparseMap();
        map.putLong(1L, 10L);
        map.putLong(2L, 20L);
        map.putLong(3L, 30L);

        assertTrue(map.remove(3L));

        assertEquals(10L, map.getLong(1L));
        assertEquals(20L, map.getLong(2L));
        assertEquals(2, map.size());
    }

    @Test
    void removingFirstInsertedKeyMovesLastValueIntoPlace() {
        SparseMap map = new SparseMap();
        map.putLong(1L, 10L);
        map.putLong(2L, 20L);
        map.putLong(3L, 30L);

        assertTrue(map.remove(1L));

        assertEquals(30L, map.getLong(3L));
        assertEquals(20L, map.getLong(2L));
        assertFalse(map.containsKey(1L));
    }

    @Test
    void reinsertedKeyGetsFreshValue() {
        SparseMap map = new SparseMap();
        map.putLong(9L, 90L);
        map.remove(9L);

        map.putLong(9L, 91L);

        assertEquals(91L, map.getLong(9L));
        assertEquals(1, map.size());
    }

    @Test
    void weightsRoundTripBitForBit() {
        SparseMap map = new SparseMap();
        double[] weights = {
                -0.0,
                0.0,
                Double.NaN,
                Double.longBitsToDouble(QUIET_NAN_WITH_PAYLOAD),
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE,
                -Double.MAX_VALUE,
                0.85,
                3.1415926535
        };

        for (int i = 0; i < weights.length; i++) {
            map.putWeight(i, weights[i]);
        }

        for (int i = 0; i < weights.length; i++) {
            assertEquals(Double.doubleToRawLongBits(weights[i]), Double.doubleToRawLongBits(map.getWeight(i)),
                    "weight index " + i);
        }
    }

    @Test
    void negativeZeroIsNotCollapsedToPositiveZero() {
        SparseMap map = new SparseMap();

        map.putWeight(1L, -0.0);

        assertEquals(Long.MIN_VALUE, map.getLong(1L));
    }

    @Test
    void nanPayloadIsPreservedInStorage() {
        SparseMap map = new SparseMap();

        map.putWeight(1L, Double.longBitsToDouble(QUIET_NAN_WITH_PAYLOAD));

        assertEquals(QUIET_NAN_WITH_PAYLOAD, map.getLong(1L));
    }

    @Test
    void weightAndIntegerAccessorsShareTheSameBits() {
        SparseMap map = new SparseMap();

        map.putLong(1L, Double.doubleToRawLongBits(2.5));

        assertEquals(2.5, map.getWeight(1L));
    }

    @Test
    void weightsSurviveSwapAndPopWithInfinities() {
        SparseMap map = new SparseMap();
        map.putWeight(1L, Double.POSITIVE_INFINITY);
        map.putWeight(2L, Double.NEGATIVE_INFINITY);
        map.putWeight(3L, -0.0);
        map.putWeight(4L, 0.5);

        assertTrue(map.remove(1L));

        assertEquals(0.5, map.getWeight(4L));
        assertEquals(Double.NEGATIVE_INFINITY, map.getWeight(2L));
        assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(map.getWeight(3L)));
    }
}
