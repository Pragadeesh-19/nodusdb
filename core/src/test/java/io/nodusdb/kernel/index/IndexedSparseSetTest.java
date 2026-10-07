package io.nodusdb.kernel.index;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexedSparseSetTest {

    @Test
    void emptySetHasNoMembers() {
        IndexedSparseSet set = new IndexedSparseSet();

        assertEquals(0, set.size());
        assertFalse(set.contains(0L));
        assertFalse(set.remove(0L));
        assertEquals(LongIntIndex.ABSENT, set.indexOf(0L));
        set.assertInvariant();
    }

    @Test
    void singleElementRoundTrip() {
        IndexedSparseSet set = new IndexedSparseSet();

        assertTrue(set.add(42L));
        assertTrue(set.contains(42L));
        assertEquals(1, set.size());
        assertEquals(42L, set.get(0));

        assertTrue(set.remove(42L));
        assertFalse(set.contains(42L));
        assertEquals(0, set.size());
        set.assertInvariant();
    }

    @Test
    void addIsIdempotentAndReturnsFalseOnDuplicate() {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(7L);

        assertFalse(set.add(7L));

        assertEquals(1, set.size());
        assertEquals(0, set.indexOf(7L));
        set.assertInvariant();
    }

    @Test
    void removeFirstElementMovesLastIntoItsSlot() {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(10L);
        set.add(20L);
        set.add(30L);

        assertTrue(set.remove(10L));

        assertEquals(2, set.size());
        assertEquals(30L, set.get(0));
        assertEquals(20L, set.get(1));
        assertFalse(set.contains(10L));
        assertEquals(0, set.indexOf(30L));
        set.assertInvariant();
    }

    @Test
    void removeLastElementMovesNothing() {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(10L);
        set.add(20L);
        set.add(30L);

        assertTrue(set.remove(30L));

        assertEquals(2, set.size());
        assertEquals(10L, set.get(0));
        assertEquals(20L, set.get(1));
        set.assertInvariant();
    }

    @Test
    void removeAbsentReturnsFalseAndChangesNothing() {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(1L);
        set.add(2L);

        assertFalse(set.remove(3L));

        assertEquals(2, set.size());
        assertEquals(1L, set.get(0));
        assertEquals(2L, set.get(1));
        set.assertInvariant();
    }

    @Test
    void growthPastInitialCapacityKeepsEveryMember() {
        IndexedSparseSet set = new IndexedSparseSet(2);
        int count = 10_000;

        for (long k = 0; k < count; k++) {
            assertTrue(set.add(k * 3L - 5_000L));
        }

        assertEquals(count, set.size());
        for (long k = 0; k < count; k++) {
            assertTrue(set.contains(k * 3L - 5_000L));
        }
        set.assertInvariant();
    }

    @Test
    void getRejectsIndexOutsideDegree() {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(1L);

        assertThrows(IndexOutOfBoundsException.class, () -> set.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> set.get(-1));
    }

    @Test
    void constructorRejectsNonPowerOfTwoCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new IndexedSparseSet(0));
        assertThrows(IllegalArgumentException.class, () -> new IndexedSparseSet(3));
        assertThrows(IllegalArgumentException.class, () -> new IndexedSparseSet(-4));
    }

    @Test
    void boundaryValuesAreStoredExactly() {
        IndexedSparseSet set = new IndexedSparseSet();
        long[] values = {0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE};

        for (long v : values) {
            assertTrue(set.add(v));
        }

        for (long v : values) {
            assertTrue(set.contains(v));
        }
        set.assertInvariant();
    }

    @Test
    void removeAtRejectsPositionThatDoesNotHoldTheValue() {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(1L);
        set.add(2L);

        assertThrows(AssertionError.class, () -> set.removeAt(2L, 0));
        assertEquals(2, set.size());
    }

    @Test
    void removeAtMatchesRemoveForTheSameElement() {
        IndexedSparseSet viaRemove = new IndexedSparseSet();
        IndexedSparseSet viaRemoveAt = new IndexedSparseSet();
        for (long v = 10; v < 15; v++) {
            viaRemove.add(v);
            viaRemoveAt.add(v);
        }

        viaRemove.remove(11L);
        viaRemoveAt.removeAt(11L, viaRemoveAt.indexOf(11L));

        assertEquals(viaRemove.size(), viaRemoveAt.size());
        for (int i = 0; i < viaRemove.size(); i++) {
            assertEquals(viaRemove.get(i), viaRemoveAt.get(i));
        }
        viaRemoveAt.assertInvariant();
    }

    @Test
    void assertInvariantDetectsDenseCorruption() throws ReflectiveOperationException {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(5L);
        set.add(6L);
        Field denseField = IndexedSparseSet.class.getDeclaredField("dense");
        denseField.setAccessible(true);
        long[] dense = (long[]) denseField.get(set);

        dense[1] = dense[0];

        assertThrows(IllegalStateException.class, set::assertInvariant);
    }

    @Test
    void assertInvariantDetectsIndexDegreeMismatch() throws ReflectiveOperationException {
        IndexedSparseSet set = new IndexedSparseSet();
        set.add(5L);
        Field indexField = IndexedSparseSet.class.getDeclaredField("index");
        indexField.setAccessible(true);
        PositionIndex index = (PositionIndex) indexField.get(set);

        index.insertAbsent(777L, 0);

        assertThrows(IllegalStateException.class, set::assertInvariant);
    }
}
