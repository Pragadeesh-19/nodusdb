package io.nodusdb.kernel.memory;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeLongArrayTest {

    @Test
    void unwrittenElementsReadAsTheDefaultAcrossChunks() {
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), MemoryBudget.unlimited(), 4, -1L);
        array.ensureCapacity(100);

        for (int i = 0; i < 100; i++) {
            assertEquals(-1L, array.get(i), "index " + i);
        }
    }

    @Test
    void writesRoundTripAcrossEveryChunkBoundary() {
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), MemoryBudget.unlimited(), 4, 0);
        array.ensureCapacity(500);

        for (int i = 0; i < 500; i++) {
            array.set(i, i * 7L + 1);
        }
        for (int i = 0; i < 500; i++) {
            assertEquals(i * 7L + 1, array.get(i), "index " + i);
        }
    }

    @Test
    void growthPreservesWrittenContents() {
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), MemoryBudget.unlimited(), 16, 0);
        array.ensureCapacity(16);
        for (int i = 0; i < 16; i++) {
            array.set(i, -i - 2L);
        }

        array.ensureCapacity(1_000);

        for (int i = 0; i < 16; i++) {
            assertEquals(-i - 2L, array.get(i), "index " + i);
        }
    }

    @Test
    void capacityIsAtLeastTheRequestedSizeAndNeverShrinks() {
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), MemoryBudget.unlimited(), 16, 0);
        assertEquals(16, array.capacity());

        array.ensureCapacity(17);
        int grown = array.capacity();
        assertTrue(grown >= 17);

        array.ensureCapacity(5);
        assertEquals(grown, array.capacity());
    }

    @Test
    void readsOutsideTheAllocatedRangeReturnTheDefault() {
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), MemoryBudget.unlimited(), 8, 9L);

        assertEquals(9L, array.get(-1));
        assertEquals(9L, array.get(8));
        assertEquals(9L, array.get(Integer.MAX_VALUE - 1));
    }

    @Test
    void writesOutsideTheAllocatedRangeAreRejected() {
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), MemoryBudget.unlimited(), 8, 0);

        assertThrows(IndexOutOfBoundsException.class, () -> array.set(8, 1L));
        assertThrows(IndexOutOfBoundsException.class, () -> array.set(-1, 1L));
    }

    @Test
    void sizesBeyondTheNodeLimitAreRejected() {
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), MemoryBudget.unlimited(), 16, 0);

        assertThrows(IllegalArgumentException.class,
                () -> array.ensureCapacity(NativeLongArray.MAX_CAPACITY + 1));
    }
}
