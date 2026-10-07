package io.nodusdb.kernel.memory;

import io.nodusdb.kernel.adjacency.LowDegreeSlab;
import io.nodusdb.kernel.adjacency.NodeTable;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeAllocatorBudgetTest {

    @Test
    void longArrayChargesEveryChunkItAppends() {
        MemoryBudget budget = MemoryBudget.unlimited();
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), budget, 4, 0L);

        assertEquals(4 * Long.BYTES, budget.used());
        array.ensureCapacity(5);
        assertEquals(8 * Long.BYTES, budget.used());
        array.ensureCapacity(100);
        assertEquals((long) array.capacity() * Long.BYTES, budget.used());
    }

    @Test
    void longArrayGrowthThatDoesNotFitChangesNothing() {
        MemoryBudget budget = MemoryBudget.limitedTo(4 * Long.BYTES + 100);
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), budget, 4, 0L);
        array.set(3, 77L);

        assertThrows(MemoryLimitExceededException.class, () -> array.ensureCapacity(1_000));

        assertEquals(4, array.capacity());
        assertEquals(4 * Long.BYTES, budget.used());
        assertEquals(77L, array.get(3));
    }

    @Test
    void longArrayBytesToReachMatchesWhatGrowthCharges() {
        MemoryBudget budget = MemoryBudget.unlimited();
        NativeLongArray array = new NativeLongArray(Arena.ofAuto(), budget, 8, 0L);
        long before = budget.used();

        long predicted = array.bytesToReach(500);
        array.ensureCapacity(500);

        assertEquals(predicted, budget.used() - before);
        assertEquals(0, array.bytesToReach(500));
    }

    @Test
    void longArrayTooSmallBudgetFailsAtConstruction() {
        MemoryBudget budget = MemoryBudget.limitedTo(8);

        assertThrows(MemoryLimitExceededException.class, () -> new NativeLongArray(Arena.ofAuto(), budget, 4, 0L));

        assertEquals(0, budget.used());
    }

    @Test
    void slabChargesBlocksAndFlagsPerChunk() {
        MemoryBudget budget = MemoryBudget.unlimited();
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), budget, 4);

        slab.allocateBlock();

        assertEquals(4L * LowDegreeSlab.BLOCK_SIZE * Long.BYTES + 4, budget.used());
    }

    @Test
    void slabRefusesAChunkThatDoesNotFitAndKeepsItsBlocks() {
        long firstChunk = 2L * LowDegreeSlab.BLOCK_SIZE * Long.BYTES + 2;
        MemoryBudget budget = MemoryBudget.limitedTo(firstChunk);
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), budget, 2);
        int first = slab.allocateBlock();
        int second = slab.allocateBlock();
        slab.set(first, 0, 11L);
        slab.set(second, 0, 22L);

        assertThrows(MemoryLimitExceededException.class, slab::allocateBlock);

        assertEquals(2, slab.allocatedBlocks());
        assertEquals(11L, slab.get(first, 0));
        assertEquals(22L, slab.get(second, 0));
    }

    @Test
    void slabReportsWhetherABlockCanBeAllocated() {
        long firstChunk = 2L * LowDegreeSlab.BLOCK_SIZE * Long.BYTES + 2;
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.limitedTo(firstChunk), 2);

        assertTrue(slab.canAllocateBlock());
        int first = slab.allocateBlock();
        assertTrue(slab.canAllocateBlock());
        slab.allocateBlock();
        assertFalse(slab.canAllocateBlock());

        slab.freeBlock(first);
        assertTrue(slab.canAllocateBlock());
        assertEquals(first, slab.allocateBlock());
    }

    @Test
    void slabReservationPreventsAChargeOnTheNextAllocation() {
        MemoryBudget budget = MemoryBudget.unlimited();
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), budget, 2);
        slab.allocateBlock();
        slab.allocateBlock();

        slab.reserveAdditional(1);
        long reserved = budget.used();
        slab.allocateBlock();

        assertEquals(reserved, budget.used());
    }

    @Test
    void slabReservationIsRefusedWhenTheChunkDoesNotFit() {
        long firstChunk = 2L * LowDegreeSlab.BLOCK_SIZE * Long.BYTES + 2;
        MemoryBudget budget = MemoryBudget.limitedTo(firstChunk);
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), budget, 2);
        slab.allocateBlock();
        slab.allocateBlock();

        assertThrows(MemoryLimitExceededException.class, () -> slab.reserveAdditional(1));

        assertEquals(firstChunk, budget.used());
        assertEquals(2, slab.allocatedBlocks());
    }

    @Test
    void poolReservationMakesTheNextAllocationFreeOfCharge() {
        MemoryBudget budget = MemoryBudget.unlimited();
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto(), budget);

        pool.reserveBlocks(blocks(12));
        long reserved = budget.used();
        int handle = pool.allocate(12);

        assertEquals(reserved, budget.used());
        assertEquals(12, pool.logWordsOf(handle));
    }

    @Test
    void poolReservationCoversEveryAllocationWithinTheReservedWords() {
        MemoryBudget budget = MemoryBudget.unlimited();
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto(), budget);
        int[] needed = blocks(12, 11, 5);

        pool.reserveBlocks(needed);
        long reserved = budget.used();
        pool.allocate(12);
        pool.allocate(11);
        pool.allocate(5);

        assertEquals(reserved, budget.used());
    }

    @Test
    void poolReservationIsPricedBeforeItIsMade() {
        MemoryBudget budget = MemoryBudget.unlimited();
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto(), budget);
        int[] needed = blocks(14);

        long priced = pool.bytesToReserveBlocks(needed);
        pool.reserveBlocks(needed);

        assertEquals(priced, budget.used() - (1L << 10) * Long.BYTES);
    }

    @Test
    void poolReservationIsRefusedWhenTheNextChunkDoesNotFit() {
        MemoryBudget budget = MemoryBudget.limitedTo(1_024L * Long.BYTES);
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto(), budget);
        int small = pool.allocate(3);

        assertThrows(MemoryLimitExceededException.class, () -> pool.reserveBlocks(blocks(12)));
        assertThrows(MemoryLimitExceededException.class, () -> pool.allocate(12));

        assertEquals(3, pool.logWordsOf(small));
        assertEquals(1_024L * Long.BYTES, budget.used());
    }

    @Test
    void nodeTableBytesToHoldMatchesWhatGrowthCharges() {
        MemoryBudget budget = MemoryBudget.unlimited();
        NodeTable table = new NodeTable(Arena.ofAuto(), budget, 16);
        long before = budget.used();

        long predicted = table.bytesToHold(10_000);
        table.ensureCapacity(10_000);

        assertEquals(predicted, budget.used() - before);
    }

    @Test
    void poolReservationIsSatisfiedByFreeBlocksOfTheSameClass() {
        MemoryBudget budget = MemoryBudget.unlimited();
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto(), budget);
        int handle = pool.allocate(10);
        pool.release(handle);
        long before = budget.used();

        assertEquals(0, pool.bytesToReserveBlocks(blocks(10)));
        pool.reserveBlocks(blocks(10));

        assertEquals(before, budget.used());
        assertEquals(handle, pool.allocate(10));
    }

    @Test
    void poolReservationOfNothingPricesNothing() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto(), MemoryBudget.unlimited());
        pool.allocate(10);

        assertEquals(0, pool.bytesToReserveBlocks(blocks()));
    }

    @Test
    void slabReservationCountsFreeBlocksBeforeFreshOnes() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 2);
        int first = slab.allocateBlock();
        slab.allocateBlock();
        slab.freeBlock(first);

        assertEquals(0, slab.bytesToReserve(1));
        assertTrue(slab.bytesToReserve(2) > 0);
    }

    private static int[] blocks(int... logWords) {
        int[] counts = new int[NativeBlockPool.CLASS_COUNT];
        for (int log : logWords) {
            counts[log]++;
        }
        return counts;
    }
}
