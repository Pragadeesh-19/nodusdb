package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LowDegreeSlabTest {

    @Test
    void allocatedBlocksAreDistinctAndIndependent() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 2);
        int a = slab.allocateBlock();
        int b = slab.allocateBlock();

        assertNotEquals(a, b);
        for (int offset = 0; offset < LowDegreeSlab.BLOCK_SIZE; offset++) {
            slab.set(a, offset, 100L + offset);
            slab.set(b, offset, 200L + offset);
        }
        for (int offset = 0; offset < LowDegreeSlab.BLOCK_SIZE; offset++) {
            assertEquals(100L + offset, slab.get(a, offset));
            assertEquals(200L + offset, slab.get(b, offset));
        }
        assertEquals(2, slab.allocatedBlocks());
    }

    @Test
    void freedBlockIsReusedBeforeFreshBlocks() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 4);
        int a = slab.allocateBlock();
        slab.allocateBlock();

        slab.freeBlock(a);
        int reused = slab.allocateBlock();

        assertEquals(a, reused);
        assertEquals(2, slab.allocatedBlocks());
        assertEquals(4, slab.blockCapacity());
    }

    @Test
    void churnWithinCapacityNeverGrows() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 8);
        List<Integer> live = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            live.add(slab.allocateBlock());
        }

        for (int round = 0; round < 1_000; round++) {
            slab.freeBlock(live.remove(round % live.size()));
            live.add(slab.allocateBlock());
        }

        assertEquals(8, slab.blockCapacity());
        assertEquals(8, slab.allocatedBlocks());
    }

    @Test
    void rejectsDoubleFreeFreeOfNeverAllocatedAndOutOfRange() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 4);
        int a = slab.allocateBlock();
        slab.freeBlock(a);

        assertThrows(IllegalStateException.class, () -> slab.freeBlock(a));
        assertThrows(IllegalStateException.class, () -> slab.freeBlock(3));
        assertThrows(IllegalStateException.class, () -> slab.freeBlock(-1));
        assertThrows(IllegalStateException.class, () -> slab.freeBlock(999));
    }

    @Test
    void growthPreservesExistingContents() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 2);
        int count = 100;
        int[] blocks = new int[count];
        for (int i = 0; i < count; i++) {
            blocks[i] = slab.allocateBlock();
            slab.set(blocks[i], 0, i * 1_000L);
            slab.set(blocks[i], LowDegreeSlab.BLOCK_SIZE - 1, -i);
        }

        assertTrue(slab.blockCapacity() >= count);
        for (int i = 0; i < count; i++) {
            assertEquals(i * 1_000L, slab.get(blocks[i], 0));
            assertEquals(-i, slab.get(blocks[i], LowDegreeSlab.BLOCK_SIZE - 1));
        }
    }

    @Test
    void freeingAfterGrowthStillReusesBlocks() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 2);
        int first = slab.allocateBlock();
        for (int i = 0; i < 10; i++) {
            slab.allocateBlock();
        }

        slab.freeBlock(first);

        assertEquals(first, slab.allocateBlock());
    }

    @Test
    void offsetOutsideBlockIsRejectedByAssertion() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 2);
        int block = slab.allocateBlock();

        assertThrows(AssertionError.class, () -> slab.set(block, LowDegreeSlab.BLOCK_SIZE, 1L));
        assertThrows(AssertionError.class, () -> slab.get(block, -1));
    }

    @Test
    void everyBlockStartsOnA64ByteBoundaryAcrossChunks() {
        LowDegreeSlab slab = new LowDegreeSlab(Arena.ofAuto(), MemoryBudget.unlimited(), 2);
        for (int i = 0; i < 300; i++) {
            int block = slab.allocateBlock();
            assertEquals(0L, slab.addressOf(block) % 64, "block " + block + " is not 64-byte aligned");
        }
    }

}
