package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeBlockPoolTest {

    @Test
    void blocksOfTheSameClassAreDisjointAndKeepTheirContents() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto());
        List<Integer> handles = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            int handle = pool.allocate(3);
            for (int word = 0; word < 8; word++) {
                pool.set(handle, word, i * 100L + word);
            }
            handles.add(handle);
        }
        for (int i = 0; i < handles.size(); i++) {
            for (int word = 0; word < 8; word++) {
                assertEquals(i * 100L + word, pool.get(handles.get(i), word), "block " + i + " word " + word);
            }
        }
    }

    @Test
    void everyPayloadStartsOnACacheLine() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto());
        for (int log = NativeBlockPool.MIN_LOG_WORDS; log <= 9; log++) {
            for (int i = 0; i < 20; i++) {
                int handle = pool.allocate(log);
                assertEquals(0, handle % NativeBlockPool.LINE_WORDS, "log " + log + " handle " + handle);
                assertEquals(log, pool.logWordsOf(handle));
            }
        }
    }

    @Test
    void misalignedHandlesAreNotBlocks() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto());
        int handle = pool.allocate(NativeBlockPool.MIN_LOG_WORDS);

        assertEquals(-1, pool.logWordsOf(handle + 1));
        assertEquals(-1, pool.logWordsOf(handle + 4));
    }

    @Test
    void releasedBlockIsReusedForTheSameClassOnly() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto());
        int small = pool.allocate(3);
        pool.release(small);

        assertEquals(small, pool.allocate(3));
        assertNotEquals(small, pool.allocate(5));
    }

    @Test
    void logWordsReportsLiveBlocksOnly() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto());
        int handle = pool.allocate(4);

        assertEquals(4, pool.logWordsOf(handle));
        pool.release(handle);
        assertEquals(-1, pool.logWordsOf(handle));
        assertEquals(-1, pool.logWordsOf(0));
        assertEquals(-1, pool.logWordsOf(1_000_000));
    }

    @Test
    void doubleReleaseAndForeignHandlesAreRejected() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto());
        int handle = pool.allocate(3);
        pool.release(handle);

        assertThrows(IllegalStateException.class, () -> pool.release(handle));
        assertThrows(IllegalStateException.class, () -> pool.release(0));
        assertThrows(IllegalStateException.class, () -> pool.release(500_000));
    }

    @Test
    void sizeClassesOutsideTheSupportedRangeAreRejected() {
        NativeBlockPool pool = new NativeBlockPool(Arena.ofAuto());

        assertThrows(IllegalArgumentException.class, () -> pool.allocate(0));
        assertThrows(IllegalArgumentException.class, () -> pool.allocate(NativeBlockPool.MIN_LOG_WORDS - 1));
        assertThrows(IllegalArgumentException.class, () -> pool.allocate(NativeBlockPool.MAX_LOG_WORDS + 1));
    }
}
