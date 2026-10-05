package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkLayoutTest {

    @Test
    void everyIndexLandsInExactlyOneChunkAndChunksTileTheRange() {
        ChunkLayout layout = new ChunkLayout(4);
        int chunk = 0;
        for (int index = 0; index < 4_000; index++) {
            int c = layout.chunkOf(index);
            int base = layout.chunkBase(c);
            int offset = layout.offsetOf(index, c);
            assertTrue(offset >= 0 && offset < layout.chunkSize(c), "index " + index + " outside chunk " + c);
            assertEquals(index, base + offset);
            chunk = Math.max(chunk, c);
        }
        for (int c = 0; c < chunk; c++) {
            assertEquals(layout.chunkBase(c) + layout.chunkSize(c), layout.chunkBase(c + 1),
                    "gap or overlap after chunk " + c);
        }
    }

    @Test
    void firstChunkHoldsTheInitialSizeAndEachLaterChunkDoubles() {
        ChunkLayout layout = new ChunkLayout(16);

        assertEquals(16, layout.chunkSize(0));
        assertEquals(16, layout.chunkSize(1));
        assertEquals(32, layout.chunkSize(2));
        assertEquals(64, layout.chunkSize(3));
        assertEquals(0, layout.chunkOf(15));
        assertEquals(1, layout.chunkOf(16));
        assertEquals(2, layout.chunkOf(32));
        assertEquals(2, layout.chunkOf(63));
        assertEquals(3, layout.chunkOf(64));
    }

    @Test
    void rejectsSizesThatAreNotPowersOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkLayout(0));
        assertThrows(IllegalArgumentException.class, () -> new ChunkLayout(3));
        assertThrows(IllegalArgumentException.class, () -> new ChunkLayout(-4));
    }
}
