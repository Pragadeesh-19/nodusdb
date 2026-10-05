package io.nodusdb.lake;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DictionaryIndexCodecTest {

    private static byte[] encode(int[] indices, int dictionarySize) {
        MemorySegment segment = Arena.ofAuto().allocate((long) indices.length * Integer.BYTES);
        MemorySegment.copy(indices, 0, segment, ValueLayout.JAVA_INT, 0, indices.length);
        try (NativeSink sink = new NativeSink()) {
            DictionaryIndexCodec.encode(segment, indices.length, dictionarySize, sink);
            return sink.segment().toArray(ValueLayout.JAVA_BYTE);
        }
    }

    @Test
    void roundTripsIndicesAcrossDictionarySizesAndPartialGroups() throws IOException {
        Random random = new Random(11);
        int[] dictionarySizes = {1, 2, 3, 4, 5, 17, 256, 1000, 65_536};
        for (int dictionarySize : dictionarySizes) {
            for (int count : new int[] {0, 1, 7, 8, 9, 100, 1023}) {
                int[] indices = new int[count];
                for (int i = 0; i < count; i++) {
                    indices[i] = random.nextInt(dictionarySize);
                }
                byte[] encoded = encode(indices, dictionarySize);

                int[] decoded = DictionaryIndexCodec.decode(encoded, 0, encoded.length, count);

                assertArrayEquals(indices, decoded, "dictionary " + dictionarySize + " count " + count);
            }
        }
    }

    @Test
    void decodesRunLengthRun() throws IOException {
        byte[] page = {3, 10, 5};

        int[] decoded = DictionaryIndexCodec.decode(page, 0, page.length, 5);

        assertArrayEquals(new int[] {5, 5, 5, 5, 5}, decoded);
    }

    @Test
    void chooseWidthFromDictionarySize() {
        assertEquals(1, DictionaryIndexCodec.bitWidthFor(1));
        assertEquals(1, DictionaryIndexCodec.bitWidthFor(2));
        assertEquals(2, DictionaryIndexCodec.bitWidthFor(3));
        assertEquals(16, DictionaryIndexCodec.bitWidthFor(65_536));
    }
}
