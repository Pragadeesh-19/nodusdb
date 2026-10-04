package io.nodusdb.lake;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnappyCodecTest {

    @Test
    void decodesLiteralVector() throws IOException {
        byte[] encoded = {3, 8, 'a', 'b', 'c'};

        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), SnappyCodec.decompress(encoded));
    }

    @Test
    void decodesOverlappingCopyVector() throws IOException {
        byte[] encoded = {9, 8, 'a', 'b', 'c', 0x16, 3, 0};

        assertArrayEquals("abcabcabc".getBytes(StandardCharsets.UTF_8), SnappyCodec.decompress(encoded));
    }

    @Test
    void roundTripsEdgeInputs() throws IOException {
        assertRoundTrip(new byte[0]);
        assertRoundTrip(new byte[] {1});
        assertRoundTrip(new byte[] {1, 2, 3});
        assertRoundTrip(new byte[] {1, 2, 3, 4, 1, 2, 3, 4});
        assertRoundTrip(new byte[200]);
    }

    @Test
    void roundTripsRandomAndRepetitiveData() throws IOException {
        Random random = new Random(42);
        byte[] noise = new byte[1 << 20];
        random.nextBytes(noise);
        assertRoundTrip(noise);

        byte[] repeated = new byte[1 << 20];
        for (int i = 0; i < repeated.length; i++) {
            repeated[i] = (byte) (i % 251);
        }
        assertRoundTrip(repeated);
    }

    @Test
    void roundTripsMatchesFartherThanTheTwoByteOffsetLimit() throws IOException {
        Random random = new Random(7);
        byte[] block = new byte[100_000];
        random.nextBytes(block);
        byte[] input = Arrays.copyOf(block, block.length + 50_000);
        System.arraycopy(block, 0, input, block.length, 50_000);

        assertRoundTrip(input);
    }

    @Test
    void rejectsCopyThatReachesBeforeTheOutput() {
        byte[] encoded = {4, 0x0E, 1, 0};

        assertThrows(IOException.class, () -> SnappyCodec.decompress(encoded));
    }

    @Test
    void rejectsTruncatedLiteral() {
        byte[] encoded = {3, 8, 'a'};

        assertThrows(IOException.class, () -> SnappyCodec.decompress(encoded));
    }

    private static void assertRoundTrip(byte[] input) throws IOException {
        assertArrayEquals(input, SnappyCodec.decompress(SnappyCodec.compress(input)));
    }
}
