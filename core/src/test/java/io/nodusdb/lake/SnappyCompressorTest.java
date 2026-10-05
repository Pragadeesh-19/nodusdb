package io.nodusdb.lake;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SnappyCompressorTest {

    private static final long[] GOLDEN_CRC = {1268071522L, 353004L, 565289893L, 3523407757L};
    private static final int[] GOLDEN_LENGTH = {429, 3776, 80973, 1};

    @Test
    void outputMatchesTheRecordedFormatOnFixedInputs() {
        try (SnappyCompressor compressor = new SnappyCompressor(); NativeSink sink = new NativeSink()) {
            for (int which = 0; which < GOLDEN_CRC.length; which++) {
                sink.reset();
                compressor.compress(segmentOf(goldenInput(which)), sink);
                byte[] out = sink.segment().toArray(ValueLayout.JAVA_BYTE);

                CRC32 crc = new CRC32();
                crc.update(out);
                assertEquals(GOLDEN_LENGTH[which], out.length, "length of case " + which);
                assertEquals(GOLDEN_CRC[which], crc.getValue(), "checksum of case " + which);
            }
        }
    }

    @Test
    void randomAndRepetitiveInputsRoundTripThroughTheDecoder() throws IOException {
        Random random = new Random(20_260_502L);
        try (SnappyCompressor compressor = new SnappyCompressor(); NativeSink sink = new NativeSink()) {
            for (int trial = 0; trial < 200; trial++) {
                byte[] input = new byte[random.nextInt(4_096)];
                int alphabet = 1 + random.nextInt(8);
                for (int i = 0; i < input.length; i++) {
                    input[i] = (byte) ('a' + random.nextInt(alphabet));
                }
                sink.reset();
                compressor.compress(segmentOf(input), sink);

                assertArrayEquals(input, SnappyCodec.decompress(sink.segment().toArray(ValueLayout.JAVA_BYTE)),
                        "trial " + trial);
            }
        }
    }

    @Test
    void resetSinkReproducesTheSameBytes() {
        byte[] input = "column chunk payload ".repeat(64).getBytes(StandardCharsets.UTF_8);
        try (SnappyCompressor compressor = new SnappyCompressor(); NativeSink sink = new NativeSink()) {
            compressor.compress(segmentOf(input), sink);
            byte[] first = sink.segment().toArray(ValueLayout.JAVA_BYTE);

            sink.reset();
            compressor.compress(segmentOf(input), sink);

            assertArrayEquals(first, sink.segment().toArray(ValueLayout.JAVA_BYTE));
        }
    }

    private static byte[] goldenInput(int which) {
        Random random = new Random(7L + which);
        return switch (which) {
            case 0 -> "abcabcabc".repeat(1_000).getBytes(StandardCharsets.UTF_8);
            case 1 -> {
                byte[] bytes = new byte[5_000];
                for (int i = 0; i < bytes.length; i++) {
                    bytes[i] = (byte) ('a' + random.nextInt(4));
                }
                yield bytes;
            }
            case 2 -> {
                byte[] block = new byte[70_000];
                random.nextBytes(block);
                byte[] bytes = new byte[block.length * 3];
                for (int copy = 0; copy < 3; copy++) {
                    System.arraycopy(block, 0, bytes, copy * block.length, block.length);
                }
                yield bytes;
            }
            default -> new byte[0];
        };
    }

    private static MemorySegment segmentOf(byte[] bytes) {
        Arena arena = Arena.ofAuto();
        MemorySegment segment = arena.allocate(bytes.length);
        MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return segment;
    }
}
