package io.nodusdb.avro;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroEncoderTest {

    private static String hex(AvroEncoder encoder) {
        return HexFormat.of().formatHex(encoder.toByteArray());
    }

    @Test
    void integersUseTheZigzagVarintsOfTheAvroSpecification() {
        assertEquals("00", hex(new AvroEncoder().writeLong(0)));
        assertEquals("01", hex(new AvroEncoder().writeLong(-1)));
        assertEquals("02", hex(new AvroEncoder().writeLong(1)));
        assertEquals("03", hex(new AvroEncoder().writeLong(-2)));
        assertEquals("04", hex(new AvroEncoder().writeLong(2)));
        assertEquals("7f", hex(new AvroEncoder().writeLong(-64)));
        assertEquals("8001", hex(new AvroEncoder().writeLong(64)));
        assertEquals("8101", hex(new AvroEncoder().writeLong(-65)));
        assertEquals("8201", hex(new AvroEncoder().writeLong(65)));
    }

    @Test
    void theLargestAndSmallestLongsUseTenBytes() {
        assertEquals("feffffffffffffffff01", hex(new AvroEncoder().writeLong(Long.MAX_VALUE)));
        assertEquals("ffffffffffffffffff01", hex(new AvroEncoder().writeLong(Long.MIN_VALUE)));
    }

    @Test
    void anIntIsEncodedLikeALong() {
        assertEquals("feffffff0f", hex(new AvroEncoder().writeInt(Integer.MAX_VALUE)));
        assertEquals("ffffffff0f", hex(new AvroEncoder().writeInt(Integer.MIN_VALUE)));
    }

    @Test
    void stringsAndBytesCarryTheirLength() {
        assertEquals("06666f6f", hex(new AvroEncoder().writeString("foo")));
        assertEquals("00", hex(new AvroEncoder().writeString("")));
        assertEquals("04cafe", hex(new AvroEncoder().writeBytes(new byte[]{(byte) 0xCA, (byte) 0xFE})));
        assertEquals("04c3a9", hex(new AvroEncoder().writeString("é")));
    }

    @Test
    void booleansAreOneByte() {
        assertEquals("0001", hex(new AvroEncoder().writeBoolean(false).writeBoolean(true)));
    }

    @Test
    void unionsWriteTheBranchIndexFirst() {
        assertEquals("00", hex(new AvroEncoder().writeUnionIndex(0)));
        assertEquals("0206", hex(new AvroEncoder().writeUnionIndex(1).writeInt(3)));
    }

    @Test
    void arraysAreBlocksEndedByAZero() {
        AvroEncoder encoder = new AvroEncoder().writeBlockCount(2).writeLong(1).writeLong(2).writeEndOfBlocks();

        assertEquals("04020400", hex(encoder));
    }

    @Test
    void anEmptyBlockCountIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new AvroEncoder().writeBlockCount(0));
        assertThrows(IllegalArgumentException.class, () -> new AvroEncoder().writeBlockCount(-3));
    }

    @Test
    void theBufferGrowsPastItsInitialSize() {
        AvroEncoder encoder = new AvroEncoder();
        byte[] big = new byte[10_000];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) i;
        }

        encoder.writeBytes(big).writeString("tail");

        assertEquals(10_000 + 3 + 5, encoder.size());
        byte[] all = encoder.toByteArray();
        assertArrayEquals("tail".getBytes(StandardCharsets.UTF_8), java.util.Arrays.copyOfRange(all, all.length - 4,
                all.length));
    }

    @Test
    void resetStartsOver() {
        AvroEncoder encoder = new AvroEncoder().writeString("abc");

        encoder.reset();
        encoder.writeLong(1);

        assertEquals("02", hex(encoder));
        assertEquals(1, encoder.size());
    }
}
