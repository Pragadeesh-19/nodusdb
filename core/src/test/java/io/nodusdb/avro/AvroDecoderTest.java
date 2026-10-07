package io.nodusdb.avro;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroDecoderTest {

    @Test
    void everyKindOfValueRoundTripsThroughTheEncoder() {
        byte[] encoded = new AvroEncoder().writeBoolean(true).writeInt(-7).writeLong(Long.MIN_VALUE)
                .writeString("héllo").writeBytes(new byte[]{1, 2, 3}).writeUnionIndex(1).writeLong(42)
                .toByteArray();
        AvroDecoder decoder = new AvroDecoder(encoded);

        assertTrue(decoder.readBoolean());
        assertEquals(-7, decoder.readInt());
        assertEquals(Long.MIN_VALUE, decoder.readLong());
        assertEquals("héllo", decoder.readString());
        assertArrayEquals(new byte[]{1, 2, 3}, decoder.readBytes());
        assertEquals(1, decoder.readUnionIndex());
        assertEquals(42, decoder.readLong());
        assertTrue(decoder.atEnd());
    }

    @Test
    void manyRandomLongsRoundTrip() {
        Random random = new Random(11);
        long[] values = new long[2_000];
        AvroEncoder encoder = new AvroEncoder();
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(4) == 0 ? random.nextLong() : random.nextInt(1_000) - 500;
            encoder.writeLong(values[i]);
        }

        AvroDecoder decoder = new AvroDecoder(encoder.toByteArray());

        for (long value : values) {
            assertEquals(value, decoder.readLong());
        }
        assertTrue(decoder.atEnd());
    }

    @Test
    void theIntegerLimitsRoundTripAndOutOfRangeValuesAreRefused() {
        byte[] encoded = new AvroEncoder().writeInt(Integer.MAX_VALUE).writeInt(Integer.MIN_VALUE)
                .writeLong(Integer.MAX_VALUE + 1L).toByteArray();
        AvroDecoder decoder = new AvroDecoder(encoded);

        assertEquals(Integer.MAX_VALUE, decoder.readInt());
        assertEquals(Integer.MIN_VALUE, decoder.readInt());
        assertThrows(AvroFormatException.class, decoder::readInt);
    }

    @Test
    void truncatingAtAnyLengthIsReportedAsAFormatError() {
        byte[] encoded = new AvroEncoder().writeLong(Long.MAX_VALUE).writeString("tail").writeBytes(new byte[5])
                .toByteArray();

        for (int length = 0; length < encoded.length; length++) {
            byte[] cut = Arrays.copyOf(encoded, length);
            assertThrows(AvroFormatException.class, () -> {
                AvroDecoder decoder = new AvroDecoder(cut);
                decoder.readLong();
                decoder.readString();
                decoder.readBytes();
            }, "length " + length);
        }
    }

    @Test
    void aVarintLongerThanTenBytesIsRefused() {
        byte[] endless = new byte[11];
        Arrays.fill(endless, (byte) 0x80);

        assertThrows(AvroFormatException.class, () -> new AvroDecoder(endless).readLong());
    }

    @Test
    void aLengthBeyondTheRemainingDataIsRefused() {
        byte[] encoded = new AvroEncoder().writeLong(1_000).writeRaw(new byte[3]).toByteArray();

        assertThrows(AvroFormatException.class, () -> new AvroDecoder(encoded).readBytes());
        assertThrows(AvroFormatException.class, () -> new AvroDecoder(new AvroEncoder().writeLong(-4).toByteArray())
                .readBytes());
        assertThrows(AvroFormatException.class, () -> new AvroDecoder(new byte[2]).readRaw(3));
    }

    @Test
    void aBooleanOtherThanZeroOrOneIsRefused() {
        assertThrows(AvroFormatException.class, () -> new AvroDecoder(new byte[]{2}).readBoolean());
        assertFalse(new AvroDecoder(new byte[]{0}).readBoolean());
    }

    @Test
    void aNegativeUnionIndexIsRefused() {
        assertThrows(AvroFormatException.class,
                () -> new AvroDecoder(new AvroEncoder().writeLong(-1).toByteArray()).readUnionIndex());
    }

    @Test
    void blockCountsHandleBothFormsAndTheEnd() {
        byte[] positive = new AvroEncoder().writeBlockCount(3).writeEndOfBlocks().toByteArray();
        byte[] negative = new AvroEncoder().writeLong(-3).writeLong(99).writeEndOfBlocks().toByteArray();

        AvroDecoder first = new AvroDecoder(positive);
        AvroDecoder second = new AvroDecoder(negative);

        assertEquals(3, first.readBlockCount());
        assertEquals(0, first.readBlockCount());
        assertEquals(3, second.readBlockCount());
        assertEquals(0, second.readBlockCount());
    }

    @Test
    void aRangeOutsideTheDataIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new AvroDecoder(new byte[4], -1, 2));
        assertThrows(IllegalArgumentException.class, () -> new AvroDecoder(new byte[4], 2, 5));
        assertThrows(IllegalArgumentException.class, () -> new AvroDecoder(new byte[4], 3, 2));
    }

    @Test
    void decodingStopsAtTheEndOfItsRange() {
        byte[] encoded = new AvroEncoder().writeLong(1).writeLong(2).writeLong(3).toByteArray();
        AvroDecoder decoder = new AvroDecoder(encoded, 1, 2);

        assertEquals(2, decoder.readLong());
        assertTrue(decoder.atEnd());
        assertThrows(AvroFormatException.class, decoder::readLong);
        assertEquals(2, decoder.position());
    }
}
