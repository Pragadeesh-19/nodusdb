package io.nodusdb.log.record;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SegmentHeaderTest {

    @Test
    void aWrittenHeaderInspectsAsValidAndReturnsItsFields() {
        ByteBuffer buffer = ByteBuffer.allocate(64);

        SegmentHeader.write(buffer, 16, 1234L, 77L);

        assertEquals(Verdict.VALID, SegmentHeader.inspect(buffer, 16, 48));
        assertEquals(77L, SegmentHeader.baseLsn(buffer, 16));
        assertEquals(1234L, SegmentHeader.createdMicros(buffer, 16));
    }

    @Test
    void aShortRegionIsIncomplete() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        SegmentHeader.write(buffer, 0, 1L, 1L);

        for (int end = 0; end < SegmentHeader.BYTES; end++) {
            assertEquals(Verdict.INCOMPLETE, SegmentHeader.inspect(buffer, 0, end), "end " + end);
        }
    }

    @Test
    void anySingleBitFlipIsRejected() {
        ByteBuffer buffer = ByteBuffer.allocate(SegmentHeader.BYTES);
        SegmentHeader.write(buffer, 0, 99L, 5L);
        byte[] original = buffer.array();

        for (int bit = 0; bit < SegmentHeader.BYTES * 8; bit++) {
            byte[] damaged = original.clone();
            damaged[bit / 8] ^= (byte) (1 << (bit % 8));
            assertNotEquals(Verdict.VALID, SegmentHeader.inspect(ByteBuffer.wrap(damaged), 0, damaged.length),
                    "bit " + bit);
        }
    }

    @Test
    void aBaseLsnBelowOneIsInvalid() {
        ByteBuffer buffer = ByteBuffer.allocate(SegmentHeader.BYTES);
        SegmentHeader.write(buffer, 0, 1L, 0L);

        assertEquals(Verdict.INVALID, SegmentHeader.inspect(buffer, 0, SegmentHeader.BYTES));
    }
}
