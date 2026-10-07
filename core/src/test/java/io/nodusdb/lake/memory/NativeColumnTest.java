package io.nodusdb.lake.memory;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NativeColumnTest {

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;

    @Test
    void capacityIsAPowerOfTwoAtLeastTheRequest() {
        try (NativeColumn column = new NativeColumn(1_000)) {
            assertEquals(1_024, column.capacity());
        }
        try (NativeColumn column = new NativeColumn(1)) {
            assertEquals(NativeColumn.ALIGNMENT, column.capacity());
        }
    }

    @Test
    void segmentStartsOnACacheLine() {
        for (int i = 0; i < 16; i++) {
            try (NativeColumn column = new NativeColumn(64L << i)) {
                assertEquals(0, column.segment().address() % NativeColumn.ALIGNMENT, "column " + i);
            }
        }
    }

    @Test
    void freshMemoryIsZero() {
        try (NativeColumn column = new NativeColumn(4096)) {
            for (long offset = 0; offset < column.capacity(); offset += Long.BYTES) {
                assertEquals(0L, column.segment().get(LONG, offset));
            }
        }
    }

    @Test
    void growthKeepsContentsAndDoubles() {
        try (NativeColumn column = new NativeColumn(64)) {
            for (long i = 0; i < 8; i++) {
                column.segment().setAtIndex(LONG, i, 1_000 + i);
            }
            column.ensureCapacity(column.capacity() + 1);

            assertEquals(128, column.capacity());
            assertEquals(0, column.segment().address() % NativeColumn.ALIGNMENT);
            for (long i = 0; i < 8; i++) {
                assertEquals(1_000 + i, column.segment().getAtIndex(LONG, i));
            }
        }
    }

    @Test
    void growthToALargeRequestRoundsToThatPowerOfTwo() {
        try (NativeColumn column = new NativeColumn(64)) {
            column.ensureCapacity(1_000);

            assertEquals(1_024, column.capacity());
        }
    }

    @Test
    void ensureCapacityBelowTheCurrentSizeKeepsTheSegment() {
        try (NativeColumn column = new NativeColumn(512)) {
            MemorySegment before = column.segment();
            column.ensureCapacity(256);

            assertEquals(before.address(), column.segment().address());
        }
    }

    @Test
    void closeIsIdempotent() {
        NativeColumn column = new NativeColumn(64);
        column.close();
        column.close();
    }

    @Test
    void sizesOutsideTheSupportedRangeAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new NativeColumn(0));
        assertThrows(IllegalArgumentException.class, () -> new NativeColumn(NativeColumn.MAX_BYTES + 1));
    }
}
