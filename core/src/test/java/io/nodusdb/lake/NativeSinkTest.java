package io.nodusdb.lake;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NativeSinkTest {

    @Test
    void primitivesAppendInOrder() {
        try (NativeSink sink = new NativeSink()) {
            sink.put(0xAB);
            sink.putLittleEndian(0x0102_0304L, 3);
            sink.putVarint(300);

            assertArrayEquals(new byte[] {(byte) 0xAB, 0x04, 0x03, 0x02, (byte) 0xAC, 0x02},
                    sink.segment().toArray(ValueLayout.JAVA_BYTE));
        }
    }

    @Test
    void segmentAppendCopiesTheRequestedRange() {
        try (Arena arena = Arena.ofConfined(); NativeSink sink = new NativeSink()) {
            MemorySegment source = arena.allocate(5);
            for (int i = 0; i < 5; i++) {
                source.set(ValueLayout.JAVA_BYTE, i, (byte) (10 + i));
            }

            sink.write(source, 1, 3);

            assertArrayEquals(new byte[] {11, 12, 13}, sink.segment().toArray(ValueLayout.JAVA_BYTE));
        }
    }

    @Test
    void growthKeepsEveryByte() {
        int count = 50_000;
        try (NativeSink sink = new NativeSink()) {
            for (int i = 0; i < count; i++) {
                sink.put(i);
            }

            assertEquals(count, sink.length());
            byte[] bytes = sink.segment().toArray(ValueLayout.JAVA_BYTE);
            for (int i = 0; i < count; i++) {
                assertEquals((byte) i, bytes[i]);
            }
        }
    }

    @Test
    void resetReusesTheBufferFromTheStart() {
        try (NativeSink sink = new NativeSink()) {
            sink.putLittleEndian(-1L, 8);
            sink.reset();
            sink.put(7);

            assertEquals(1, sink.length());
            assertArrayEquals(new byte[] {7}, sink.segment().toArray(ValueLayout.JAVA_BYTE));
        }
    }
}
