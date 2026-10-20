package io.nodusdb.kernel.memory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

public final class NativeMemorySelfTest {

    private static final long SEGMENT_BYTES = 256;

    private final StringBuilder failures = new StringBuilder();
    private int failed;

    private NativeMemorySelfTest() {
    }

    public static Result run(boolean expectPointers) {
        NativeMemorySelfTest test = new NativeMemorySelfTest();
        test.checkMode(expectPointers);
        try (Arena arena = Arena.ofConfined()) {
            test.checkRoundTrips(arena.allocate(SEGMENT_BYTES, 64));
            test.checkUnaligned(arena.allocate(SEGMENT_BYTES, 64));
            test.checkLittleEndian(arena.allocate(SEGMENT_BYTES, 64));
            test.checkBounds(arena.allocate(SEGMENT_BYTES, 64));
            test.checkEmpty(arena.allocate(0));
            test.checkReadOnly(arena.allocate(SEGMENT_BYTES, 64));
        }
        test.checkClosed();
        test.checkAuto(Arena.ofAuto().allocate(SEGMENT_BYTES, 64));
        test.checkHeap(MemorySegment.ofArray(new long[(int) (SEGMENT_BYTES / Long.BYTES)]));
        return new Result(test.failed, test.failures.toString());
    }

    public record Result(int failures, String messages) {
    }

    private void checkMode(boolean expectPointers) {
        expect(NativeMemory.usesPointers() == expectPointers,
                "usesPointers() is " + NativeMemory.usesPointers() + ", expected " + expectPointers);
    }

    private void checkRoundTrips(MemorySegment segment) {
        long elements = SEGMENT_BYTES / Long.BYTES;
        for (long i = 0; i < elements; i++) {
            long value = 0x0102030405060708L * (i + 1) ^ Long.MIN_VALUE;
            NativeMemory.setLongAtIndex(segment, i, value);
            expect(NativeMemory.getLongAtIndex(segment, i) == value, "long round trip at index " + i);
            expect(segment.get(ValueLayout.JAVA_LONG, i * Long.BYTES) == value, "long written at index " + i);
        }
        for (long i = 0; i < SEGMENT_BYTES / Integer.BYTES; i++) {
            int value = (int) (0x9E3779B9L * (i + 1));
            NativeMemory.setIntAtIndex(segment, i, value);
            expect(NativeMemory.getIntAtIndex(segment, i) == value, "int round trip at index " + i);
            expect(segment.get(ValueLayout.JAVA_INT, i * Integer.BYTES) == value, "int written at index " + i);
        }
        for (long offset = 0; offset < SEGMENT_BYTES; offset++) {
            byte value = (byte) (offset * 31 + 7);
            NativeMemory.setByte(segment, offset, value);
            expect(NativeMemory.getByte(segment, offset) == value, "byte round trip at offset " + offset);
            expect(segment.get(ValueLayout.JAVA_BYTE, offset) == value, "byte written at offset " + offset);
        }
    }

    private void checkUnaligned(MemorySegment segment) {
        for (long offset = 0; offset + Long.BYTES <= SEGMENT_BYTES; offset++) {
            long value = 0x1122334455667788L + offset;
            NativeMemory.setLongUnaligned(segment, offset, value);
            expect(NativeMemory.getLongUnaligned(segment, offset) == value, "unaligned long at offset " + offset);
            expect(segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset) == value,
                    "unaligned long written at offset " + offset);
        }
        for (long offset = 0; offset + Integer.BYTES <= SEGMENT_BYTES; offset++) {
            int value = 0x5A5A0000 + (int) offset;
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, offset, value);
            expect(NativeMemory.getIntUnaligned(segment, offset) == value, "unaligned int at offset " + offset);
        }
        long[] indexed = {0, 1, 7, SEGMENT_BYTES / Long.BYTES - 1};
        for (long index : indexed) {
            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES, index + 41);
            expect(NativeMemory.getLongUnalignedAtIndex(segment, index) == index + 41,
                    "unaligned long at index " + index);
            segment.set(ValueLayout.JAVA_INT_UNALIGNED, index * Integer.BYTES, (int) index + 43);
            expect(NativeMemory.getIntUnalignedAtIndex(segment, index) == index + 43,
                    "unaligned int at index " + index);
        }
    }

    private void checkLittleEndian(MemorySegment segment) {
        for (long offset = 0; offset < 8; offset++) {
            for (int b = 0; b < Integer.BYTES; b++) {
                segment.set(ValueLayout.JAVA_BYTE, offset + b, (byte) (0x10 * (b + 1) + offset));
            }
            int expected = segment.get(ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN), offset);
            expect(NativeMemory.getIntLittleEndian(segment, offset) == expected,
                    "little-endian int at offset " + offset);
        }
    }

    private void checkBounds(MemorySegment segment) {
        long last = SEGMENT_BYTES - Long.BYTES;
        NativeMemory.setLong(segment, last, 99);
        expect(NativeMemory.getLong(segment, last) == 99, "long at the last whole offset");
        expect(NativeMemory.getByte(segment, SEGMENT_BYTES - 1)
                == segment.get(ValueLayout.JAVA_BYTE, SEGMENT_BYTES - 1),
                "byte at the last offset");
        expectOutOfBounds(() -> NativeMemory.getLong(segment, last + 1), "long one byte past the end");
        expectOutOfBounds(() -> NativeMemory.getLongUnaligned(segment, SEGMENT_BYTES), "long at the end");
        expectOutOfBounds(() -> NativeMemory.getInt(segment, SEGMENT_BYTES - 2), "int straddling the end");
        expectOutOfBounds(() -> NativeMemory.getByte(segment, SEGMENT_BYTES), "byte at the end");
        expectOutOfBounds(() -> NativeMemory.getByte(segment, -1), "negative byte offset");
        expectOutOfBounds(() -> NativeMemory.getLongAtIndex(segment, -1), "negative long index");
        expectOutOfBounds(() -> NativeMemory.getLongAtIndex(segment, SEGMENT_BYTES / Long.BYTES),
                "long index at the end");
        expectOutOfBounds(() -> NativeMemory.getLongAtIndex(segment, Long.MAX_VALUE), "long index that overflows");
        expectOutOfBounds(() -> NativeMemory.setLong(segment, last + 1, 1), "long write past the end");
        expectOutOfBounds(() -> NativeMemory.setInt(segment, SEGMENT_BYTES, 1), "int write at the end");
        expectOutOfBounds(() -> NativeMemory.setByte(segment, -1, (byte) 1), "negative byte write");
        expectOutOfBounds(() -> NativeMemory.getIntLittleEndian(segment, SEGMENT_BYTES - 3),
                "little-endian past the end");
        expect(NativeMemory.getLong(segment, last) == 99, "a refused write changed nothing");
    }

    private void checkEmpty(MemorySegment empty) {
        expectOutOfBounds(() -> NativeMemory.getByte(empty, 0), "byte in an empty segment");
        expectOutOfBounds(() -> NativeMemory.getLong(empty, 0), "long in an empty segment");
        expectOutOfBounds(() -> NativeMemory.setInt(empty, 0, 1), "int write in an empty segment");
    }

    private void checkReadOnly(MemorySegment segment) {
        NativeMemory.setLong(segment, 0, 5);
        MemorySegment readOnly = segment.asReadOnly();
        expect(NativeMemory.getLong(readOnly, 0) == 5, "read through a read-only view");
        expectRefused(() -> NativeMemory.setLong(readOnly, 0, 6), "long write to a read-only view");
        expectRefused(() -> NativeMemory.setByte(readOnly, 0, (byte) 6), "byte write to a read-only view");
        expect(NativeMemory.getLong(segment, 0) == 5, "a read-only view was not written");
    }

    private void checkClosed() {
        Arena arena = Arena.ofConfined();
        MemorySegment segment = arena.allocate(SEGMENT_BYTES, 64);
        arena.close();
        expectClosed(() -> NativeMemory.getLong(segment, 0), "long read after the arena closed");
        expectClosed(() -> NativeMemory.setInt(segment, 0, 1), "int write after the arena closed");
        expectClosed(() -> NativeMemory.getByte(segment, 0), "byte read after the arena closed");
    }

    private void checkAuto(MemorySegment segment) {
        NativeMemory.setLongAtIndex(segment, 3, 123456789L);
        System.gc();
        expect(NativeMemory.getLongAtIndex(segment, 3) == 123456789L, "auto arena segment survives a collection");
    }

    private void checkHeap(MemorySegment heap) {
        NativeMemory.setLongAtIndex(heap, 2, -7);
        expect(NativeMemory.getLongAtIndex(heap, 2) == -7, "heap segment round trip");
        expectOutOfBounds(() -> NativeMemory.getLongAtIndex(heap, SEGMENT_BYTES / Long.BYTES), "heap segment bounds");
    }

    private void expectOutOfBounds(Runnable access, String what) {
        try {
            access.run();
            fail(what + " did not throw");
        } catch (IndexOutOfBoundsException expected) {
            return;
        } catch (RuntimeException other) {
            fail(what + " threw " + other.getClass().getName() + " instead of IndexOutOfBoundsException");
        }
    }

    private void expectClosed(Runnable access, String what) {
        try {
            access.run();
            fail(what + " did not throw");
        } catch (IllegalStateException expected) {
            return;
        } catch (RuntimeException other) {
            fail(what + " threw " + other.getClass().getName() + " instead of IllegalStateException");
        }
    }

    private void expectRefused(Runnable access, String what) {
        try {
            access.run();
            fail(what + " did not throw");
        } catch (UnsupportedOperationException | IllegalArgumentException expected) {
            return;
        } catch (RuntimeException other) {
            fail(what + " threw " + other.getClass().getName());
        }
    }

    private void expect(boolean condition, String what) {
        if (!condition) {
            fail(what);
        }
    }

    private void fail(String what) {
        failed++;
        if (failures.length() < 4096) {
            failures.append(what).append('\n');
        }
    }
}
