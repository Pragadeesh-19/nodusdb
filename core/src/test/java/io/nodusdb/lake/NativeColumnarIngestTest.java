package io.nodusdb.lake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeColumnarIngestTest {

    private static final LakeSchema SCHEMA = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("name", LakeSchema.Type.UTF8)));
    private static final LakeTable.Config CONFIG = new LakeTable.Config(1 << 20, 1 << 26, 1 << 4, 1 << 6, 0L,
            ParquetCodec.UNCOMPRESSED);

    @TempDir
    Path directory;

    @Test
    void nativeSegmentsProduceTheSameRowsAsArrays() throws IOException {
        byte[][] names = {"alpha".getBytes(StandardCharsets.UTF_8), new byte[0],
                "gamma-gamma".getBytes(StandardCharsets.UTF_8)};
        try (Arena arena = Arena.ofConfined();
             LakeTable byArray = LakeTable.open(directory.resolve("array"), SCHEMA, CONFIG);
             LakeTable byNative = LakeTable.open(directory.resolve("native"), SCHEMA, CONFIG)) {
            ColumnarRows heap = batch(new long[] {1L, 2L, 3L}, new long[] {10L, 20L, 30L},
                    new int[] {4, 5, 6}, names);
            ColumnarRows nativeRows = nativeBatch(arena, new long[] {1L, 2L, 3L}, new long[] {10L, 20L, 30L},
                    new int[] {4, 5, 6}, names);

            byArray.upsertColumns(heap);
            byNative.upsertColumns(nativeRows);

            for (long key = 1; key <= 3; key++) {
                LakeRow expected = byArray.get(key).orElseThrow();
                LakeRow actual = byNative.get(key).orElseThrow();
                assertEquals(expected.longValues()[0], actual.longValues()[0]);
                assertEquals(expected.intValues()[0], actual.intValues()[0]);
                assertArrayEquals(expected.varCharValues()[0], actual.varCharValues()[0]);
            }
        }
    }

    @Test
    void unalignedNativeAddressesAreReadInPlace() throws IOException {
        try (Arena arena = Arena.ofConfined(); LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            MemorySegment keys = misaligned(arena, Long.BYTES);
            keys.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 7L);
            MemorySegment amounts = misaligned(arena, Long.BYTES);
            amounts.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 99L);
            MemorySegment statuses = misaligned(arena, Integer.BYTES);
            statuses.set(ValueLayout.JAVA_INT_UNALIGNED, 0, 3);
            MemorySegment offsets = misaligned(arena, 2 * Integer.BYTES);
            offsets.set(ValueLayout.JAVA_INT_UNALIGNED, 0, 0);
            offsets.set(ValueLayout.JAVA_INT_UNALIGNED, Integer.BYTES, 3);
            MemorySegment data = arena.allocate(3);
            MemorySegment.copy("odd".getBytes(StandardCharsets.UTF_8), 0, data, ValueLayout.JAVA_BYTE, 0, 3);

            table.upsertColumns(new ColumnarRows(1, keys, List.of(amounts), List.of(statuses),
                    List.of(offsets), List.of(data)));

            assertEquals(99L, table.get(7L).orElseThrow().longValues()[0]);
        }
    }

    private static MemorySegment misaligned(Arena arena, long bytes) {
        return arena.allocate(bytes + 16, 16).asSlice(1, bytes);
    }

    @Test
    void longVarCharRowsGrowTheStagingBufferAndRoundTrip() throws IOException {
        int rows = 40;
        long[] keys = new long[rows];
        long[] amounts = new long[rows];
        int[] statuses = new int[rows];
        byte[][] names = new byte[rows][];
        for (int i = 0; i < rows; i++) {
            keys[i] = i + 1L;
            amounts[i] = i;
            statuses[i] = i;
            names[i] = "x".repeat(300 + i).getBytes(StandardCharsets.UTF_8);
        }
        try (Arena arena = Arena.ofConfined(); LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            assertEquals(rows, table.upsertColumns(nativeBatch(arena, keys, amounts, statuses, names)));

            for (int i = 0; i < rows; i++) {
                assertArrayEquals(names[i], table.get(keys[i]).orElseThrow().varCharValues()[0]);
            }
        }
    }

    @Test
    void offsetPastTheDataSegmentRejectsTheWholeBatch() throws IOException {
        try (Arena arena = Arena.ofConfined(); LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            ColumnarRows rows = nativeBatch(arena, new long[] {1L, 2L}, new long[] {1L, 2L}, new int[] {1, 2},
                    new byte[][] {"ab".getBytes(StandardCharsets.UTF_8), "cd".getBytes(StandardCharsets.UTF_8)});
            MemorySegment offsets = rows.varCharOffsets().get(0);
            offsets.set(ValueLayout.JAVA_INT_UNALIGNED, 2 * Integer.BYTES, 99);

            assertThrows(IllegalArgumentException.class, () -> table.upsertColumns(rows));

            assertEquals(0L, table.aggregate(0).count());
            assertFalse(table.get(1L).isPresent());
        }
    }

    @Test
    void recordRejectsColumnsShorterThanTheRowCount() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keys = arena.allocate(8);
            MemorySegment offsets = arena.allocate(2 * Integer.BYTES);
            assertThrows(IllegalArgumentException.class, () -> new ColumnarRows(2, keys, List.of(), List.of(),
                    List.of(offsets), List.of(arena.allocate(0))));
        }
    }

    private static ColumnarRows batch(long[] keys, long[] amounts, int[] statuses, byte[][] names) {
        return ArrayBatch.of(keys, amounts, statuses, names);
    }

    private static ColumnarRows nativeBatch(Arena arena, long[] keys, long[] amounts, int[] statuses,
                                            byte[][] names) {
        int total = 0;
        int[] offsets = new int[names.length + 1];
        for (int i = 0; i < names.length; i++) {
            offsets[i] = total;
            total += names[i].length;
        }
        offsets[names.length] = total;
        MemorySegment keySegment = copyOf(arena, keys);
        MemorySegment amountSegment = copyOf(arena, amounts);
        MemorySegment statusSegment = copyOf(arena, statuses);
        MemorySegment offsetSegment = copyOf(arena, offsets);
        MemorySegment dataSegment = arena.allocate(Math.max(1, total));
        int cursor = 0;
        for (byte[] name : names) {
            MemorySegment.copy(name, 0, dataSegment, ValueLayout.JAVA_BYTE, cursor, name.length);
            cursor += name.length;
        }
        return new ColumnarRows(keys.length, keySegment, List.of(amountSegment), List.of(statusSegment),
                List.of(offsetSegment), List.of(dataSegment));
    }

    private static MemorySegment copyOf(Arena arena, long[] values) {
        MemorySegment segment = arena.allocate((long) values.length * Long.BYTES, Long.BYTES);
        MemorySegment.copy(values, 0, segment, ValueLayout.JAVA_LONG, 0, values.length);
        return segment;
    }

    private static MemorySegment copyOf(Arena arena, int[] values) {
        MemorySegment segment = arena.allocate((long) values.length * Integer.BYTES, Integer.BYTES);
        MemorySegment.copy(values, 0, segment, ValueLayout.JAVA_INT, 0, values.length);
        return segment;
    }

    private static final class ArrayBatch {

        private ArrayBatch() {
        }

        static ColumnarRows of(long[] keys, long[] amounts, int[] statuses, byte[][] names) {
            int total = 0;
            int[] offsets = new int[names.length + 1];
            for (int i = 0; i < names.length; i++) {
                offsets[i] = total;
                total += names[i].length;
            }
            offsets[names.length] = total;
            byte[] data = new byte[total];
            int cursor = 0;
            for (byte[] name : names) {
                System.arraycopy(name, 0, data, cursor, name.length);
                cursor += name.length;
            }
            return new ColumnarRows(keys.length, MemorySegment.ofArray(keys),
                    List.of(MemorySegment.ofArray(amounts)), List.of(MemorySegment.ofArray(statuses)),
                    List.of(MemorySegment.ofArray(offsets)), List.of(MemorySegment.ofArray(data)));
        }
    }
}
