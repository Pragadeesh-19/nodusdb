package io.nodusdb.lake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnarUpsertTest {

    private static final LakeSchema SCHEMA = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("name", LakeSchema.Type.UTF8)));
    private static final LakeTable.Config CONFIG = new LakeTable.Config(1 << 20, 1 << 26, 1 << 10, 1 << 12, 0L,
            ParquetCodec.SNAPPY);
    private static final long NAN_PAYLOAD = 0x7FF8_0000_0000_0001L;

    @TempDir
    Path directory;

    @Test
    void columnarBatchMatchesRowUpsertsBitForBit() throws IOException {
        int rows = 5_000;
        long[] keys = new long[rows];
        long[] amounts = new long[rows];
        long[] scoreBits = new long[rows];
        int[] statuses = new int[rows];
        byte[][] names = new byte[rows][];
        for (int i = 0; i < rows; i++) {
            keys[i] = i + 1L;
            amounts[i] = i * 3L - 77;
            scoreBits[i] = i % 500 == 0 ? NAN_PAYLOAD : Double.doubleToRawLongBits(i * 0.25);
            statuses[i] = i % 11;
            names[i] = (i % 97 == 0 ? "" : "name-" + (i % 97)).getBytes(StandardCharsets.UTF_8);
        }
        try (LakeTable byRow = LakeTable.open(directory.resolve("row"), SCHEMA, CONFIG);
             LakeTable byColumn = LakeTable.open(directory.resolve("column"), SCHEMA, CONFIG)) {
            for (int i = 0; i < rows; i++) {
                UpsertArrays.upsert(byRow, keys[i], new long[] {amounts[i], scoreBits[i]}, new int[] {statuses[i]},
                        names[i], new int[] {names[i].length});
            }

            assertEquals(rows, byColumn.upsertColumns(ArrayRows.of(keys, amounts, scoreBits, statuses, names)));

            for (int i = 0; i < rows; i++) {
                assertSameRow(byRow.get(keys[i]), byColumn.get(keys[i]));
            }
            assertSameAggregate(byRow, byColumn);
        }
    }

    @Test
    void laterRowInBatchWinsForDuplicateKey() throws IOException {
        long[] keys = {1L, 1L};
        long[] amounts = {10L, 20L};
        long[] scoreBits = {Double.doubleToRawLongBits(1.0), Double.doubleToRawLongBits(2.0)};
        int[] statuses = {1, 2};
        byte[][] names = {"first".getBytes(StandardCharsets.UTF_8), "second".getBytes(StandardCharsets.UTF_8)};
        try (LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            table.upsertColumns(ArrayRows.of(keys, amounts, scoreBits, statuses, names));

            LakeRow row = table.get(1L).orElseThrow();
            assertEquals(20L, row.longValues()[0]);
            assertEquals("second", new String(row.varCharValues()[0], StandardCharsets.UTF_8));
        }
    }

    @Test
    void rejectedOffsetsLeaveTheTableUnchanged() throws IOException {
        ColumnarRows rows = ArrayRows.fromColumns(new long[] {1L, 2L, 3L},
                new long[][] {{1L, 2L, 3L}, {0L, 0L, 0L}},
                new int[][] {{1, 2, 3}},
                new int[][] {{0, 3, 2, 5}},
                new byte[][] {"abcde".getBytes(StandardCharsets.UTF_8)});
        try (LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            assertThrows(IllegalArgumentException.class, () -> table.upsertColumns(rows));

            assertEquals(0L, table.aggregate(0).count());
            assertFalse(table.get(1L).isPresent());
        }
    }

    @Test
    void layoutThatDoesNotMatchTheSchemaIsRejected() throws IOException {
        ColumnarRows rows = ArrayRows.fromColumns(new long[] {1L}, new long[][] {{1L}},
                new int[][] {{1}}, new int[][] {{0, 0}}, new byte[][] {new byte[0]});
        try (LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            assertThrows(IllegalArgumentException.class, () -> table.upsertColumns(rows));
        }
    }

    @Test
    void aggregatesSkipTombstonesInTheActiveBuffer() throws IOException {
        int rows = 1_000;
        long[] keys = new long[rows];
        long[] amounts = new long[rows];
        long[] scoreBits = new long[rows];
        int[] statuses = new int[rows];
        byte[][] names = new byte[rows][];
        long expectedAmount = 0;
        double expectedScore = 0;
        long expectedStatus = 0;
        for (int i = 0; i < rows; i++) {
            keys[i] = i + 1L;
            amounts[i] = i + 1L;
            scoreBits[i] = Double.doubleToRawLongBits(i + 0.5);
            statuses[i] = i % 7;
            names[i] = "n".getBytes(StandardCharsets.UTF_8);
            if (i >= 10) {
                expectedAmount += amounts[i];
                expectedScore += i + 0.5;
                expectedStatus += statuses[i];
            }
        }
        try (LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            table.upsertColumns(ArrayRows.of(keys, amounts, scoreBits, statuses, names));
            for (long key = 1; key <= 10; key++) {
                table.delete(key);
            }

            Aggregate amount = table.aggregate(0);
            Aggregate score = table.aggregate(1);
            Aggregate status = table.aggregate(2);

            assertEquals(990L, amount.count());
            assertEquals(expectedAmount, amount.sum(), 0.0);
            assertEquals(expectedScore, score.sum(), 0.0);
            assertEquals(expectedStatus, status.sum(), 0.0);
        }
    }

    @Test
    void sumsCoverTheUnrolledRemainder() throws IOException {
        int rows = 1_003;
        long[] keys = new long[rows];
        long[] amounts = new long[rows];
        long[] scoreBits = new long[rows];
        int[] statuses = new int[rows];
        byte[][] names = new byte[rows][];
        for (int i = 0; i < rows; i++) {
            keys[i] = i + 1L;
            amounts[i] = i + 1L;
            scoreBits[i] = Double.doubleToRawLongBits((i + 1) * 0.25);
            statuses[i] = 1;
            names[i] = new byte[0];
        }
        try (LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            table.upsertColumns(ArrayRows.of(keys, amounts, scoreBits, statuses, names));

            assertEquals(0.25 * 503_506, table.aggregate(1).sum(), 0.0);
            assertEquals(1_003 * 1_004 / 2L, table.aggregate(0).sum(), 0.0);
            assertEquals(1_003L, table.aggregate(2).sum());
            assertEquals(1_003L, table.aggregate(0).count());
        }
    }

    @Test
    void emptyTableHasZeroCountAndNumericAggregatesRejectText() throws IOException {
        try (LakeTable table = LakeTable.open(directory, SCHEMA, CONFIG)) {
            assertEquals(0L, table.aggregate(0).count());
            assertThrows(IllegalArgumentException.class, () -> table.aggregate(3));
        }
    }

    private static void assertSameRow(Optional<LakeRow> expected, Optional<LakeRow> actual) {
        assertEquals(expected.isPresent(), actual.isPresent());
        if (expected.isPresent()) {
            assertEquals(expected.get().keyHash(), actual.get().keyHash());
            assertArrayEquals(expected.get().longValues(), actual.get().longValues());
            assertArrayEquals(expected.get().intValues(), actual.get().intValues());
            assertEquals(expected.get().varCharValues().length, actual.get().varCharValues().length);
            for (int i = 0; i < expected.get().varCharValues().length; i++) {
                assertArrayEquals(expected.get().varCharValues()[i], actual.get().varCharValues()[i]);
            }
        }
    }

    private static void assertSameAggregate(LakeTable expected, LakeTable actual) {
        Aggregate want = expected.aggregate(0);
        Aggregate got = actual.aggregate(0);
        assertEquals(want.count(), got.count());
        assertEquals(want.sum(), got.sum(), 0.0);
    }

    private static final class ArrayRows {

        private ArrayRows() {
        }

        static ColumnarRows of(long[] keys, long[] amounts, long[] scoreBits, int[] statuses, byte[][] names) {
            int[] nameOffsets = new int[names.length + 1];
            int total = 0;
            for (int i = 0; i < names.length; i++) {
                nameOffsets[i] = total;
                total += names[i].length;
            }
            nameOffsets[names.length] = total;
            byte[] nameBytes = new byte[total];
            int cursor = 0;
            for (byte[] name : names) {
                System.arraycopy(name, 0, nameBytes, cursor, name.length);
                cursor += name.length;
            }
            return fromColumns(keys, new long[][] {amounts, scoreBits}, new int[][] {statuses},
                    new int[][] {nameOffsets}, new byte[][] {nameBytes});
        }

        static ColumnarRows fromColumns(long[] keys, long[][] longs, int[][] ints, int[][] offsets, byte[][] data) {
            List<MemorySegment> longSegments = new ArrayList<>();
            for (long[] column : longs) {
                longSegments.add(MemorySegment.ofArray(column));
            }
            List<MemorySegment> intSegments = new ArrayList<>();
            for (int[] column : ints) {
                intSegments.add(MemorySegment.ofArray(column));
            }
            List<MemorySegment> offsetSegments = new ArrayList<>();
            for (int[] column : offsets) {
                offsetSegments.add(MemorySegment.ofArray(column));
            }
            List<MemorySegment> dataSegments = new ArrayList<>();
            for (byte[] column : data) {
                dataSegments.add(MemorySegment.ofArray(column));
            }
            return new ColumnarRows(keys.length, MemorySegment.ofArray(keys), longSegments, intSegments,
                    offsetSegments, dataSegments);
        }
    }
}
