package io.nodusdb.lake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetRoundTripTest {

    private static final int ROWS = 100_000;
    private static final long KEY_STRIDE = 0x9E3779B97F4A7C15L;
    private static final String GLYPHS = "abcXYZ019 -_éü中文☃";
    private static final LakeSchema SCHEMA = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("label", LakeSchema.Type.UTF8)));

    @TempDir
    Path directory;

    @Test
    void hundredThousandMixedRowsRoundTripBitForBit() throws IOException {
        Random random = new Random(20260);
        DeltaMemTable table = new DeltaMemTable(SCHEMA.memtableSchema(), 1 << 17, 1 << 22);
        for (int i = 0; i < ROWS; i++) {
            long key = (i + 1) * KEY_STRIDE;
            long[] longs = {random.nextLong(), random.nextLong()};
            int[] ints = {random.nextInt()};
            byte[] label = randomLabel(random).getBytes(StandardCharsets.UTF_8);
            UpsertArrays.upsert(table, key, longs, ints, label, new int[] {label.length});
        }
        table.assertInvariant();

        int[] rows = new int[table.size()];
        for (int row = 0; row < rows.length; row++) {
            rows[row] = row;
        }
        Path file = directory.resolve("roundtrip.parquet");
        try (RowSelection selection = RowSelection.of(rows)) {
            ParquetWriter.write(file, SCHEMA, table, selection, ParquetCodec.SNAPPY);
        }
        ParquetReader.Contents contents = ParquetReader.read(file, SCHEMA);

        assertEquals(ROWS, contents.keyHashes().length);
        for (int row = 0; row < ROWS; row++) {
            assertEquals(table.keyHashAt(row), contents.keyHashes()[row], "key row " + row);
            assertEquals(table.longAt(0, row), contents.longValues()[0][row], "amount row " + row);
            assertEquals(table.longAt(1, row), contents.longValues()[1][row], "score bits row " + row);
            assertEquals(table.intAt(0, row), contents.intValues()[0][row], "status row " + row);
            byte[] expected = new byte[table.varCharLength(0, row)];
            table.copyVarChar(0, row, expected, 0);
            assertArrayEquals(expected, contents.varCharValues()[0][row], "label row " + row);
        }
    }

    @Test
    void tombstoneFileRoundTripsKeysOnly() throws IOException {
        DeltaMemTable table = new DeltaMemTable(LakeSchema.KEYS_ONLY.memtableSchema(), 8, 16);
        table.tombstone(11L);
        table.tombstone(22L);
        Path file = directory.resolve("deletes.parquet");

        try (RowSelection selection = RowSelection.of(new int[] {0, 1})) {
            ParquetWriter.write(file, LakeSchema.KEYS_ONLY, table, selection, ParquetCodec.SNAPPY);

        }
        ParquetReader.Contents contents = ParquetReader.read(file, LakeSchema.KEYS_ONLY);

        assertArrayEquals(new long[] {11L, 22L}, contents.keyHashes());
    }

    @Test
    void uncompressedFileRoundTripsWithItsDictionaryPages() throws IOException {
        DeltaMemTable table = new DeltaMemTable(SCHEMA.memtableSchema(), 1 << 12, 1 << 16);
        for (int i = 0; i < 3_000; i++) {
            byte[] label = (i % 2 == 0 ? "even" : "odd").getBytes(StandardCharsets.UTF_8);
            UpsertArrays.upsert(table, i + 1L, new long[] {i, Double.doubleToRawLongBits(i * 0.5)}, new int[] {i % 4}, label,
                    new int[] {label.length});
        }
        int[] rows = new int[table.size()];
        for (int row = 0; row < rows.length; row++) {
            rows[row] = row;
        }
        Path file = directory.resolve("uncompressed.parquet");

        try (RowSelection selection = RowSelection.of(rows)) {
            ParquetWriter.write(file, SCHEMA, table, selection, ParquetCodec.UNCOMPRESSED);

        }
        ParquetReader.Contents contents = ParquetReader.read(file, SCHEMA);

        assertEquals(3_000, contents.keyHashes().length);
        assertEquals(table.keyHashAt(2_999), contents.keyHashes()[2_999]);
        assertEquals(table.longAt(0, 1_234), contents.longValues()[0][1_234]);
        assertArrayEquals("odd".getBytes(StandardCharsets.UTF_8), contents.varCharValues()[0][2_001]);
    }

    @Test
    void emptySelectionRoundTrips() throws IOException {
        DeltaMemTable table = new DeltaMemTable(SCHEMA.memtableSchema(), 4, 16);
        Path file = directory.resolve("empty.parquet");

        try (RowSelection selection = RowSelection.of(new int[0])) {
            ParquetWriter.write(file, SCHEMA, table, selection, ParquetCodec.SNAPPY);

        }
        ParquetReader.Contents contents = ParquetReader.read(file, SCHEMA);

        assertEquals(0, contents.keyHashes().length);
    }

    private static String randomLabel(Random random) {
        int length = random.nextInt(24);
        StringBuilder label = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            label.append(GLYPHS.charAt(random.nextInt(GLYPHS.length())));
        }
        return label.toString();
    }
}
