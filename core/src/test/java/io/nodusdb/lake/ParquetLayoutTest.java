package io.nodusdb.lake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetLayoutTest {

    private static final int ROWS = 2 * ParquetWriter.ROW_GROUP_ROWS + 1_000;
    private static final int COLUMN_AMOUNT = 1;
    private static final int COLUMN_SCORE = 2;
    private static final int COLUMN_STATUS = 3;
    private static final int COLUMN_LABEL = 4;
    private static final int CHUNK_META_DATA = 3;
    private static final int META_DICTIONARY_PAGE_OFFSET = 11;
    private static final int META_ENCODINGS = 2;
    private static final int META_STATISTICS = 12;
    private static final int STATISTICS_MAX = 5;
    private static final int STATISTICS_MIN = 6;
    private static final int ENCODING_RLE_DICTIONARY = 8;
    private static final String[] LABELS = {"gamma", "alpha", "beta"};
    private static final LakeSchema SCHEMA = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("label", LakeSchema.Type.UTF8)));

    @TempDir
    Path directory;

    @Test
    void rowGroupsRoundTripInRowOrder() throws IOException {
        DeltaMemTable table = fill(ROWS);
        Path file = write(table);

        ParquetReader.Contents contents = ParquetReader.read(file, SCHEMA);

        assertEquals(3, rowGroupCount(file));
        assertEquals(ROWS, contents.keyHashes().length);
        for (int row = 0; row < ROWS; row++) {
            assertEquals(table.keyHashAt(row), contents.keyHashes()[row], "key row " + row);
            assertEquals(table.longAt(0, row), contents.longValues()[0][row], "amount row " + row);
            assertEquals(table.longAt(1, row), contents.longValues()[1][row], "score row " + row);
            assertEquals(table.intAt(0, row), contents.intValues()[0][row], "status row " + row);
            assertArrayEquals(label(table, row), contents.varCharValues()[0][row], "label row " + row);
        }
    }

    @Test
    void onlyLowCardinalityColumnsAreDictionaryEncoded() throws IOException {
        Path file = write(fill(ROWS));

        Map<?, ?> status = chunk(file, 0, COLUMN_STATUS);
        Map<?, ?> label = chunk(file, 0, COLUMN_LABEL);
        Map<?, ?> amount = chunk(file, 0, COLUMN_AMOUNT);

        assertTrue(status.containsKey(META_DICTIONARY_PAGE_OFFSET));
        assertTrue(label.containsKey(META_DICTIONARY_PAGE_OFFSET));
        assertTrue(((List<?>) label.get(META_ENCODINGS)).contains((long) ENCODING_RLE_DICTIONARY));
        assertFalse(amount.containsKey(META_DICTIONARY_PAGE_OFFSET));
    }

    @Test
    void chunksCarryMinAndMaxStatistics() throws IOException {
        Path file = write(fill(ROWS));
        long lastAmount = (long) (ParquetWriter.ROW_GROUP_ROWS - 1) * 7_919L;

        Map<?, ?> amountStatistics = statistics(chunk(file, 0, COLUMN_AMOUNT));
        Map<?, ?> labelStatistics = statistics(chunk(file, 0, COLUMN_LABEL));

        assertArrayEquals(littleEndian(0L), (byte[]) amountStatistics.get(STATISTICS_MIN));
        assertArrayEquals(littleEndian(lastAmount), (byte[]) amountStatistics.get(STATISTICS_MAX));
        assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), (byte[]) labelStatistics.get(STATISTICS_MIN));
        assertArrayEquals("gamma".getBytes(StandardCharsets.UTF_8), (byte[]) labelStatistics.get(STATISTICS_MAX));
    }

    @Test
    void doubleColumnWithZeroHasNoStatistics() throws IOException {
        Path file = write(fill(ROWS));

        assertFalse(chunk(file, 0, COLUMN_SCORE).containsKey(META_STATISTICS));
    }

    private DeltaMemTable fill(int rows) {
        DeltaMemTable table = new DeltaMemTable(SCHEMA.memtableSchema(), 1 << 18, 1 << 22);
        for (int i = 0; i < rows; i++) {
            long[] longs = {i * 7_919L, Double.doubleToRawLongBits(i * 0.5)};
            int[] ints = {i % 5};
            byte[] label = LABELS[i % LABELS.length].getBytes(StandardCharsets.UTF_8);
            UpsertArrays.upsert(table, i + 1L, longs, ints, label, new int[] {label.length});
        }
        return table;
    }

    private Path write(DeltaMemTable table) throws IOException {
        int[] rows = new int[table.size()];
        for (int row = 0; row < rows.length; row++) {
            rows[row] = row;
        }
        Path file = directory.resolve("layout.parquet");
        try (RowSelection selection = RowSelection.of(rows)) {
            ParquetWriter.write(file, SCHEMA, table, selection, ParquetCodec.SNAPPY);
        }
        return file;
    }

    private static byte[] label(DeltaMemTable table, int row) {
        byte[] value = new byte[table.varCharLength(0, row)];
        table.copyVarChar(0, row, value, 0);
        return value;
    }

    private static int rowGroupCount(Path file) throws IOException {
        return ((List<?>) footer(file).get(4)).size();
    }

    private static Map<?, ?> chunk(Path file, int group, int column) throws IOException {
        Map<?, ?> rowGroup = (Map<?, ?>) ((List<?>) footer(file).get(4)).get(group);
        Map<?, ?> chunk = (Map<?, ?>) ((List<?>) rowGroup.get(1)).get(column);
        return (Map<?, ?>) chunk.get(CHUNK_META_DATA);
    }

    private static Map<?, ?> statistics(Map<?, ?> columnMetadata) {
        return (Map<?, ?>) columnMetadata.get(META_STATISTICS);
    }

    private static Map<Integer, Object> footer(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        int length = ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return new ThriftCompactReader(bytes, bytes.length - 8 - length).readStruct();
    }

    private static byte[] littleEndian(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }
}
