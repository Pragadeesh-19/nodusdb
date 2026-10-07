package io.nodusdb.lake.parquet;

import io.nodusdb.lake.codec.ParquetCodec;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetColumnSourceTest {

    private static final int SCHEMA = 2;
    private static final int SCHEMA_TYPE = 1;
    private static final int SCHEMA_NAME = 4;
    private static final int SCHEMA_CONVERTED_TYPE = 6;
    private static final int SCHEMA_FIELD_ID = 9;
    private static final int SCHEMA_LOGICAL_TYPE = 10;
    private static final int CHUNK_META_DATA = 3;
    private static final int META_DICTIONARY_PAGE_OFFSET = 11;

    @TempDir
    Path directory;

    private static long micros(int row) {
        return 1_700_000_000_000_000L + row * 1_000_000L;
    }

    private static List<ColumnSpec> logSpecs() {
        return List.of(
                ColumnSpec.withFieldId("lsn", ColumnType.INT64, 1),
                ColumnSpec.withFieldId("commit_ts", ColumnType.TIMESTAMP_MICROS, 2),
                ColumnSpec.withFieldId("schema_version", ColumnType.INT32, 3),
                ColumnSpec.withFieldId("event", ColumnType.STRING, 4));
    }

    private ArrayColumnSource logSource(int rows) {
        long[] lsn = new long[rows];
        long[] ts = new long[rows];
        long[] version = new long[rows];
        String[] event = new String[rows];
        for (int i = 0; i < rows; i++) {
            lsn[i] = i + 1L;
            ts[i] = micros(i / 1_000);
            version[i] = i % 3 - 1;
            event[i] = i % 7 == 0 ? "" : (i % 2 == 0 ? "add" : "removeé");
        }
        List<ColumnSpec> specs = logSpecs();
        return ArrayColumnSource.of(rows).fixed(specs.get(0), lsn).fixed(specs.get(1), ts)
                .fixed(specs.get(2), version).strings(specs.get(3), event);
    }

    private static Map<Integer, Object> footer(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        int length = ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return new ThriftCompactReader(bytes, bytes.length - 8 - length).readStruct();
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, Object> schemaElement(Path file, int index) throws IOException {
        return (Map<Integer, Object>) ((List<Object>) footer(file).get(SCHEMA)).get(index);
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, Object> chunkMetadata(Path file, int group, int column) throws IOException {
        Map<Integer, Object> rowGroup = (Map<Integer, Object>) ((List<Object>) footer(file).get(4)).get(group);
        Map<Integer, Object> chunk = (Map<Integer, Object>) ((List<Object>) rowGroup.get(1)).get(column);
        return (Map<Integer, Object>) chunk.get(CHUNK_META_DATA);
    }

    @Test
    void fieldIdsAndTheTimestampLogicalTypeAreWrittenIntoTheSchema() throws IOException {
        Path file = directory.resolve("log.parquet");

        ParquetWriter.write(file, logSource(50), ParquetCodec.SNAPPY);

        Map<Integer, Object> root = schemaElement(file, 0);
        assertFalse(root.containsKey(SCHEMA_FIELD_ID));
        assertEquals(4L, root.get(5));
        Map<Integer, Object> lsn = schemaElement(file, 1);
        assertEquals(1L, lsn.get(SCHEMA_FIELD_ID));
        assertEquals(2L, lsn.get(SCHEMA_TYPE));
        assertFalse(lsn.containsKey(SCHEMA_CONVERTED_TYPE));
        assertFalse(lsn.containsKey(SCHEMA_LOGICAL_TYPE));
        Map<Integer, Object> timestamp = schemaElement(file, 2);
        assertEquals(2L, timestamp.get(SCHEMA_FIELD_ID));
        assertEquals(2L, timestamp.get(SCHEMA_TYPE));
        assertEquals(10L, timestamp.get(SCHEMA_CONVERTED_TYPE));
        @SuppressWarnings("unchecked")
        Map<Integer, Object> logical = (Map<Integer, Object>) timestamp.get(SCHEMA_LOGICAL_TYPE);
        @SuppressWarnings("unchecked")
        Map<Integer, Object> timestampType = (Map<Integer, Object>) logical.get(8);
        assertEquals(Boolean.TRUE, timestampType.get(1));
        @SuppressWarnings("unchecked")
        Map<Integer, Object> unit = (Map<Integer, Object>) timestampType.get(2);
        assertTrue(unit.containsKey(2), "the unit must be microseconds");
        assertEquals(1, unit.size());
        Map<Integer, Object> version = schemaElement(file, 3);
        assertEquals(3L, version.get(SCHEMA_FIELD_ID));
        assertEquals(1L, version.get(SCHEMA_TYPE));
        Map<Integer, Object> event = schemaElement(file, 4);
        assertEquals(4L, event.get(SCHEMA_FIELD_ID));
        assertEquals(0L, event.get(SCHEMA_CONVERTED_TYPE));
        assertEquals("event", new String((byte[]) event.get(SCHEMA_NAME), StandardCharsets.UTF_8));
    }

    @Test
    void columnsWithoutFieldIdsWriteNone() throws IOException {
        Path file = directory.resolve("plain.parquet");
        ArrayColumnSource source = ArrayColumnSource.of(3)
                .fixed(ColumnSpec.of("a", ColumnType.INT64), 1, 2, 3)
                .strings(ColumnSpec.of("b", ColumnType.STRING), "x", "y", "z");

        ParquetWriter.write(file, source, ParquetCodec.UNCOMPRESSED);

        assertFalse(schemaElement(file, 1).containsKey(SCHEMA_FIELD_ID));
        assertFalse(schemaElement(file, 2).containsKey(SCHEMA_FIELD_ID));
        assertEquals(2, ((List<?>) footer(file).get(SCHEMA)).size() - 1);
    }

    @Test
    void everyColumnTypeRoundTripsAcrossRowGroups() throws IOException {
        int rows = 2 * ParquetWriter.ROW_GROUP_ROWS + 1_234;
        Path file = directory.resolve("groups.parquet");

        ParquetWriter.write(file, logSource(rows), ParquetCodec.SNAPPY);
        ParquetReader.Columns columns = ParquetReader.read(file, logSpecs());

        assertEquals(rows, columns.fixed()[0].length);
        for (int i = 0; i < rows; i++) {
            assertEquals(i + 1L, columns.fixed()[0][i], "lsn " + i);
            assertEquals(micros(i / 1_000), columns.fixed()[1][i], "ts " + i);
            assertEquals(i % 3 - 1, columns.fixed()[2][i], "version " + i);
            String expected = i % 7 == 0 ? "" : (i % 2 == 0 ? "add" : "removeé");
            assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), columns.strings()[3][i], "event " + i);
        }
        assertEquals(3, ((List<?>) footer(file).get(4)).size());
    }

    @Test
    void anEmptySourceWritesAValidFileWithoutBounds() throws IOException {
        Path file = directory.resolve("empty.parquet");

        WrittenFile written = ParquetWriter.write(file, logSource(0), ParquetCodec.SNAPPY);

        assertEquals(0, written.rowCount());
        assertEquals(Files.size(file), written.fileBytes());
        for (ColumnMetrics metrics : written.columns()) {
            assertEquals(0, metrics.valueCount());
            assertFalse(metrics.hasBounds());
        }
        assertEquals(0, ParquetReader.read(file, logSpecs()).fixed()[0].length);
    }

    @Test
    void theWrittenFileReportsItsRowsSizeAndPerColumnBounds() throws IOException {
        int rows = ParquetWriter.ROW_GROUP_ROWS + 500;
        Path file = directory.resolve("metrics.parquet");

        WrittenFile written = ParquetWriter.write(file, logSource(rows), ParquetCodec.SNAPPY);

        assertEquals(rows, written.rowCount());
        assertEquals(Files.size(file), written.fileBytes());
        assertEquals(4, written.columns().size());
        ColumnMetrics lsn = written.columns().get(0);
        assertEquals(1, lsn.fieldId());
        assertEquals("lsn", lsn.name());
        assertEquals(rows, lsn.valueCount());
        assertEquals(0, lsn.nullCount());
        assertTrue(lsn.compressedBytes() > 0);
        assertArrayEquals(littleEndian(1, 8), lsn.lowerBound());
        assertArrayEquals(littleEndian(rows, 8), lsn.upperBound());
        ColumnMetrics timestamp = written.columns().get(1);
        assertArrayEquals(littleEndian(micros(0), 8), timestamp.lowerBound());
        assertArrayEquals(littleEndian(micros((rows - 1) / 1_000), 8), timestamp.upperBound());
        ColumnMetrics version = written.columns().get(2);
        assertArrayEquals(littleEndian(-1, 4), version.lowerBound());
        assertArrayEquals(littleEndian(1, 4), version.upperBound());
        ColumnMetrics event = written.columns().get(3);
        assertArrayEquals(new byte[0], event.lowerBound());
        assertArrayEquals("removeé".getBytes(StandardCharsets.UTF_8), event.upperBound());
    }

    @Test
    void boundsCombineAcrossRowGroupsInTheRightOrder() throws IOException {
        int rows = 2 * ParquetWriter.ROW_GROUP_ROWS;
        long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = rows - i - 1_000_000L;
        }
        ArrayColumnSource source = ArrayColumnSource.of(rows).fixed(ColumnSpec.of("v", ColumnType.INT64), values);

        WrittenFile written = ParquetWriter.write(directory.resolve("order.parquet"), source, ParquetCodec.SNAPPY);

        assertArrayEquals(littleEndian(1 - 1_000_000L, 8), written.columns().get(0).lowerBound());
        assertArrayEquals(littleEndian(rows - 1_000_000L, 8), written.columns().get(0).upperBound());
    }

    @Test
    void boundsAcrossGroupsOfOppositeSignOrderBySignNotByRawBytes() throws IOException {
        int rows = ParquetWriter.ROW_GROUP_ROWS + 3;
        long[] values = new long[rows];
        for (int i = 0; i < ParquetWriter.ROW_GROUP_ROWS; i++) {
            values[i] = 10 + i;
        }
        values[ParquetWriter.ROW_GROUP_ROWS] = -5;
        values[ParquetWriter.ROW_GROUP_ROWS + 1] = -2;
        values[ParquetWriter.ROW_GROUP_ROWS + 2] = -9;
        ArrayColumnSource source = ArrayColumnSource.of(rows).fixed(ColumnSpec.of("v", ColumnType.INT64), values);

        WrittenFile written = ParquetWriter.write(directory.resolve("signs2.parquet"), source, ParquetCodec.SNAPPY);

        assertArrayEquals(littleEndian(-9, 8), written.columns().get(0).lowerBound());
        assertArrayEquals(littleEndian(10 + ParquetWriter.ROW_GROUP_ROWS - 1, 8),
                written.columns().get(0).upperBound());
    }

    @Test
    void oneChunkWithoutStatisticsMakesTheFileBoundsAbsent() throws IOException {
        int rows = ParquetWriter.ROW_GROUP_ROWS + 2;
        long[] bits = new long[rows];
        for (int i = 0; i < rows; i++) {
            bits[i] = Double.doubleToRawLongBits(1.5 + i);
        }
        bits[rows - 1] = Double.doubleToRawLongBits(0.0);
        ArrayColumnSource source = ArrayColumnSource.of(rows).fixed(ColumnSpec.of("d", ColumnType.DOUBLE), bits);

        WrittenFile written = ParquetWriter.write(directory.resolve("partial.parquet"), source, ParquetCodec.SNAPPY);

        assertNull(written.columns().get(0).lowerBound());
        assertNull(written.columns().get(0).upperBound());
    }

    @Test
    void negativeValuesOrderBySignNotByRawBytes() throws IOException {
        ArrayColumnSource source = ArrayColumnSource.of(4)
                .fixed(ColumnSpec.of("v", ColumnType.INT64), 5, -3, 200, -1_000);

        WrittenFile written = ParquetWriter.write(directory.resolve("signs.parquet"), source, ParquetCodec.SNAPPY);

        assertArrayEquals(littleEndian(-1_000, 8), written.columns().get(0).lowerBound());
        assertArrayEquals(littleEndian(200, 8), written.columns().get(0).upperBound());
    }

    @Test
    void stringBoundsOrderByUnsignedBytes() throws IOException {
        ArrayColumnSource source = ArrayColumnSource.of(3)
                .strings(ColumnSpec.of("s", ColumnType.STRING), "b", "é", "a");

        WrittenFile written = ParquetWriter.write(directory.resolve("text.parquet"), source, ParquetCodec.SNAPPY);

        assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), written.columns().get(0).lowerBound());
        assertArrayEquals("é".getBytes(StandardCharsets.UTF_8), written.columns().get(0).upperBound());
    }

    @Test
    void aDoubleColumnWithZeroHasNoBounds() throws IOException {
        long[] bits = {Double.doubleToRawLongBits(1.5), Double.doubleToRawLongBits(0.0)};
        ArrayColumnSource source = ArrayColumnSource.of(2).fixed(ColumnSpec.of("d", ColumnType.DOUBLE), bits);

        WrittenFile written = ParquetWriter.write(directory.resolve("double.parquet"), source, ParquetCodec.SNAPPY);

        assertNull(written.columns().get(0).lowerBound());
        assertFalse(written.columns().get(0).hasBounds());
    }

    @Test
    void doubleBoundsOrderByValue() throws IOException {
        long[] bits = {Double.doubleToRawLongBits(2.5), Double.doubleToRawLongBits(-7.25),
                Double.doubleToRawLongBits(10.0)};
        ArrayColumnSource source = ArrayColumnSource.of(3).fixed(ColumnSpec.of("d", ColumnType.DOUBLE), bits);

        WrittenFile written = ParquetWriter.write(directory.resolve("double.parquet"), source, ParquetCodec.SNAPPY);

        assertArrayEquals(littleEndian(Double.doubleToRawLongBits(-7.25), 8), written.columns().get(0).lowerBound());
        assertArrayEquals(littleEndian(Double.doubleToRawLongBits(10.0), 8), written.columns().get(0).upperBound());
    }

    @Test
    void aUniqueColumnSkipsTheDictionaryAndAPlainOneUsesIt() throws IOException {
        long[] repeated = new long[1_000];
        ArrayColumnSource source = ArrayColumnSource.of(1_000)
                .fixed(new ColumnSpec("unique", ColumnType.INT64, ColumnSpec.NO_FIELD_ID, true), repeated)
                .fixed(ColumnSpec.of("plain", ColumnType.INT64), repeated);
        Path file = directory.resolve("unique.parquet");

        ParquetWriter.write(file, source, ParquetCodec.UNCOMPRESSED);

        assertFalse(chunkMetadata(file, 0, 0).containsKey(META_DICTIONARY_PAGE_OFFSET));
        assertTrue(chunkMetadata(file, 0, 1).containsKey(META_DICTIONARY_PAGE_OFFSET));
    }

    @Test
    void bothCodecsProduceFilesThatReadBackIdentically() throws IOException {
        Path snappy = directory.resolve("snappy.parquet");
        Path plain = directory.resolve("plain.parquet");

        ParquetWriter.write(snappy, logSource(5_000), ParquetCodec.SNAPPY);
        ParquetWriter.write(plain, logSource(5_000), ParquetCodec.UNCOMPRESSED);

        ParquetReader.Columns a = ParquetReader.read(snappy, logSpecs());
        ParquetReader.Columns b = ParquetReader.read(plain, logSpecs());
        for (int column = 0; column < 3; column++) {
            assertArrayEquals(a.fixed()[column], b.fixed()[column]);
        }
        assertEquals(a.strings()[3].length, b.strings()[3].length);
        for (int i = 0; i < a.strings()[3].length; i++) {
            assertArrayEquals(a.strings()[3][i], b.strings()[3][i]);
        }
    }

    @Test
    void theSpecDrivenReaderRefusesAFileWithAnotherColumnCount() throws IOException {
        Path file = directory.resolve("log.parquet");
        ParquetWriter.write(file, logSource(10), ParquetCodec.SNAPPY);

        assertThrows(IOException.class, () -> ParquetReader.read(file, logSpecs().subList(0, 2)));
    }

    @Test
    void aColumnSpecValidatesItsFields() {
        assertThrows(NullPointerException.class, () -> ColumnSpec.of("a", null));
        assertThrows(IllegalArgumentException.class, () -> ColumnSpec.of("", ColumnType.INT64));
        assertThrows(IllegalArgumentException.class, () -> ColumnSpec.of(null, ColumnType.INT64));
        assertThrows(IllegalArgumentException.class, () -> ColumnSpec.withFieldId("a", ColumnType.INT64, -1));
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnSpec("a", ColumnType.STRING, ColumnSpec.NO_FIELD_ID, true));
        assertTrue(ColumnSpec.withFieldId("a", ColumnType.INT64, 5).hasFieldId());
        assertFalse(ColumnSpec.of("a", ColumnType.INT64).hasFieldId());
    }

    private static byte[] littleEndian(long value, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = (byte) (value >>> (8 * i));
        }
        return bytes;
    }
}
