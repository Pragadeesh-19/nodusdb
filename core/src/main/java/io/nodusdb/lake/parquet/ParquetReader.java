package io.nodusdb.lake.parquet;

import io.nodusdb.lake.buffer.DeltaMemTable;
import io.nodusdb.lake.model.LakeSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class ParquetReader {

    private static final int FOOTER_ROW_GROUPS = 4;
    private static final int FOOTER_NUM_ROWS = 3;
    private static final int ROW_GROUP_COLUMNS = 1;
    private static final int ROW_GROUP_NUM_ROWS = 3;

    public record Contents(long[] keyHashes, long[][] longValues, int[][] intValues, byte[][][] varCharValues) {
    }

    public record Columns(long[][] fixed, byte[][][] strings) {
    }

    private record Parsed(byte[] file, List<?> rowGroups, int totalRows) {
    }

    private ParquetReader() {
    }

    public static Contents read(Path path, LakeSchema schema) throws IOException {
        Parsed parsed = parse(path);
        byte[] file = parsed.file();
        int totalRows = parsed.totalRows();
        int columnCount = schema.fields().size() + 1;
        int[] slots = schema.slots();
        DeltaMemTable.Schema shape = schema.memtableSchema();
        long[] keyHashes = new long[totalRows];
        long[][] longValues = new long[shape.longColumns()][totalRows];
        int[][] intValues = new int[shape.intColumns()][totalRows];
        byte[][][] varCharValues = new byte[shape.varCharColumns()][totalRows][];

        int filled = 0;
        for (Object groupObject : parsed.rowGroups()) {
            Map<?, ?> group = (Map<?, ?>) groupObject;
            List<?> chunks = list(group.get(ROW_GROUP_COLUMNS));
            int groupRows = Math.toIntExact(longValue(group, ROW_GROUP_NUM_ROWS));
            requireShape(path, chunks.size(), columnCount, groupRows, totalRows - filled);
            ColumnChunkReader.readLongs(file, chunks.get(0), groupRows, keyHashes, filled);
            for (int i = 0; i < schema.fields().size(); i++) {
                Object chunk = chunks.get(i + 1);
                switch (schema.fields().get(i).type()) {
                    case INT64, DOUBLE -> ColumnChunkReader.readLongs(file, chunk, groupRows,
                            longValues[slots[i]], filled);
                    case INT32 -> ColumnChunkReader.readInts(file, chunk, groupRows, intValues[slots[i]], filled);
                    case UTF8 -> ColumnChunkReader.readStrings(file, chunk, groupRows,
                            varCharValues[slots[i]], filled);
                }
            }
            filled += groupRows;
        }
        requireComplete(path, filled, totalRows);
        return new Contents(keyHashes, longValues, intValues, varCharValues);
    }

    public static Columns read(Path path, List<ColumnSpec> columns) throws IOException {
        Parsed parsed = parse(path);
        byte[] file = parsed.file();
        int totalRows = parsed.totalRows();
        long[][] fixed = new long[columns.size()][];
        byte[][][] strings = new byte[columns.size()][][];
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).type().variableWidth()) {
                strings[i] = new byte[totalRows][];
            } else {
                fixed[i] = new long[totalRows];
            }
        }
        int filled = 0;
        for (Object groupObject : parsed.rowGroups()) {
            Map<?, ?> group = (Map<?, ?>) groupObject;
            List<?> chunks = list(group.get(ROW_GROUP_COLUMNS));
            int groupRows = Math.toIntExact(longValue(group, ROW_GROUP_NUM_ROWS));
            requireShape(path, chunks.size(), columns.size(), groupRows, totalRows - filled);
            for (int i = 0; i < columns.size(); i++) {
                Object chunk = chunks.get(i);
                switch (columns.get(i).type()) {
                    case INT64, TIMESTAMP_MICROS, DOUBLE -> ColumnChunkReader.readLongs(file, chunk, groupRows,
                            fixed[i], filled);
                    case INT32 -> readWidenedInts(file, chunk, groupRows, fixed[i], filled);
                    case STRING -> ColumnChunkReader.readStrings(file, chunk, groupRows, strings[i], filled);
                }
            }
            filled += groupRows;
        }
        requireComplete(path, filled, totalRows);
        return new Columns(fixed, strings);
    }

    private static void readWidenedInts(byte[] file, Object chunk, int rows, long[] destination, int offset)
            throws IOException {
        int[] narrow = new int[rows];
        ColumnChunkReader.readInts(file, chunk, rows, narrow, 0);
        for (int i = 0; i < rows; i++) {
            destination[offset + i] = narrow[i];
        }
    }

    private static Parsed parse(Path path) throws IOException {
        byte[] file = Files.readAllBytes(path);
        requireMagic(file);
        int footerLength = ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int footerStart = file.length - 8 - footerLength;
        if (footerLength <= 0 || footerStart < 4) {
            throw new IOException("invalid parquet footer length in " + path);
        }
        Map<Integer, Object> metadata = new ThriftCompactReader(file, footerStart).readStruct();
        int totalRows = Math.toIntExact(longValue(metadata, FOOTER_NUM_ROWS));
        return new Parsed(file, list(metadata.get(FOOTER_ROW_GROUPS)), totalRows);
    }

    private static void requireShape(Path path, int found, int expected, int groupRows, int rowsLeft)
            throws IOException {
        if (found != expected) {
            throw new IOException("expected " + expected + " columns in " + path + " but found " + found);
        }
        if (groupRows < 0 || groupRows > rowsLeft) {
            throw new IOException("row group rows exceed the footer's row count in " + path);
        }
    }

    private static void requireComplete(Path path, int filled, int totalRows) throws IOException {
        if (filled != totalRows) {
            throw new IOException("row groups hold " + filled + " rows but the footer says " + totalRows
                    + " in " + path);
        }
    }

    private static void requireMagic(byte[] file) throws IOException {
        if (file.length < 12 || !Arrays.equals(Arrays.copyOf(file, 4), ParquetWriter.MAGIC)
                || !Arrays.equals(Arrays.copyOfRange(file, file.length - 4, file.length), ParquetWriter.MAGIC)) {
            throw new IOException("not a parquet file");
        }
    }

    private static long longValue(Map<?, ?> map, int id) throws IOException {
        if (!(map.get(id) instanceof Long number)) {
            throw new IOException("missing numeric thrift field " + id);
        }
        return number;
    }

    private static List<?> list(Object value) throws IOException {
        if (!(value instanceof List<?> items)) {
            throw new IOException("missing thrift list");
        }
        return items;
    }
}
