package io.nodusdb.lake;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

final class ParquetReader {

    private static final int FOOTER_ROW_GROUPS = 4;
    private static final int FOOTER_NUM_ROWS = 3;
    private static final int ROW_GROUP_COLUMNS = 1;
    private static final int ROW_GROUP_NUM_ROWS = 3;

    record Contents(long[] keyHashes, long[][] longValues, int[][] intValues, byte[][][] varCharValues) {
    }

    private ParquetReader() {
    }

    static Contents read(Path path, LakeSchema schema) throws IOException {
        byte[] file = Files.readAllBytes(path);
        requireMagic(file);
        int footerLength = ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int footerStart = file.length - 8 - footerLength;
        if (footerLength <= 0 || footerStart < 4) {
            throw new IOException("invalid parquet footer length in " + path);
        }
        Map<Integer, Object> metadata = new ThriftCompactReader(file, footerStart).readStruct();
        int totalRows = Math.toIntExact(longValue(metadata, FOOTER_NUM_ROWS));
        List<?> rowGroups = list(metadata.get(FOOTER_ROW_GROUPS));
        int columnCount = schema.fields().size() + 1;
        int[] slots = schema.slots();
        DeltaMemTable.Schema shape = schema.memtableSchema();
        long[] keyHashes = new long[totalRows];
        long[][] longValues = new long[shape.longColumns()][totalRows];
        int[][] intValues = new int[shape.intColumns()][totalRows];
        byte[][][] varCharValues = new byte[shape.varCharColumns()][totalRows][];

        int filled = 0;
        for (Object groupObject : rowGroups) {
            Map<?, ?> group = (Map<?, ?>) groupObject;
            List<?> chunks = list(group.get(ROW_GROUP_COLUMNS));
            int groupRows = Math.toIntExact(longValue(group, ROW_GROUP_NUM_ROWS));
            if (chunks.size() != columnCount) {
                throw new IOException("expected " + columnCount + " columns in " + path + " but found " + chunks.size());
            }
            if (groupRows < 0 || groupRows > totalRows - filled) {
                throw new IOException("row group rows exceed the footer's row count in " + path);
            }
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
        if (filled != totalRows) {
            throw new IOException("row groups hold " + filled + " rows but the footer says " + totalRows);
        }
        return new Contents(keyHashes, longValues, intValues, varCharValues);
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
