package io.nodusdb.lake;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

final class ParquetReader {

    private static final int CODEC_GZIP = 2;
    private static final int FOOTER_ROW_GROUPS = 4;
    private static final int FOOTER_NUM_ROWS = 3;
    private static final int ROW_GROUP_COLUMNS = 1;
    private static final int CHUNK_META_DATA = 3;
    private static final int META_CODEC = 4;
    private static final int META_DATA_PAGE_OFFSET = 9;
    private static final int PAGE_COMPRESSED_SIZE = 3;
    private static final int PAGE_DATA_HEADER = 5;
    private static final int DATA_HEADER_NUM_VALUES = 1;

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
        List<?> rowGroups = (List<?>) metadata.get(FOOTER_ROW_GROUPS);
        Map<?, ?> rowGroup = (Map<?, ?>) rowGroups.get(0);
        List<?> chunks = (List<?>) rowGroup.get(ROW_GROUP_COLUMNS);
        int rowCount = (int) longValue(metadata, FOOTER_NUM_ROWS);
        int columnCount = schema.fields().size() + 1;
        if (chunks.size() != columnCount) {
            throw new IOException("expected " + columnCount + " columns in " + path + " but found " + chunks.size());
        }

        long[] keyHashes = decodeLongs(file, chunks.get(0), rowCount);
        int[] slots = schema.slots();
        long[][] longValues = new long[schema.memtableSchema().longColumns()][];
        int[][] intValues = new int[schema.memtableSchema().intColumns()][];
        byte[][][] varCharValues = new byte[schema.memtableSchema().varCharColumns()][][];
        for (int i = 0; i < schema.fields().size(); i++) {
            Object chunk = chunks.get(i + 1);
            switch (schema.fields().get(i).type()) {
                case INT64, DOUBLE -> longValues[slots[i]] = decodeLongs(file, chunk, rowCount);
                case INT32 -> intValues[slots[i]] = decodeInts(file, chunk, rowCount);
                case UTF8 -> varCharValues[slots[i]] = decodeVarChars(file, chunk, rowCount);
            }
        }
        return new Contents(keyHashes, longValues, intValues, varCharValues);
    }

    private static void requireMagic(byte[] file) throws IOException {
        if (file.length < 12 || !Arrays.equals(Arrays.copyOf(file, 4), ParquetWriter.MAGIC)
                || !Arrays.equals(Arrays.copyOfRange(file, file.length - 4, file.length), ParquetWriter.MAGIC)) {
            throw new IOException("not a parquet file");
        }
    }

    private static long[] decodeLongs(byte[] file, Object chunk, int rowCount) throws IOException {
        ByteBuffer payload = payload(file, chunk, rowCount, Long.BYTES * (long) rowCount);
        long[] values = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = payload.getLong();
        }
        return values;
    }

    private static int[] decodeInts(byte[] file, Object chunk, int rowCount) throws IOException {
        ByteBuffer payload = payload(file, chunk, rowCount, Integer.BYTES * (long) rowCount);
        int[] values = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = payload.getInt();
        }
        return values;
    }

    private static byte[][] decodeVarChars(byte[] file, Object chunk, int rowCount) throws IOException {
        ByteBuffer payload = payload(file, chunk, rowCount, -1);
        byte[][] values = new byte[rowCount][];
        for (int i = 0; i < rowCount; i++) {
            int length = payload.getInt();
            if (length < 0 || length > payload.remaining()) {
                throw new IOException("corrupt byte array length " + length);
            }
            values[i] = new byte[length];
            payload.get(values[i]);
        }
        return values;
    }

    private static ByteBuffer payload(byte[] file, Object chunk, int rowCount, long expectedSize) throws IOException {
        Map<?, ?> columnMetadata = (Map<?, ?>) ((Map<?, ?>) chunk).get(CHUNK_META_DATA);
        int pageOffset = (int) longValue(columnMetadata, META_DATA_PAGE_OFFSET);
        int codec = (int) longValue(columnMetadata, META_CODEC);
        ThriftCompactReader reader = new ThriftCompactReader(file, pageOffset);
        Map<Integer, Object> pageHeader = reader.readStruct();
        int compressedSize = (int) longValue(pageHeader, PAGE_COMPRESSED_SIZE);
        Map<Integer, Object> dataHeader = castMap(pageHeader.get(PAGE_DATA_HEADER));
        if (longValue(dataHeader, DATA_HEADER_NUM_VALUES) != rowCount) {
            throw new IOException("page row count does not match footer");
        }
        int start = reader.position();
        if (start + compressedSize > file.length) {
            throw new IOException("column page extends past end of file");
        }
        byte[] raw = Arrays.copyOfRange(file, start, start + compressedSize);
        byte[] bytes = codec == CODEC_GZIP ? gunzip(raw) : raw;
        if (expectedSize >= 0 && bytes.length != expectedSize) {
            throw new IOException("page payload is " + bytes.length + " bytes, expected " + expectedSize);
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static byte[] gunzip(byte[] compressed) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            return gzip.readAllBytes();
        }
    }

    private static long longValue(Map<?, ?> map, int id) {
        Object value = map.get(id);
        if (!(value instanceof Long number)) {
            throw new IllegalStateException("missing numeric thrift field " + id);
        }
        return number;
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, Object> castMap(Object value) {
        return (Map<Integer, Object>) value;
    }
}
