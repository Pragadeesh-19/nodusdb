package io.nodusdb.lake.parquet;

import io.nodusdb.lake.buffer.DeltaMemTable;
import io.nodusdb.lake.buffer.RowSelection;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.lake.model.LakeSchema;
import io.nodusdb.lake.parquet.ColumnChunkEncoder.EncodedChunk;
import io.nodusdb.lake.parquet.ColumnChunkEncoder.Page;
import io.nodusdb.lake.parquet.ColumnChunkEncoder.Statistics;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class ParquetWriter {

    static final byte[] MAGIC = {'P', 'A', 'R', '1'};
    static final int ROW_GROUP_ROWS = 122_880;

    private static final int TYPE_STRUCT = ThriftCompactWriter.TYPE_STRUCT;
    private static final int TYPE_I32 = ThriftCompactWriter.TYPE_I32;
    private static final int TYPE_BINARY = ThriftCompactWriter.TYPE_BINARY;
    private static final int REPETITION_REQUIRED = 0;
    private static final int CONVERTED_TYPE_UTF8 = 0;
    private static final int CONVERTED_TYPE_TIMESTAMP_MICROS = 10;
    private static final int FIELD_SCHEMA_TYPE = 1;
    private static final int FIELD_REPETITION = 3;
    private static final int FIELD_NAME = 4;
    private static final int FIELD_NUM_CHILDREN = 5;
    private static final int FIELD_CONVERTED_TYPE = 6;
    private static final int FIELD_FIELD_ID = 9;
    private static final int FIELD_LOGICAL_TYPE = 10;
    private static final int LOGICAL_TIMESTAMP = 8;
    private static final int TIMESTAMP_ADJUSTED_TO_UTC = 1;
    private static final int TIMESTAMP_UNIT = 2;
    private static final int TIME_UNIT_MICROS = 2;
    private static final int COLUMN_STATISTICS = 12;
    private static final int COLUMN_ORDER_TYPE_DEFINED = 1;
    private static final int COLUMN_DICTIONARY_PAGE_OFFSET = 11;
    private static final String CREATED_BY = "nodusdb lake";
    private static final int COPY_BYTES = 1 << 20;

    private record ChunkMeta(ColumnSpec column, EncodedChunk chunk, long fileOffset, long dataPageOffset,
                             long dictionaryPageOffset) {
    }

    private record RowGroupMeta(List<ChunkMeta> chunks, int rows, long totalBytes) {
    }

    private ParquetWriter() {
    }

    public static void write(Path path, LakeSchema schema, DeltaMemTable table, RowSelection rows, ParquetCodec codec)
            throws IOException {
        try (TableColumnSource source = new TableColumnSource(schema, table, rows)) {
            write(path, source, codec);
        }
    }

    public static WrittenFile write(Path path, ColumnSource source, ParquetCodec codec) throws IOException {
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new UnsupportedOperationException("the parquet writer requires a little-endian platform");
        }
        List<ColumnSpec> columns = source.columns();
        List<RowGroupMeta> groups = new ArrayList<>();
        long fileBytes;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             ColumnChunkEncoder encoder = new ColumnChunkEncoder(codec)) {
            byte[] copyBuffer = new byte[COPY_BYTES];
            drain(channel, ByteBuffer.wrap(MAGIC));
            int rowCount = source.rowCount();
            int start = 0;
            do {
                int end = Math.min(rowCount, start + ROW_GROUP_ROWS);
                List<ChunkMeta> chunks = new ArrayList<>(columns.size());
                long totalBytes = 0;
                for (int index = 0; index < columns.size(); index++) {
                    ColumnSpec column = columns.get(index);
                    EncodedChunk chunk = encodeColumn(encoder, source, index, column, start, end);
                    long fileOffset = channel.position();
                    for (Page page : chunk.pages()) {
                        drain(channel, ByteBuffer.wrap(page.header()));
                        drain(channel, page.body(), copyBuffer);
                    }
                    long dictionaryOffset = chunk.dictionaryEncoded() ? fileOffset + chunk.dictionaryPageOffset() : -1;
                    chunks.add(new ChunkMeta(column, chunk, fileOffset, fileOffset + chunk.dataPageOffset(),
                            dictionaryOffset));
                    totalBytes += chunk.uncompressedBytes();
                }
                groups.add(new RowGroupMeta(chunks, end - start, totalBytes));
                start = end;
            } while (start < rowCount);
            byte[] footer = footer(columns, groups, rowCount, codec);
            drain(channel, ByteBuffer.wrap(footer));
            drain(channel, ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(footer.length).array()));
            drain(channel, ByteBuffer.wrap(MAGIC));
            fileBytes = channel.position();
        }
        return new WrittenFile(source.rowCount(), fileBytes, metrics(columns, groups));
    }

    private static void drain(FileChannel channel, MemorySegment segment, byte[] copyBuffer) throws IOException {
        if (segment.isNative() || segment.heapBase().orElse(null) instanceof byte[]) {
            drain(channel, segment.asByteBuffer());
            return;
        }
        long position = 0;
        while (position < segment.byteSize()) {
            int length = (int) Math.min(copyBuffer.length, segment.byteSize() - position);
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, position, copyBuffer, 0, length);
            drain(channel, ByteBuffer.wrap(copyBuffer, 0, length));
            position += length;
        }
    }

    private static void drain(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static EncodedChunk encodeColumn(ColumnChunkEncoder encoder, ColumnSource source, int index,
                                             ColumnSpec column, int from, int to) {
        int count = to - from;
        if (column.type().variableWidth()) {
            ColumnSource.Strings strings = source.strings(index, from, to);
            return encoder.encodeStrings(strings.data(), strings.offsets(), count);
        }
        MemorySegment values = source.fixed(index, from, to);
        return column.unique() ? encoder.encodeUniqueFixed(values, count, column.type().physical)
                : encoder.encodeFixed(values, count, column.type().physical);
    }

    private static List<ColumnMetrics> metrics(List<ColumnSpec> columns, List<RowGroupMeta> groups) {
        List<ColumnMetrics> metrics = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            ColumnSpec column = columns.get(index);
            ColumnBounds bounds = new ColumnBounds(column.type());
            long values = 0;
            long bytes = 0;
            for (RowGroupMeta group : groups) {
                EncodedChunk chunk = group.chunks().get(index).chunk();
                bounds.add(chunk.statistics());
                values += group.rows();
                bytes += chunk.compressedBytes();
            }
            metrics.add(new ColumnMetrics(column.fieldId(), column.name(), values, 0, bytes, bounds.lower(),
                    bounds.upper()));
        }
        return metrics;
    }

    private static byte[] footer(List<ColumnSpec> columns, List<RowGroupMeta> groups, long rowCount,
                                 ParquetCodec codec) {
        ThriftCompactWriter footer = new ThriftCompactWriter();
        footer.beginStruct();
        footer.i32Field(1, 1);
        footer.listField(2, TYPE_STRUCT, columns.size() + 1);
        footer.beginStruct();
        footer.binaryField(FIELD_NAME, bytes("schema"));
        footer.i32Field(FIELD_NUM_CHILDREN, columns.size());
        footer.endStruct();
        for (ColumnSpec column : columns) {
            writeSchemaElement(footer, column);
        }
        footer.i64Field(3, rowCount);
        footer.listField(4, TYPE_STRUCT, groups.size());
        for (RowGroupMeta group : groups) {
            footer.beginStruct();
            footer.listField(1, TYPE_STRUCT, group.chunks().size());
            for (ChunkMeta chunk : group.chunks()) {
                writeChunk(footer, chunk, group.rows(), codec);
            }
            footer.i64Field(2, group.totalBytes());
            footer.i64Field(3, group.rows());
            footer.endStruct();
        }
        footer.binaryField(6, bytes(CREATED_BY));
        footer.listField(7, TYPE_STRUCT, columns.size());
        for (int i = 0; i < columns.size(); i++) {
            footer.beginStruct();
            footer.structField(COLUMN_ORDER_TYPE_DEFINED);
            footer.endStruct();
            footer.endStruct();
        }
        footer.endStruct();
        return footer.toByteArray();
    }

    private static void writeSchemaElement(ThriftCompactWriter footer, ColumnSpec column) {
        footer.beginStruct();
        footer.i32Field(FIELD_SCHEMA_TYPE, column.type().physical.code);
        footer.i32Field(FIELD_REPETITION, REPETITION_REQUIRED);
        footer.binaryField(FIELD_NAME, bytes(column.name()));
        if (column.type() == ColumnType.STRING) {
            footer.i32Field(FIELD_CONVERTED_TYPE, CONVERTED_TYPE_UTF8);
        } else if (column.type() == ColumnType.TIMESTAMP_MICROS) {
            footer.i32Field(FIELD_CONVERTED_TYPE, CONVERTED_TYPE_TIMESTAMP_MICROS);
        }
        if (column.hasFieldId()) {
            footer.i32Field(FIELD_FIELD_ID, column.fieldId());
        }
        if (column.type() == ColumnType.TIMESTAMP_MICROS) {
            writeTimestampLogicalType(footer);
        }
        footer.endStruct();
    }

    private static void writeTimestampLogicalType(ThriftCompactWriter footer) {
        footer.structField(FIELD_LOGICAL_TYPE);
        footer.structField(LOGICAL_TIMESTAMP);
        footer.boolField(TIMESTAMP_ADJUSTED_TO_UTC, true);
        footer.structField(TIMESTAMP_UNIT);
        footer.structField(TIME_UNIT_MICROS);
        footer.endStruct();
        footer.endStruct();
        footer.endStruct();
        footer.endStruct();
    }

    private static void writeChunk(ThriftCompactWriter footer, ChunkMeta meta, int rows, ParquetCodec codec) {
        EncodedChunk chunk = meta.chunk();
        footer.beginStruct();
        footer.i64Field(2, meta.fileOffset());
        footer.structField(3);
        footer.i32Field(1, meta.column().type().physical.code);
        int[] encodings = chunk.dictionaryEncoded()
                ? new int[] {ColumnChunkEncoder.ENCODING_PLAIN, ColumnChunkEncoder.ENCODING_RLE,
                        ColumnChunkEncoder.ENCODING_RLE_DICTIONARY}
                : new int[] {ColumnChunkEncoder.ENCODING_PLAIN, ColumnChunkEncoder.ENCODING_RLE};
        footer.listField(2, TYPE_I32, encodings.length);
        for (int encoding : encodings) {
            footer.i32Element(encoding);
        }
        footer.listField(3, TYPE_BINARY, 1);
        footer.binaryElement(bytes(meta.column().name()));
        footer.i32Field(4, codec.thriftId);
        footer.i64Field(5, rows);
        footer.i64Field(6, chunk.uncompressedBytes());
        footer.i64Field(7, chunk.compressedBytes());
        footer.i64Field(9, meta.dataPageOffset());
        if (chunk.dictionaryEncoded()) {
            footer.i64Field(COLUMN_DICTIONARY_PAGE_OFFSET, meta.dictionaryPageOffset());
        }
        Statistics statistics = chunk.statistics();
        if (statistics.present()) {
            statistics.writeTo(footer, COLUMN_STATISTICS);
        }
        footer.endStruct();
        footer.endStruct();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
