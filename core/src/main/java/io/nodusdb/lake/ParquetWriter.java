package io.nodusdb.lake;

import io.nodusdb.lake.ColumnChunkEncoder.EncodedChunk;
import io.nodusdb.lake.ColumnChunkEncoder.Physical;
import io.nodusdb.lake.ColumnChunkEncoder.Statistics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class ParquetWriter {

    static final byte[] MAGIC = {'P', 'A', 'R', '1'};
    static final int ROW_GROUP_ROWS = 122_880;

    private static final int TYPE_STRUCT = ThriftCompactWriter.TYPE_STRUCT;
    private static final int TYPE_I32 = ThriftCompactWriter.TYPE_I32;
    private static final int TYPE_BINARY = ThriftCompactWriter.TYPE_BINARY;
    private static final int REPETITION_REQUIRED = 0;
    private static final int CONVERTED_TYPE_UTF8 = 0;
    private static final int FIELD_SCHEMA_TYPE = 1;
    private static final int FIELD_REPETITION = 3;
    private static final int FIELD_NAME = 4;
    private static final int FIELD_NUM_CHILDREN = 5;
    private static final int FIELD_CONVERTED_TYPE = 6;
    private static final int COLUMN_STATISTICS = 12;
    private static final int COLUMN_ORDER_TYPE_DEFINED = 1;
    private static final int COLUMN_DICTIONARY_PAGE_OFFSET = 11;
    private static final String CREATED_BY = "nodusdb lake";

    private enum Source {
        KEY, LONG, INT, VAR
    }

    private record Column(String name, Physical physical, boolean utf8, Source source, int slot) {
    }

    private record ChunkMeta(Column column, EncodedChunk chunk, long fileOffset, long dataPageOffset,
                             long dictionaryPageOffset) {
    }

    private record RowGroupMeta(List<ChunkMeta> chunks, int rows, long totalBytes) {
    }

    private ParquetWriter() {
    }

    static void write(Path path, LakeSchema schema, DeltaMemTable table, int[] rows, ParquetCodec codec)
            throws IOException {
        List<Column> columns = columns(schema);
        ColumnChunkEncoder encoder = new ColumnChunkEncoder(codec);
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.write(MAGIC, 0, MAGIC.length);
        List<RowGroupMeta> groups = new ArrayList<>();
        int start = 0;
        do {
            int end = Math.min(rows.length, start + ROW_GROUP_ROWS);
            List<ChunkMeta> chunks = new ArrayList<>(columns.size());
            long totalBytes = 0;
            for (Column column : columns) {
                EncodedChunk chunk = encodeColumn(encoder, column, table, rows, start, end);
                long fileOffset = file.size();
                file.write(chunk.bytes(), 0, chunk.bytes().length);
                long dictionaryOffset = chunk.dictionaryEncoded() ? fileOffset + chunk.dictionaryPageOffset() : -1;
                chunks.add(new ChunkMeta(column, chunk, fileOffset, fileOffset + chunk.dataPageOffset(),
                        dictionaryOffset));
                totalBytes += chunk.uncompressedBytes();
            }
            groups.add(new RowGroupMeta(chunks, end - start, totalBytes));
            start = end;
        } while (start < rows.length);
        byte[] footer = footer(columns, groups, rows.length, codec);
        file.write(footer, 0, footer.length);
        file.write(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.length).array(),
                0, Integer.BYTES);
        file.write(MAGIC, 0, MAGIC.length);
        Files.write(path, file.toByteArray());
    }

    private static List<Column> columns(LakeSchema schema) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column(LakeSchema.KEY_COLUMN, Physical.INT64, false, Source.KEY, 0));
        int[] slots = schema.slots();
        for (int i = 0; i < schema.fields().size(); i++) {
            LakeSchema.Field field = schema.fields().get(i);
            columns.add(switch (field.type()) {
                case INT64 -> new Column(field.name(), Physical.INT64, false, Source.LONG, slots[i]);
                case DOUBLE -> new Column(field.name(), Physical.DOUBLE, false, Source.LONG, slots[i]);
                case INT32 -> new Column(field.name(), Physical.INT32, false, Source.INT, slots[i]);
                case UTF8 -> new Column(field.name(), Physical.BYTE_ARRAY, true, Source.VAR, slots[i]);
            });
        }
        return columns;
    }

    private static EncodedChunk encodeColumn(ColumnChunkEncoder encoder, Column column, DeltaMemTable table,
                                             int[] rows, int from, int to) {
        return switch (column.source()) {
            case KEY -> encoder.encodeUniqueFixed(gatherLongs(table.keyHashColumn(), rows, from, to),
                    column.physical());
            case LONG -> encoder.encodeFixed(gatherLongs(table.longColumn(column.slot()), rows, from, to),
                    column.physical());
            case INT -> encoder.encodeFixed(gatherInts(table.intColumn(column.slot()), rows, from, to),
                    column.physical());
            case VAR -> {
                int count = to - from;
                int[] offsets = new int[count + 1];
                int[] lengths = table.varCharLengthColumn(column.slot());
                for (int i = 0; i < count; i++) {
                    offsets[i + 1] = offsets[i] + lengths[rows[from + i]];
                }
                byte[] data = new byte[offsets[count]];
                int[] starts = table.varCharOffsetColumn(column.slot());
                byte[] slab = table.varCharSlab();
                for (int i = 0; i < count; i++) {
                    int row = rows[from + i];
                    System.arraycopy(slab, starts[row], data, offsets[i], lengths[row]);
                }
                yield encoder.encodeStrings(data, offsets);
            }
        };
    }

    private static long[] gatherLongs(long[] source, int[] rows, int from, int to) {
        long[] values = new long[to - from];
        for (int i = 0; i < values.length; i++) {
            values[i] = source[rows[from + i]];
        }
        return values;
    }

    private static long[] gatherInts(int[] source, int[] rows, int from, int to) {
        long[] values = new long[to - from];
        for (int i = 0; i < values.length; i++) {
            values[i] = source[rows[from + i]];
        }
        return values;
    }

    private static byte[] footer(List<Column> columns, List<RowGroupMeta> groups, long rowCount, ParquetCodec codec) {
        ThriftCompactWriter footer = new ThriftCompactWriter();
        footer.beginStruct();
        footer.i32Field(1, 1);
        footer.listField(2, TYPE_STRUCT, columns.size() + 1);
        footer.beginStruct();
        footer.binaryField(FIELD_NAME, bytes("schema"));
        footer.i32Field(FIELD_NUM_CHILDREN, columns.size());
        footer.endStruct();
        for (Column column : columns) {
            footer.beginStruct();
            footer.i32Field(FIELD_SCHEMA_TYPE, column.physical().code);
            footer.i32Field(FIELD_REPETITION, REPETITION_REQUIRED);
            footer.binaryField(FIELD_NAME, bytes(column.name()));
            if (column.utf8()) {
                footer.i32Field(FIELD_CONVERTED_TYPE, CONVERTED_TYPE_UTF8);
            }
            footer.endStruct();
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

    private static void writeChunk(ThriftCompactWriter footer, ChunkMeta meta, int rows, ParquetCodec codec) {
        EncodedChunk chunk = meta.chunk();
        footer.beginStruct();
        footer.i64Field(2, meta.fileOffset());
        footer.structField(3);
        footer.i32Field(1, meta.column().physical().code);
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
        footer.i64Field(7, chunk.bytes().length);
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
