package io.nodusdb.lake;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

final class ParquetWriter {

    static final byte[] MAGIC = {'P', 'A', 'R', '1'};

    private static final int PHYSICAL_INT32 = 1;
    private static final int PHYSICAL_INT64 = 2;
    private static final int PHYSICAL_DOUBLE = 5;
    private static final int PHYSICAL_BYTE_ARRAY = 6;
    private static final int ENCODING_PLAIN = 0;
    private static final int ENCODING_RLE = 3;
    private static final int CODEC_GZIP = 2;
    private static final int PAGE_TYPE_DATA = 0;
    private static final int REPETITION_REQUIRED = 0;
    private static final int CONVERTED_TYPE_UTF8 = 0;
    private static final int FIELD_SCHEMA_TYPE = 1;
    private static final int FIELD_REPETITION = 3;
    private static final int FIELD_NAME = 4;
    private static final int FIELD_NUM_CHILDREN = 5;
    private static final int FIELD_CONVERTED_TYPE = 6;
    private static final String CREATED_BY = "nodusdb lake";

    private enum Source {
        KEY, LONG, INT, VAR
    }

    private record Column(String name, int physicalType, boolean utf8, Source source, int slot) {
    }

    private record ChunkInfo(Column column, long offset, int uncompressedSize, int compressedSize) {
    }

    private ParquetWriter() {
    }

    static void write(Path path, LakeSchema schema, DeltaMemTable table, int[] rows) throws IOException {
        List<Column> columns = columns(schema);
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.write(MAGIC, 0, MAGIC.length);
        List<ChunkInfo> chunks = new ArrayList<>(columns.size());
        for (Column column : columns) {
            long offset = file.size();
            byte[] payload = encodePage(column, table, rows);
            byte[] compressed = gzip(payload);
            byte[] header = pageHeader(payload.length, compressed.length, rows.length);
            file.write(header, 0, header.length);
            file.write(compressed, 0, compressed.length);
            chunks.add(new ChunkInfo(column, offset, header.length + payload.length,
                    header.length + compressed.length));
        }
        byte[] footer = footer(chunks, rows.length);
        file.write(footer, 0, footer.length);
        file.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.length).array(), 0, 4);
        file.write(MAGIC, 0, MAGIC.length);
        Files.write(path, file.toByteArray());
    }

    private static List<Column> columns(LakeSchema schema) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column(LakeSchema.KEY_COLUMN, PHYSICAL_INT64, false, Source.KEY, 0));
        int[] slots = schema.slots();
        for (int i = 0; i < schema.fields().size(); i++) {
            LakeSchema.Field field = schema.fields().get(i);
            columns.add(switch (field.type()) {
                case INT64 -> new Column(field.name(), PHYSICAL_INT64, false, Source.LONG, slots[i]);
                case DOUBLE -> new Column(field.name(), PHYSICAL_DOUBLE, false, Source.LONG, slots[i]);
                case INT32 -> new Column(field.name(), PHYSICAL_INT32, false, Source.INT, slots[i]);
                case UTF8 -> new Column(field.name(), PHYSICAL_BYTE_ARRAY, true, Source.VAR, slots[i]);
            });
        }
        return columns;
    }

    private static byte[] encodePage(Column column, DeltaMemTable table, int[] rows) {
        ByteBuffer buffer = ByteBuffer.allocate(payloadSize(column, table, rows)).order(ByteOrder.LITTLE_ENDIAN);
        switch (column.source()) {
            case KEY -> {
                for (int row : rows) {
                    buffer.putLong(table.keyHashAt(row));
                }
            }
            case LONG -> {
                for (int row : rows) {
                    buffer.putLong(table.longAt(column.slot(), row));
                }
            }
            case INT -> {
                for (int row : rows) {
                    buffer.putInt(table.intAt(column.slot(), row));
                }
            }
            case VAR -> {
                for (int row : rows) {
                    int length = table.varCharLength(column.slot(), row);
                    byte[] value = new byte[length];
                    table.copyVarChar(column.slot(), row, value, 0);
                    buffer.putInt(length);
                    buffer.put(value);
                }
            }
        }
        return buffer.array();
    }

    private static int payloadSize(Column column, DeltaMemTable table, int[] rows) {
        return switch (column.source()) {
            case KEY, LONG -> rows.length * Long.BYTES;
            case INT -> rows.length * Integer.BYTES;
            case VAR -> {
                long size = (long) rows.length * Integer.BYTES;
                for (int row : rows) {
                    size += table.varCharLength(column.slot(), row);
                }
                yield Math.toIntExact(size);
            }
        };
    }

    private static byte[] gzip(byte[] payload) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (FastGzipOutputStream gzip = new FastGzipOutputStream(compressed)) {
            gzip.write(payload);
        }
        return compressed.toByteArray();
    }

    private static final class FastGzipOutputStream extends GZIPOutputStream {

        FastGzipOutputStream(OutputStream out) throws IOException {
            super(out);
            def.setLevel(Deflater.BEST_SPEED);
        }
    }

    private static byte[] pageHeader(int uncompressedSize, int compressedSize, int rowCount) {
        ThriftCompactWriter header = new ThriftCompactWriter();
        header.beginStruct();
        header.i32Field(1, PAGE_TYPE_DATA);
        header.i32Field(2, uncompressedSize);
        header.i32Field(3, compressedSize);
        header.structField(5);
        header.i32Field(1, rowCount);
        header.i32Field(2, ENCODING_PLAIN);
        header.i32Field(3, ENCODING_RLE);
        header.i32Field(4, ENCODING_RLE);
        header.endStruct();
        header.endStruct();
        return header.toByteArray();
    }

    private static byte[] footer(List<ChunkInfo> chunks, int rowCount) {
        ThriftCompactWriter footer = new ThriftCompactWriter();
        footer.beginStruct();
        footer.i32Field(1, 1);
        footer.listField(2, ThriftCompactWriter.TYPE_STRUCT, chunks.size() + 1);
        footer.beginStruct();
        footer.binaryField(FIELD_NAME, bytes("schema"));
        footer.i32Field(FIELD_NUM_CHILDREN, chunks.size());
        footer.endStruct();
        for (ChunkInfo chunk : chunks) {
            Column column = chunk.column();
            footer.beginStruct();
            footer.i32Field(FIELD_SCHEMA_TYPE, column.physicalType());
            footer.i32Field(FIELD_REPETITION, REPETITION_REQUIRED);
            footer.binaryField(FIELD_NAME, bytes(column.name()));
            if (column.utf8()) {
                footer.i32Field(FIELD_CONVERTED_TYPE, CONVERTED_TYPE_UTF8);
            }
            footer.endStruct();
        }
        footer.i64Field(3, rowCount);
        footer.listField(4, ThriftCompactWriter.TYPE_STRUCT, 1);
        footer.beginStruct();
        footer.listField(1, ThriftCompactWriter.TYPE_STRUCT, chunks.size());
        long totalUncompressed = 0;
        for (ChunkInfo chunk : chunks) {
            totalUncompressed += chunk.uncompressedSize();
            writeChunk(footer, chunk, rowCount);
        }
        footer.i64Field(2, totalUncompressed);
        footer.i64Field(3, rowCount);
        footer.endStruct();
        footer.binaryField(6, bytes(CREATED_BY));
        footer.endStruct();
        return footer.toByteArray();
    }

    private static void writeChunk(ThriftCompactWriter footer, ChunkInfo chunk, int rowCount) {
        footer.beginStruct();
        footer.i64Field(2, chunk.offset());
        footer.structField(3);
        footer.i32Field(1, chunk.column().physicalType());
        footer.listField(2, ThriftCompactWriter.TYPE_I32, 2);
        footer.i32Element(ENCODING_PLAIN);
        footer.i32Element(ENCODING_RLE);
        footer.listField(3, ThriftCompactWriter.TYPE_BINARY, 1);
        footer.binaryElement(bytes(chunk.column().name()));
        footer.i32Field(4, CODEC_GZIP);
        footer.i64Field(5, rowCount);
        footer.i64Field(6, chunk.uncompressedSize());
        footer.i64Field(7, chunk.compressedSize());
        footer.i64Field(9, chunk.offset());
        footer.endStruct();
        footer.endStruct();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
