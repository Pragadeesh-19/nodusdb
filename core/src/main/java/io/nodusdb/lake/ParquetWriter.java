package io.nodusdb.lake;

import io.nodusdb.lake.ColumnChunkEncoder.EncodedChunk;
import io.nodusdb.lake.ColumnChunkEncoder.Page;
import io.nodusdb.lake.ColumnChunkEncoder.Physical;
import io.nodusdb.lake.ColumnChunkEncoder.Statistics;

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
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

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

    private record Gather(NativeColumn longs, NativeColumn offsets, NativeColumn data) {
    }

    private ParquetWriter() {
    }

    static void write(Path path, LakeSchema schema, DeltaMemTable table, RowSelection rows, ParquetCodec codec)
            throws IOException {
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new UnsupportedOperationException("the parquet writer requires a little-endian platform");
        }
        List<Column> columns = columns(schema);
        List<RowGroupMeta> groups = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             ColumnChunkEncoder encoder = new ColumnChunkEncoder(codec);
             NativeColumn longs = new NativeColumn(NativeColumn.ALIGNMENT);
             NativeColumn offsets = new NativeColumn(NativeColumn.ALIGNMENT);
             NativeColumn data = new NativeColumn(NativeColumn.ALIGNMENT)) {
            Gather gather = new Gather(longs, offsets, data);
            drain(channel, ByteBuffer.wrap(MAGIC));
            int start = 0;
            do {
                int end = Math.min(rows.size(), start + ROW_GROUP_ROWS);
                List<ChunkMeta> chunks = new ArrayList<>(columns.size());
                long totalBytes = 0;
                for (Column column : columns) {
                    EncodedChunk chunk = encodeColumn(encoder, gather, column, table, rows, start, end);
                    long fileOffset = channel.position();
                    for (Page page : chunk.pages()) {
                        drain(channel, ByteBuffer.wrap(page.header()));
                        drain(channel, page.body().asByteBuffer());
                    }
                    long dictionaryOffset = chunk.dictionaryEncoded() ? fileOffset + chunk.dictionaryPageOffset() : -1;
                    chunks.add(new ChunkMeta(column, chunk, fileOffset, fileOffset + chunk.dataPageOffset(),
                            dictionaryOffset));
                    totalBytes += chunk.uncompressedBytes();
                }
                groups.add(new RowGroupMeta(chunks, end - start, totalBytes));
                start = end;
            } while (start < rows.size());
            byte[] footer = footer(columns, groups, rows.size(), codec);
            drain(channel, ByteBuffer.wrap(footer));
            drain(channel, ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(footer.length).array()));
            drain(channel, ByteBuffer.wrap(MAGIC));
        }
    }

    private static void drain(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
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

    private static EncodedChunk encodeColumn(ColumnChunkEncoder encoder, Gather gather, Column column,
                                             DeltaMemTable table, RowSelection rows, int from, int to) {
        int count = to - from;
        return switch (column.source()) {
            case KEY -> encoder.encodeUniqueFixed(gatherLongs(table.keyHashColumn(), rows, from, to, gather.longs()),
                    count, column.physical());
            case LONG -> encoder.encodeFixed(gatherLongs(table.longColumn(column.slot()), rows, from, to,
                    gather.longs()), count, column.physical());
            case INT -> encoder.encodeFixed(gatherInts(table.intColumn(column.slot()), rows, from, to,
                    gather.longs()), count, column.physical());
            case VAR -> encodeStrings(encoder, gather, table, column.slot(), rows, from, count);
        };
    }

    private static EncodedChunk encodeStrings(ColumnChunkEncoder encoder, Gather gather, DeltaMemTable table,
                                              int slot, RowSelection rows, int from, int count) {
        NativeColumn offsets = gather.offsets();
        NativeColumn data = gather.data();
        offsets.ensureCapacity((count + 1L) * Integer.BYTES);
        MemorySegment lengths = table.varCharLengthColumn(slot);
        MemorySegment starts = table.varCharOffsetColumn(slot);
        MemorySegment slab = table.varCharSlab();
        offsets.segment().setAtIndex(INT, 0, 0);
        for (int i = 0; i < count; i++) {
            int row = rows.rowAt(from + i);
            offsets.segment().setAtIndex(INT, i + 1, offsets.segment().getAtIndex(INT, i)
                    + lengths.getAtIndex(INT, row));
        }
        long total = offsets.segment().getAtIndex(INT, count);
        data.ensureCapacity(Math.max(1, total));
        for (int i = 0; i < count; i++) {
            int row = rows.rowAt(from + i);
            MemorySegment.copy(slab, starts.getAtIndex(INT, row), data.segment(),
                    offsets.segment().getAtIndex(INT, i), lengths.getAtIndex(INT, row));
        }
        return encoder.encodeStrings(data.segment().asSlice(0, total),
                offsets.segment().asSlice(0, (count + 1L) * Integer.BYTES), count);
    }

    private static MemorySegment gatherLongs(MemorySegment source, RowSelection rows, int from, int to, NativeColumn target) {
        int count = to - from;
        target.ensureCapacity(count * (long) Long.BYTES);
        MemorySegment out = target.segment();
        for (int i = 0; i < count; i++) {
            out.setAtIndex(LONG, i, source.getAtIndex(LONG, rows.rowAt(from + i)));
        }
        return out.asSlice(0, count * (long) Long.BYTES);
    }

    private static MemorySegment gatherInts(MemorySegment source, RowSelection rows, int from, int to, NativeColumn target) {
        int count = to - from;
        target.ensureCapacity(count * (long) Long.BYTES);
        MemorySegment out = target.segment();
        for (int i = 0; i < count; i++) {
            out.setAtIndex(LONG, i, source.getAtIndex(INT, rows.rowAt(from + i)));
        }
        return out.asSlice(0, count * (long) Long.BYTES);
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
