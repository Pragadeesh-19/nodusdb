package io.nodusdb.lake;

import io.nodusdb.kernel.LongIntIndex;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/*
 * One column chunk: an optional dictionary page, then one data page, each compressed with the codec.
 *
 *   chunk := [dictionary page header][dictionary payload]   (only when dictionary-encoded)
 *            [data page header][data payload]
 *   dictionary payload := PLAIN distinct values
 *   data payload       := RLE_DICTIONARY indices, or PLAIN values
 *
 * A column is dictionary-encoded only when the dictionary and its indices are smaller than the
 * plain values, and it has at most MAX_DICTIONARY_VALUES distinct entries.
 */
final class ColumnChunkEncoder {

    static final int MAX_DICTIONARY_VALUES = 1 << 16;

    static final int ENCODING_PLAIN = 0;
    static final int ENCODING_RLE = 3;
    static final int ENCODING_RLE_DICTIONARY = 8;

    private static final int PAGE_TYPE_DATA = 0;
    private static final int PAGE_TYPE_DICTIONARY = 2;
    private static final int LONG_DICTIONARY_CAPACITY = 1 << 10;
    private static final int ABSENT = -1;
    private static final int DATA_HEADER_ID = 5;
    private static final int DICTIONARY_HEADER_ID = 7;
    private static final int STATISTICS_DEPRECATED_MAX = 1;
    private static final int STATISTICS_DEPRECATED_MIN = 2;
    private static final int STATISTICS_NULL_COUNT = 3;
    private static final int STATISTICS_MAX = 5;
    private static final int STATISTICS_MIN = 6;

    enum Physical {
        INT32(1, Integer.BYTES), INT64(2, Long.BYTES), DOUBLE(5, Long.BYTES), BYTE_ARRAY(6, 0);

        final int code;
        final int width;

        Physical(int code, int width) {
            this.code = code;
            this.width = width;
        }
    }

    record Statistics(byte[] min, byte[] max, boolean signedOrder) {

        static final Statistics NONE = new Statistics(null, null, false);

        boolean present() {
            return min != null;
        }

        void writeTo(ThriftCompactWriter writer, int fieldId) {
            writer.structField(fieldId);
            if (signedOrder) {
                writer.binaryField(STATISTICS_DEPRECATED_MAX, max);
                writer.binaryField(STATISTICS_DEPRECATED_MIN, min);
            }
            writer.i64Field(STATISTICS_NULL_COUNT, 0L);
            writer.binaryField(STATISTICS_MAX, max);
            writer.binaryField(STATISTICS_MIN, min);
            writer.endStruct();
        }
    }

    record EncodedChunk(byte[] bytes, int dictionaryPageOffset, int dataPageOffset, int uncompressedBytes,
                        Statistics statistics) {

        boolean dictionaryEncoded() {
            return dictionaryPageOffset >= 0;
        }
    }

    private final ParquetCodec codec;

    ColumnChunkEncoder(ParquetCodec codec) {
        this.codec = codec;
    }

    EncodedChunk encodeFixed(long[] values, Physical physical) {
        int count = values.length;
        Statistics statistics = fixedStatistics(values, physical);
        LongDictionary dictionary = LongDictionary.build(values, MAX_DICTIONARY_VALUES);
        if (dictionary != null && dictionaryPays(dictionary.distinct().length,
                (long) dictionary.distinct().length * physical.width, count, (long) count * physical.width)) {
            byte[] dictionaryPayload = plainFixed(dictionary.distinct(), dictionary.distinct().length, physical);
            byte[] dataPayload = DictionaryIndexCodec.encode(dictionary.indices(), dictionary.distinct().length);
            return assemble(dictionaryPayload, dictionary.distinct().length, dataPayload, count,
                    ENCODING_RLE_DICTIONARY, statistics);
        }
        return assemble(null, 0, plainFixed(values, count, physical), count, ENCODING_PLAIN, statistics);
    }

    EncodedChunk encodeUniqueFixed(long[] values, Physical physical) {
        int count = values.length;
        return assemble(null, 0, plainFixed(values, count, physical), count, ENCODING_PLAIN,
                fixedStatistics(values, physical));
    }

    EncodedChunk encodeStrings(byte[] data, int[] offsets) {
        int count = offsets.length - 1;
        Statistics statistics = stringStatistics(data, offsets, count);
        StringDictionary dictionary = StringDictionary.build(data, offsets, count, MAX_DICTIONARY_VALUES);
        long plainBytes = (long) offsets[count] + (long) Integer.BYTES * count;
        if (dictionary != null && dictionaryPays(dictionary.distinctCount(), dictionary.dictionaryBytes(),
                count, plainBytes)) {
            byte[] dictionaryPayload = plainStrings(data, offsets, dictionary.firstRows(), dictionary.distinctCount());
            byte[] dataPayload = DictionaryIndexCodec.encode(dictionary.indices(), dictionary.distinctCount());
            return assemble(dictionaryPayload, dictionary.distinctCount(), dataPayload, count,
                    ENCODING_RLE_DICTIONARY, statistics);
        }
        return assemble(null, 0, plainStrings(data, offsets, null, count), count, ENCODING_PLAIN, statistics);
    }

    private static boolean dictionaryPays(int distinct, long dictionaryBytes, int count, long plainBytes) {
        long indexBytes = ((long) count * DictionaryIndexCodec.bitWidthFor(distinct) + 7) / 8 + 2;
        return distinct > 0 && distinct <= MAX_DICTIONARY_VALUES && dictionaryBytes + indexBytes < plainBytes;
    }

    private EncodedChunk assemble(byte[] dictionaryPayload, int dictionaryCount, byte[] dataPayload,
                                  int valueCount, int dataEncoding, Statistics statistics) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int uncompressed = 0;
        int dictionaryOffset = -1;
        if (dictionaryPayload != null) {
            dictionaryOffset = 0;
            uncompressed += writeDictionaryPage(out, dictionaryPayload, dictionaryCount);
        }
        int dataOffset = out.size();
        uncompressed += writeDataPage(out, dataPayload, valueCount, dataEncoding, statistics);
        return new EncodedChunk(out.toByteArray(), dictionaryOffset, dataOffset, uncompressed, statistics);
    }

    private int writeDictionaryPage(ByteArrayOutputStream out, byte[] payload, int count) {
        byte[] compressed = codec.compress(payload);
        ThriftCompactWriter header = new ThriftCompactWriter();
        header.beginStruct();
        header.i32Field(1, PAGE_TYPE_DICTIONARY);
        header.i32Field(2, payload.length);
        header.i32Field(3, compressed.length);
        header.structField(DICTIONARY_HEADER_ID);
        header.i32Field(1, count);
        header.i32Field(2, ENCODING_PLAIN);
        header.endStruct();
        header.endStruct();
        return writePage(out, header.toByteArray(), compressed, payload.length);
    }

    private int writeDataPage(ByteArrayOutputStream out, byte[] payload, int valueCount, int encoding,
                              Statistics statistics) {
        byte[] compressed = codec.compress(payload);
        ThriftCompactWriter header = new ThriftCompactWriter();
        header.beginStruct();
        header.i32Field(1, PAGE_TYPE_DATA);
        header.i32Field(2, payload.length);
        header.i32Field(3, compressed.length);
        header.structField(DATA_HEADER_ID);
        header.i32Field(1, valueCount);
        header.i32Field(2, encoding);
        header.i32Field(3, ENCODING_RLE);
        header.i32Field(4, ENCODING_RLE);
        if (statistics.present()) {
            statistics.writeTo(header, 5);
        }
        header.endStruct();
        header.endStruct();
        return writePage(out, header.toByteArray(), compressed, payload.length);
    }

    private static int writePage(ByteArrayOutputStream out, byte[] header, byte[] compressed, int payloadLength) {
        out.write(header, 0, header.length);
        out.write(compressed, 0, compressed.length);
        return header.length + payloadLength;
    }

    private static byte[] plainFixed(long[] values, int count, Physical physical) {
        ByteBuffer buffer = ByteBuffer.allocate(count * physical.width).order(ByteOrder.LITTLE_ENDIAN);
        if (physical == Physical.INT32) {
            for (int i = 0; i < count; i++) {
                buffer.putInt((int) values[i]);
            }
        } else {
            buffer.asLongBuffer().put(values, 0, count);
        }
        return buffer.array();
    }

    private static byte[] plainStrings(byte[] data, int[] offsets, int[] rows, int count) {
        long size = 0;
        for (int i = 0; i < count; i++) {
            int row = rows == null ? i : rows[i];
            size += Integer.BYTES + (offsets[row + 1] - offsets[row]);
        }
        ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(size)).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            int row = rows == null ? i : rows[i];
            int start = offsets[row];
            int length = offsets[row + 1] - start;
            buffer.putInt(length);
            buffer.put(data, start, length);
        }
        return buffer.array();
    }

    private static Statistics fixedStatistics(long[] values, Physical physical) {
        if (values.length == 0) {
            return Statistics.NONE;
        }
        if (physical == Physical.DOUBLE) {
            return doubleStatistics(values);
        }
        long min = values[0];
        long max = values[0];
        for (long value : values) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return new Statistics(littleEndian(min, physical.width), littleEndian(max, physical.width), true);
    }

    private static Statistics doubleStatistics(long[] bits) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (long raw : bits) {
            double value = Double.longBitsToDouble(raw);
            if (Double.isNaN(value) || value == 0.0) {
                return Statistics.NONE;
            }
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return new Statistics(littleEndian(Double.doubleToRawLongBits(min), Long.BYTES),
                littleEndian(Double.doubleToRawLongBits(max), Long.BYTES), true);
    }

    private static Statistics stringStatistics(byte[] data, int[] offsets, int count) {
        if (count == 0) {
            return Statistics.NONE;
        }
        int minRow = 0;
        int maxRow = 0;
        for (int row = 1; row < count; row++) {
            if (compare(data, offsets, row, minRow) < 0) {
                minRow = row;
            }
            if (compare(data, offsets, row, maxRow) > 0) {
                maxRow = row;
            }
        }
        return new Statistics(Arrays.copyOfRange(data, offsets[minRow], offsets[minRow + 1]),
                Arrays.copyOfRange(data, offsets[maxRow], offsets[maxRow + 1]), false);
    }

    private static int compare(byte[] data, int[] offsets, int left, int right) {
        return Arrays.compareUnsigned(data, offsets[left], offsets[left + 1],
                data, offsets[right], offsets[right + 1]);
    }

    private static byte[] littleEndian(long value, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = (byte) (value >>> (8 * i));
        }
        return bytes;
    }

    private record LongDictionary(long[] distinct, int[] indices) {

        static LongDictionary build(long[] values, int limit) {
            LongIntIndex index = new LongIntIndex(LONG_DICTIONARY_CAPACITY);
            long[] distinct = new long[LONG_DICTIONARY_CAPACITY];
            int[] indices = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                int id = index.get(values[i]);
                if (id == ABSENT) {
                    if (count == limit) {
                        return null;
                    }
                    id = count;
                    index.put(values[i], id);
                    if (count == distinct.length) {
                        distinct = Arrays.copyOf(distinct, distinct.length * 2);
                    }
                    distinct[count++] = values[i];
                }
                indices[i] = id;
            }
            return new LongDictionary(Arrays.copyOf(distinct, count), indices);
        }
    }

    private static final class StringDictionary {

        private final int[] firstRows;
        private final int[] indices;
        private final long dictionaryBytes;

        private StringDictionary(int[] firstRows, int[] indices, long dictionaryBytes) {
            this.firstRows = firstRows;
            this.indices = indices;
            this.dictionaryBytes = dictionaryBytes;
        }

        static StringDictionary build(byte[] data, int[] offsets, int count, int limit) {
            Map<ByteRange, Integer> ids = new HashMap<>();
            int[] firstRows = new int[Math.min(count, limit)];
            int[] indices = new int[count];
            long dictionaryBytes = 0;
            for (int row = 0; row < count; row++) {
                ByteRange range = new ByteRange(data, offsets[row], offsets[row + 1]);
                Integer id = ids.get(range);
                if (id == null) {
                    if (ids.size() == limit) {
                        return null;
                    }
                    id = ids.size();
                    ids.put(range, id);
                    firstRows[id] = row;
                    dictionaryBytes += Integer.BYTES + (offsets[row + 1] - offsets[row]);
                }
                indices[row] = id;
            }
            return new StringDictionary(Arrays.copyOf(firstRows, ids.size()), indices, dictionaryBytes);
        }

        int[] firstRows() {
            return firstRows;
        }

        int[] indices() {
            return indices;
        }

        int distinctCount() {
            return firstRows.length;
        }

        long dictionaryBytes() {
            return dictionaryBytes;
        }
    }

    private static final class ByteRange {

        private final byte[] data;
        private final int start;
        private final int end;
        private final int hash;

        ByteRange(byte[] data, int start, int end) {
            this.data = data;
            this.start = start;
            this.end = end;
            int h = 1;
            for (int i = start; i < end; i++) {
                h = 31 * h + data[i];
            }
            this.hash = h;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ByteRange range
                    && Arrays.equals(data, start, end, range.data, range.start, range.end);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
