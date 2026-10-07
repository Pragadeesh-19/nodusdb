package io.nodusdb.lake.parquet;

import io.nodusdb.lake.codec.NativeSink;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.lake.codec.SnappyCompressor;
import io.nodusdb.lake.memory.NativeColumn;
import io.nodusdb.lake.memory.NativeKeyIndex;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

public final class ColumnChunkEncoder implements AutoCloseable {

    static final int MAX_DICTIONARY_VALUES = 1 << 16;

    static final int ENCODING_PLAIN = 0;
    static final int ENCODING_RLE = 3;
    static final int ENCODING_RLE_DICTIONARY = 8;

    private static final int PAGE_TYPE_DATA = 0;
    private static final int PAGE_TYPE_DICTIONARY = 2;
    private static final int LONG_DICTIONARY_SLOTS = 1 << 11;
    private static final int STRING_DICTIONARY_MIN_SLOTS = 16;
    private static final int DATA_HEADER_ID = 5;
    private static final int DICTIONARY_HEADER_ID = 7;
    private static final int STATISTICS_DEPRECATED_MAX = 1;
    private static final int STATISTICS_DEPRECATED_MIN = 2;
    private static final int STATISTICS_NULL_COUNT = 3;
    private static final int STATISTICS_MAX = 5;
    private static final int STATISTICS_MIN = 6;
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

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

    record Page(byte[] header, MemorySegment body) {

        long size() {
            return header.length + body.byteSize();
        }
    }

    record EncodedChunk(List<Page> pages, int dictionaryPageOffset, int dataPageOffset, int uncompressedBytes,
                        Statistics statistics) {

        boolean dictionaryEncoded() {
            return dictionaryPageOffset >= 0;
        }

        long compressedBytes() {
            long total = 0;
            for (Page page : pages) {
                total += page.size();
            }
            return total;
        }
    }

    private final ParquetCodec codec;
    private final SnappyCompressor snappy = new SnappyCompressor();
    private final NativeSink dictionaryPayload = new NativeSink();
    private final NativeSink dataPayload = new NativeSink();
    private final NativeSink dictionaryBody = new NativeSink();
    private final NativeSink dataBody = new NativeSink();
    private final NativeColumn indices = new NativeColumn(NativeColumn.ALIGNMENT);
    private final LongDictionary longDictionary = new LongDictionary();
    private final StringDictionary stringDictionary = new StringDictionary();

    ColumnChunkEncoder(ParquetCodec codec) {
        this.codec = codec;
    }

    EncodedChunk encodeFixed(MemorySegment values, int count, Physical physical) {
        resetScratch();
        Statistics statistics = fixedStatistics(values, count, physical);
        ensureIndices(count);
        if (longDictionary.build(values, count, MAX_DICTIONARY_VALUES, indices)) {
            int distinct = longDictionary.count();
            if (dictionaryPays(distinct, (long) distinct * physical.width, count, (long) count * physical.width)) {
                MemorySegment dictionary = plainFixed(longDictionary.distinct(), distinct, physical,
                        dictionaryPayload);
                DictionaryIndexCodec.encode(indices.segment(), count, distinct, dataPayload);
                return assemble(dictionary, distinct, dataPayload.segment(), count,
                        ENCODING_RLE_DICTIONARY, statistics);
            }
        }
        return assemble(null, 0, plainFixed(values, count, physical, dataPayload), count, ENCODING_PLAIN,
                statistics);
    }

    EncodedChunk encodeUniqueFixed(MemorySegment values, int count, Physical physical) {
        resetScratch();
        Statistics statistics = fixedStatistics(values, count, physical);
        return assemble(null, 0, plainFixed(values, count, physical, dataPayload), count, ENCODING_PLAIN,
                statistics);
    }

    EncodedChunk encodeStrings(MemorySegment data, MemorySegment offsets, int count) {
        resetScratch();
        Statistics statistics = stringStatistics(data, offsets, count);
        ensureIndices(count);
        long plainBytes = offsets.getAtIndex(INT, count) + (long) Integer.BYTES * count;
        if (stringDictionary.build(data, offsets, count, MAX_DICTIONARY_VALUES, indices)) {
            int distinct = stringDictionary.count();
            if (dictionaryPays(distinct, stringDictionary.dictionaryBytes(), count, plainBytes)) {
                MemorySegment dictionary = plainStrings(data, offsets, stringDictionary.firstRows(), distinct,
                        dictionaryPayload);
                DictionaryIndexCodec.encode(indices.segment(), count, distinct, dataPayload);
                return assemble(dictionary, distinct, dataPayload.segment(), count,
                        ENCODING_RLE_DICTIONARY, statistics);
            }
        }
        return assemble(null, 0, plainStrings(data, offsets, null, count, dataPayload), count, ENCODING_PLAIN,
                statistics);
    }

    @Override
    public void close() {
        snappy.close();
        dictionaryPayload.close();
        dataPayload.close();
        dictionaryBody.close();
        dataBody.close();
        indices.close();
        longDictionary.close();
        stringDictionary.close();
    }

    private void resetScratch() {
        dictionaryPayload.reset();
        dataPayload.reset();
        dictionaryBody.reset();
        dataBody.reset();
    }

    private void ensureIndices(int count) {
        indices.ensureCapacity((long) count * Integer.BYTES);
    }

    private static boolean dictionaryPays(int distinct, long dictionaryBytes, int count, long plainBytes) {
        long indexBytes = ((long) count * DictionaryIndexCodec.bitWidthFor(distinct) + 7) / 8 + 2;
        return distinct > 0 && distinct <= MAX_DICTIONARY_VALUES && dictionaryBytes + indexBytes < plainBytes;
    }

    private static MemorySegment plainFixed(MemorySegment values, int count, Physical physical, NativeSink sink) {
        if (physical != Physical.INT32) {
            return values.asSlice(0, (long) count * Long.BYTES);
        }
        sink.reset();
        for (int i = 0; i < count; i++) {
            sink.putLittleEndian(values.getAtIndex(LONG, i), Integer.BYTES);
        }
        return sink.segment();
    }

    private static MemorySegment plainStrings(MemorySegment data, MemorySegment offsets, MemorySegment firstRows,
                                              int count, NativeSink sink) {
        sink.reset();
        for (int i = 0; i < count; i++) {
            int row = firstRows == null ? i : firstRows.getAtIndex(INT, i);
            int start = offsets.getAtIndex(INT, row);
            int length = offsets.getAtIndex(INT, row + 1) - start;
            sink.putLittleEndian(length, Integer.BYTES);
            sink.write(data, start, length);
        }
        return sink.segment();
    }

    private EncodedChunk assemble(MemorySegment dictionaryPayload, int dictionaryCount, MemorySegment dataPayload,
                                  int valueCount, int dataEncoding, Statistics statistics) {
        List<Page> pages = new ArrayList<>(2);
        int uncompressed = 0;
        int dictionaryOffset = -1;
        int dataOffset = 0;
        if (dictionaryPayload != null) {
            dictionaryOffset = 0;
            Page page = dictionaryPage(dictionaryPayload, dictionaryCount);
            pages.add(page);
            uncompressed += page.header().length + Math.toIntExact(dictionaryPayload.byteSize());
            dataOffset = Math.toIntExact(page.size());
        }
        Page page = dataPage(dataPayload, valueCount, dataEncoding, statistics);
        pages.add(page);
        uncompressed += page.header().length + Math.toIntExact(dataPayload.byteSize());
        return new EncodedChunk(pages, dictionaryOffset, dataOffset, uncompressed, statistics);
    }

    private Page dictionaryPage(MemorySegment payload, int count) {
        MemorySegment body = codec.compress(payload, dictionaryBody, snappy);
        ThriftCompactWriter header = new ThriftCompactWriter();
        header.beginStruct();
        header.i32Field(1, PAGE_TYPE_DICTIONARY);
        header.i32Field(2, (int) payload.byteSize());
        header.i32Field(3, (int) body.byteSize());
        header.structField(DICTIONARY_HEADER_ID);
        header.i32Field(1, count);
        header.i32Field(2, ENCODING_PLAIN);
        header.endStruct();
        header.endStruct();
        return new Page(header.toByteArray(), body);
    }

    private Page dataPage(MemorySegment payload, int valueCount, int encoding, Statistics statistics) {
        MemorySegment body = codec.compress(payload, dataBody, snappy);
        ThriftCompactWriter header = new ThriftCompactWriter();
        header.beginStruct();
        header.i32Field(1, PAGE_TYPE_DATA);
        header.i32Field(2, (int) payload.byteSize());
        header.i32Field(3, (int) body.byteSize());
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
        return new Page(header.toByteArray(), body);
    }

    private static Statistics fixedStatistics(MemorySegment values, int count, Physical physical) {
        if (count == 0) {
            return Statistics.NONE;
        }
        if (physical == Physical.DOUBLE) {
            return doubleStatistics(values, count);
        }
        long min = values.getAtIndex(LONG, 0);
        long max = min;
        for (int i = 1; i < count; i++) {
            long value = values.getAtIndex(LONG, i);
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return new Statistics(littleEndian(min, physical.width), littleEndian(max, physical.width), true);
    }

    private static Statistics doubleStatistics(MemorySegment bits, int count) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < count; i++) {
            double value = Double.longBitsToDouble(bits.getAtIndex(LONG, i));
            if (Double.isNaN(value) || value == 0.0) {
                return Statistics.NONE;
            }
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return new Statistics(littleEndian(Double.doubleToRawLongBits(min), Long.BYTES),
                littleEndian(Double.doubleToRawLongBits(max), Long.BYTES), true);
    }

    private static Statistics stringStatistics(MemorySegment data, MemorySegment offsets, int count) {
        if (count == 0) {
            return Statistics.NONE;
        }
        int minRow = 0;
        int maxRow = 0;
        for (int row = 1; row < count; row++) {
            if (compareRows(data, offsets, row, minRow) < 0) {
                minRow = row;
            }
            if (compareRows(data, offsets, row, maxRow) > 0) {
                maxRow = row;
            }
        }
        return new Statistics(rowBytes(data, offsets, minRow), rowBytes(data, offsets, maxRow), false);
    }

    private static byte[] rowBytes(MemorySegment data, MemorySegment offsets, int row) {
        int start = offsets.getAtIndex(INT, row);
        int end = offsets.getAtIndex(INT, row + 1);
        return data.asSlice(start, end - start).toArray(BYTE);
    }

    private static int compareRows(MemorySegment data, MemorySegment offsets, int left, int right) {
        int leftStart = offsets.getAtIndex(INT, left);
        int leftEnd = offsets.getAtIndex(INT, left + 1);
        int rightStart = offsets.getAtIndex(INT, right);
        int rightEnd = offsets.getAtIndex(INT, right + 1);
        int shared = Math.min(leftEnd - leftStart, rightEnd - rightStart);
        long difference = MemorySegment.mismatch(data, leftStart, leftStart + shared, data, rightStart,
                rightStart + shared);
        if (difference >= 0) {
            return Byte.compareUnsigned(data.get(BYTE, leftStart + difference), data.get(BYTE, rightStart + difference));
        }
        return Integer.compare(leftEnd - leftStart, rightEnd - rightStart);
    }

    private static byte[] littleEndian(long value, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = (byte) (value >>> (8 * i));
        }
        return bytes;
    }

    private static final class LongDictionary implements AutoCloseable {

        private final NativeKeyIndex index = new NativeKeyIndex(LONG_DICTIONARY_SLOTS);
        private final NativeColumn distinct = new NativeColumn((long) LONG_DICTIONARY_SLOTS * Long.BYTES);
        private int count;

        boolean build(MemorySegment values, int valueCount, int limit, NativeColumn indices) {
            index.clear();
            count = 0;
            for (int i = 0; i < valueCount; i++) {
                long value = values.getAtIndex(LONG, i);
                int id = index.find(distinct.segment(), value);
                if (id == NativeKeyIndex.ABSENT) {
                    if (count == limit) {
                        return false;
                    }
                    distinct.ensureCapacity((count + 1L) * Long.BYTES);
                    distinct.segment().setAtIndex(LONG, count, value);
                    index.insert(distinct.segment(), value, count);
                    id = count++;
                }
                indices.segment().setAtIndex(INT, i, id);
            }
            return true;
        }

        int count() {
            return count;
        }

        MemorySegment distinct() {
            return distinct.segment();
        }

        @Override
        public void close() {
            index.close();
            distinct.close();
        }
    }

    private static final class StringDictionary implements AutoCloseable {

        private final NativeColumn slots = new NativeColumn(STRING_DICTIONARY_MIN_SLOTS * Integer.BYTES);
        private final NativeColumn firstRows = new NativeColumn(Integer.BYTES);
        private int count;
        private long dictionaryBytes;

        boolean build(MemorySegment data, MemorySegment offsets, int rows, int limit, NativeColumn indices) {
            int slotCount = slotCountFor(rows);
            slots.ensureCapacity((long) slotCount * Integer.BYTES);
            slots.segment().asSlice(0, (long) slotCount * Integer.BYTES).fill((byte) 0);
            int mask = slotCount - 1;
            count = 0;
            dictionaryBytes = 0;
            for (int row = 0; row < rows; row++) {
                int start = offsets.getAtIndex(INT, row);
                int end = offsets.getAtIndex(INT, row + 1);
                int slot = hash(data, start, end) & mask;
                int id = -1;
                for (int entry = slots.segment().getAtIndex(INT, slot);
                     entry != 0;
                     entry = slots.segment().getAtIndex(INT, slot)) {
                    if (sameRow(data, offsets, firstRows.segment().getAtIndex(INT, entry - 1), row)) {
                        id = entry - 1;
                        break;
                    }
                    slot = (slot + 1) & mask;
                }
                if (id < 0) {
                    if (count == limit) {
                        return false;
                    }
                    firstRows.ensureCapacity((count + 1L) * Integer.BYTES);
                    firstRows.segment().setAtIndex(INT, count, row);
                    slots.segment().setAtIndex(INT, slot, count + 1);
                    dictionaryBytes += Integer.BYTES + (end - start);
                    id = count++;
                }
                indices.segment().setAtIndex(INT, row, id);
            }
            return true;
        }

        int count() {
            return count;
        }

        MemorySegment firstRows() {
            return firstRows.segment();
        }

        long dictionaryBytes() {
            return dictionaryBytes;
        }

        @Override
        public void close() {
            slots.close();
            firstRows.close();
        }

        private static int slotCountFor(int rows) {
            int slotCount = STRING_DICTIONARY_MIN_SLOTS;
            while (slotCount < 2L * rows) {
                slotCount <<= 1;
            }
            return slotCount;
        }

        private static boolean sameRow(MemorySegment data, MemorySegment offsets, int left, int right) {
            int leftStart = offsets.getAtIndex(INT, left);
            int leftLength = offsets.getAtIndex(INT, left + 1) - leftStart;
            int rightStart = offsets.getAtIndex(INT, right);
            int rightLength = offsets.getAtIndex(INT, right + 1) - rightStart;
            return leftLength == rightLength
                    && MemorySegment.mismatch(data, leftStart, leftStart + leftLength, data, rightStart,
                    rightStart + rightLength) < 0;
        }

        private static int hash(MemorySegment data, int start, int end) {
            int h = 1;
            for (int i = start; i < end; i++) {
                h = 31 * h + data.get(BYTE, i);
            }
            return h;
        }
    }
}
