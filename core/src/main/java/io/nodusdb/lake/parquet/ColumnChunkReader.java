package io.nodusdb.lake.parquet;

import io.nodusdb.lake.codec.SnappyCodec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public final class ColumnChunkReader {

    private static final int CODEC_UNCOMPRESSED = 0;
    private static final int CODEC_SNAPPY = 1;
    private static final int CODEC_GZIP = 2;
    private static final int CHUNK_META_DATA = 3;
    private static final int META_CODEC = 4;
    private static final int META_DATA_PAGE_OFFSET = 9;
    private static final int META_DICTIONARY_PAGE_OFFSET = 11;
    private static final int PAGE_TYPE = 1;
    private static final int PAGE_UNCOMPRESSED_SIZE = 2;
    private static final int PAGE_COMPRESSED_SIZE = 3;
    private static final int PAGE_DATA_HEADER = 5;
    private static final int PAGE_DICTIONARY_HEADER = 7;
    private static final int HEADER_NUM_VALUES = 1;
    private static final int HEADER_ENCODING = 2;
    private static final int PAGE_TYPE_DATA = 0;
    private static final int PAGE_TYPE_DICTIONARY = 2;
    private static final int ENCODING_PLAIN_DICTIONARY = 2;
    private static final int ENCODING_RLE_DICTIONARY = 8;

    private ColumnChunkReader() {
    }

    static void readLongs(byte[] file, Object chunk, int rows, long[] destination, int offset) throws IOException {
        Chunk decoded = open(file, chunk, rows);
        if (decoded.dictionaryEncoded()) {
            long[] dictionary = plainLongs(decoded.dictionaryPage());
            int[] indices = DictionaryIndexCodec.decode(decoded.data().payload(), 0,
                    decoded.data().payload().length, rows);
            for (int i = 0; i < rows; i++) {
                destination[offset + i] = dictionary[checkedIndex(indices[i], dictionary.length)];
            }
        } else {
            requireSize(decoded.data().payload(), (long) rows * Long.BYTES);
            ByteBuffer.wrap(decoded.data().payload()).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer()
                    .get(destination, offset, rows);
        }
    }

    static void readInts(byte[] file, Object chunk, int rows, int[] destination, int offset) throws IOException {
        Chunk decoded = open(file, chunk, rows);
        if (decoded.dictionaryEncoded()) {
            int[] dictionary = plainInts(decoded.dictionaryPage());
            int[] indices = DictionaryIndexCodec.decode(decoded.data().payload(), 0,
                    decoded.data().payload().length, rows);
            for (int i = 0; i < rows; i++) {
                destination[offset + i] = dictionary[checkedIndex(indices[i], dictionary.length)];
            }
        } else {
            requireSize(decoded.data().payload(), (long) rows * Integer.BYTES);
            ByteBuffer.wrap(decoded.data().payload()).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
                    .get(destination, offset, rows);
        }
    }

    static void readStrings(byte[] file, Object chunk, int rows, byte[][] destination, int offset)
            throws IOException {
        Chunk decoded = open(file, chunk, rows);
        if (decoded.dictionaryEncoded()) {
            byte[][] dictionary = plainStrings(decoded.dictionaryPage());
            int[] indices = DictionaryIndexCodec.decode(decoded.data().payload(), 0,
                    decoded.data().payload().length, rows);
            for (int i = 0; i < rows; i++) {
                destination[offset + i] = dictionary[checkedIndex(indices[i], dictionary.length)];
            }
        } else {
            byte[][] values = plainStrings(decoded.data().payload(), rows);
            System.arraycopy(values, 0, destination, offset, rows);
        }
    }

    private static Chunk open(byte[] file, Object chunk, int rows) throws IOException {
        Map<?, ?> meta = (Map<?, ?>) ((Map<?, ?>) chunk).get(CHUNK_META_DATA);
        int codec = (int) longValue(meta, META_CODEC);
        Page dictionary = null;
        if (meta.containsKey(META_DICTIONARY_PAGE_OFFSET)) {
            dictionary = readPage(file, longValue(meta, META_DICTIONARY_PAGE_OFFSET), codec);
            if (dictionary.type() != PAGE_TYPE_DICTIONARY) {
                throw new IOException("dictionary offset points at a page of type " + dictionary.type());
            }
        }
        Page data = readPage(file, longValue(meta, META_DATA_PAGE_OFFSET), codec);
        if (data.type() != PAGE_TYPE_DATA) {
            throw new IOException("data offset points at a page of type " + data.type());
        }
        if (data.valueCount() != rows) {
            throw new IOException("page holds " + data.valueCount() + " values, row group has " + rows);
        }
        return new Chunk(dictionary, data);
    }

    private static Page readPage(byte[] file, long offset, int codec) throws IOException {
        if (offset < 0 || offset >= file.length) {
            throw new IOException("page offset " + offset + " is outside the file");
        }
        ThriftCompactReader reader = new ThriftCompactReader(file, (int) offset);
        Map<Integer, Object> header = reader.readStruct();
        int type = (int) longValue(header, PAGE_TYPE);
        int uncompressedSize = (int) longValue(header, PAGE_UNCOMPRESSED_SIZE);
        int compressedSize = (int) longValue(header, PAGE_COMPRESSED_SIZE);
        Map<?, ?> details = type == PAGE_TYPE_DICTIONARY
                ? structField(header, PAGE_DICTIONARY_HEADER)
                : structField(header, PAGE_DATA_HEADER);
        int valueCount = (int) longValue(details, HEADER_NUM_VALUES);
        int encoding = type == PAGE_TYPE_DICTIONARY ? ENCODING_PLAIN_DICTIONARY
                : (int) longValue(details, HEADER_ENCODING);
        int start = reader.position();
        if (compressedSize < 0 || start + compressedSize > file.length) {
            throw new IOException("column page extends past end of file");
        }
        byte[] raw = Arrays.copyOfRange(file, start, start + compressedSize);
        byte[] payload = decompress(codec, raw);
        if (payload.length != uncompressedSize) {
            throw new IOException("page payload is " + payload.length + " bytes, header says " + uncompressedSize);
        }
        return new Page(type, encoding, valueCount, payload);
    }

    private static byte[] decompress(int codec, byte[] raw) throws IOException {
        return switch (codec) {
            case CODEC_UNCOMPRESSED -> raw;
            case CODEC_SNAPPY -> SnappyCodec.decompress(raw);
            case CODEC_GZIP -> gunzip(raw);
            default -> throw new IOException("unsupported parquet codec " + codec);
        };
    }

    private static byte[] gunzip(byte[] compressed) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            return gzip.readAllBytes();
        }
    }

    private static long[] plainLongs(Page dictionary) throws IOException {
        requireDictionary(dictionary);
        requireSize(dictionary.payload(), (long) dictionary.valueCount() * Long.BYTES);
        long[] values = new long[dictionary.valueCount()];
        ByteBuffer.wrap(dictionary.payload()).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(values);
        return values;
    }

    private static int[] plainInts(Page dictionary) throws IOException {
        requireDictionary(dictionary);
        requireSize(dictionary.payload(), (long) dictionary.valueCount() * Integer.BYTES);
        int[] values = new int[dictionary.valueCount()];
        ByteBuffer.wrap(dictionary.payload()).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(values);
        return values;
    }

    private static byte[][] plainStrings(Page dictionary) throws IOException {
        requireDictionary(dictionary);
        return plainStrings(dictionary.payload(), dictionary.valueCount());
    }

    private static byte[][] plainStrings(byte[] payload, int count) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        byte[][] values = new byte[count][];
        for (int i = 0; i < count; i++) {
            if (buffer.remaining() < Integer.BYTES) {
                throw new IOException("byte array page ends inside a length prefix");
            }
            int length = buffer.getInt();
            if (length < 0 || length > buffer.remaining()) {
                throw new IOException("corrupt byte array length " + length);
            }
            values[i] = new byte[length];
            buffer.get(values[i]);
        }
        if (buffer.hasRemaining()) {
            throw new IOException("byte array page has " + buffer.remaining() + " trailing bytes");
        }
        return values;
    }

    private static void requireDictionary(Page dictionary) throws IOException {
        if (dictionary == null) {
            throw new IOException("dictionary-encoded page has no dictionary page");
        }
    }

    private static void requireSize(byte[] payload, long expected) throws IOException {
        if (payload.length != expected) {
            throw new IOException("page payload is " + payload.length + " bytes, expected " + expected);
        }
    }

    private static int checkedIndex(int index, int dictionarySize) throws IOException {
        if (index < 0 || index >= dictionarySize) {
            throw new IOException("dictionary index " + index + " outside dictionary of " + dictionarySize);
        }
        return index;
    }

    private static long longValue(Map<?, ?> map, int id) throws IOException {
        if (!(map.get(id) instanceof Long number)) {
            throw new IOException("missing numeric thrift field " + id);
        }
        return number;
    }

    private static Map<?, ?> structField(Map<?, ?> header, int id) throws IOException {
        if (!(header.get(id) instanceof Map<?, ?> struct)) {
            throw new IOException("missing page header struct " + id);
        }
        return struct;
    }

    private record Page(int type, int encoding, int valueCount, byte[] payload) {
    }

    private record Chunk(Page dictionaryPage, Page data) {

        boolean dictionaryEncoded() {
            return data.encoding() == ENCODING_RLE_DICTIONARY || data.encoding() == ENCODING_PLAIN_DICTIONARY;
        }
    }
}
