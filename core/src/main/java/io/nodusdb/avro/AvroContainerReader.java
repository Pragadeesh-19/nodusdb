package io.nodusdb.avro;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class AvroContainerReader {

    public record Block(long records, byte[] data) {
    }

    public record Container(Map<String, byte[]> metadata, List<Block> blocks) {

        public String metadataText(String key) {
            byte[] value = metadata.get(key);
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        }
    }

    private static final byte[] MAGIC = {'O', 'b', 'j', 1};
    private static final String CODEC_KEY = "avro.codec";
    private static final String NULL_CODEC = "null";

    private AvroContainerReader() {
    }

    public static Container read(byte[] bytes) {
        if (bytes.length < MAGIC.length || !Arrays.equals(bytes, 0, MAGIC.length, MAGIC, 0, MAGIC.length)) {
            throw new AvroFormatException("not an Avro object container file");
        }
        AvroDecoder decoder = new AvroDecoder(bytes, MAGIC.length, bytes.length);
        Map<String, byte[]> metadata = readMetadata(decoder);
        byte[] codec = metadata.get(CODEC_KEY);
        if (codec != null && !NULL_CODEC.equals(new String(codec, StandardCharsets.UTF_8))) {
            throw new AvroFormatException("only the null codec is supported");
        }
        byte[] sync = decoder.readRaw(AvroContainerWriter.SYNC_BYTES);
        List<Block> blocks = new ArrayList<>();
        while (!decoder.atEnd()) {
            long records = decoder.readLong();
            long size = decoder.readLong();
            if (records < 0 || size < 0 || size > Integer.MAX_VALUE) {
                throw new AvroFormatException("a block declares " + records + " records and " + size + " bytes");
            }
            byte[] data = decoder.readRaw((int) size);
            if (!Arrays.equals(decoder.readRaw(AvroContainerWriter.SYNC_BYTES), sync)) {
                throw new AvroFormatException("a block is not followed by the sync marker");
            }
            blocks.add(new Block(records, data));
        }
        return new Container(metadata, blocks);
    }

    private static Map<String, byte[]> readMetadata(AvroDecoder decoder) {
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        long count = decoder.readBlockCount();
        while (count != 0) {
            for (long i = 0; i < count; i++) {
                String key = decoder.readString();
                metadata.put(key, decoder.readBytes());
            }
            count = decoder.readBlockCount();
        }
        return metadata;
    }
}
