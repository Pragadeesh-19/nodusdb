package io.nodusdb.avro;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;

public final class AvroContainerWriter {

    public static final int SYNC_BYTES = 16;

    private static final byte[] MAGIC = {'O', 'b', 'j', 1};
    private static final int BLOCK_FLUSH_BYTES = 64 << 10;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final byte[] sync;
    private final AvroEncoder output = new AvroEncoder();
    private final AvroEncoder block = new AvroEncoder();
    private long blockRecords;

    public AvroContainerWriter(String schemaJson, Map<String, String> metadata) {
        this(schemaJson, metadata, randomSync());
    }

    public AvroContainerWriter(String schemaJson, Map<String, String> metadata, byte[] sync) {
        if (sync.length != SYNC_BYTES) {
            throw new IllegalArgumentException("a sync marker holds " + SYNC_BYTES + " bytes");
        }
        this.sync = sync.clone();
        writeHeader(schemaJson, metadata);
    }

    public void append(byte[] encodedRecord) {
        block.writeRaw(encodedRecord);
        blockRecords++;
        if (block.size() >= BLOCK_FLUSH_BYTES) {
            flushBlock();
        }
    }

    public byte[] finish() {
        flushBlock();
        return output.toByteArray();
    }

    private void writeHeader(String schemaJson, Map<String, String> metadata) {
        Map<String, String> entries = new LinkedHashMap<>();
        entries.put("avro.schema", schemaJson);
        entries.put("avro.codec", "null");
        entries.putAll(metadata);
        output.writeRaw(MAGIC);
        output.writeBlockCount(entries.size());
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            output.writeString(entry.getKey());
            output.writeBytes(entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
        output.writeEndOfBlocks();
        output.writeRaw(sync);
    }

    private void flushBlock() {
        if (blockRecords == 0) {
            return;
        }
        output.writeLong(blockRecords);
        output.writeLong(block.size());
        output.writeRaw(block.toByteArray());
        output.writeRaw(sync);
        block.reset();
        blockRecords = 0;
    }

    private static byte[] randomSync() {
        byte[] marker = new byte[SYNC_BYTES];
        RANDOM.nextBytes(marker);
        return marker;
    }
}
