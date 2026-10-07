package io.nodusdb.avro;

import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonNull;
import io.nodusdb.json.JsonNumber;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonString;
import io.nodusdb.verify.PythonVerifier;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroContainerWriterTest {

    private static final String SCHEMA = """
            {"type":"record","name":"Row","fields":[
              {"name":"id","type":"long"},
              {"name":"name","type":"string"},
              {"name":"flag","type":"boolean"},
              {"name":"maybe","type":["null","int"],"default":null},
              {"name":"data","type":"bytes"},
              {"name":"tags","type":{"type":"array","items":"long"}}]}
            """;

    @TempDir
    Path directory;

    private static byte[] row(long id, String name, boolean flag, Integer maybe, byte[] data, long... tags) {
        AvroEncoder row = new AvroEncoder().writeLong(id).writeString(name).writeBoolean(flag);
        if (maybe == null) {
            row.writeUnionIndex(0);
        } else {
            row.writeUnionIndex(1).writeInt(maybe);
        }
        row.writeBytes(data);
        if (tags.length > 0) {
            row.writeBlockCount(tags.length);
            for (long tag : tags) {
                row.writeLong(tag);
            }
        }
        return row.writeEndOfBlocks().toByteArray();
    }

    private Optional<JsonObject> dump(byte[] container) throws IOException {
        Path file = directory.resolve("container.avro");
        Files.write(file, container);
        return PythonVerifier.run(directory, "avro_dump.py", file.toString());
    }

    @Test
    void aRealAvroReaderDecodesTheRecordsSchemaAndMetadata() throws IOException {
        AvroContainerWriter writer = new AvroContainerWriter(SCHEMA, Map.of("format-version", "2", "x", "y"));
        writer.append(row(1, "one", true, null, new byte[]{1, 2}, 10, 20));
        writer.append(row(-2, "é中", false, 7, new byte[0]));
        writer.append(row(Long.MAX_VALUE, "", true, -1, new byte[]{(byte) 0xFF}, Long.MIN_VALUE));
        Optional<JsonObject> dumped = dump(writer.finish());
        Assumptions.assumeTrue(dumped.isPresent(), "fastavro is not available");

        JsonObject report = dumped.get();

        assertEquals("null", report.requireString("codec"));
        JsonObject metadata = report.requireObject("metadata");
        assertEquals("2", metadata.requireString("format-version"));
        assertEquals("y", metadata.requireString("x"));
        assertEquals("Row", report.requireObject("schema").requireString("name"));
        JsonArray records = report.requireArray("records");
        assertEquals(3, records.size());
        JsonObject first = (JsonObject) records.get(0);
        assertEquals(1, first.requireLong("id"));
        assertEquals("one", first.requireString("name"));
        assertTrue(first.get("maybe") instanceof JsonNull);
        assertEquals("0102", first.requireObject("data").requireString("hex"));
        assertEquals(2, first.requireArray("tags").size());
        JsonObject second = (JsonObject) records.get(1);
        assertEquals(-2, second.requireLong("id"));
        assertEquals("é中", second.requireString("name"));
        assertFalse(second.boolOr("flag", true));
        assertEquals(7, second.requireLong("maybe"));
        assertEquals(0, second.requireArray("tags").size());
        JsonObject third = (JsonObject) records.get(2);
        assertEquals(Long.MAX_VALUE, third.requireLong("id"));
        assertEquals(Long.MIN_VALUE, ((JsonNumber) third.requireArray("tags").get(0)).asLong());
        assertEquals("ff", third.requireObject("data").requireString("hex"));
    }

    @Test
    void manyRecordsSpanSeveralBlocksAndKeepTheirOrder() throws IOException {
        AvroContainerWriter writer = new AvroContainerWriter(SCHEMA, Map.of());
        int rows = 20_000;
        for (int i = 0; i < rows; i++) {
            writer.append(row(i, "row-" + i, i % 2 == 0, i % 3 == 0 ? null : i, new byte[]{(byte) i}, i, i + 1));
        }
        byte[] container = writer.finish();
        Optional<JsonObject> dumped = dump(container);
        Assumptions.assumeTrue(dumped.isPresent(), "fastavro is not available");

        JsonArray records = dumped.get().requireArray("records");

        assertTrue(container.length > 3 * (64 << 10), "the data must need several blocks");
        assertEquals(rows, records.size());
        for (int i = 0; i < rows; i++) {
            JsonObject record = (JsonObject) records.get(i);
            assertEquals(i, record.requireLong("id"));
            assertEquals("row-" + i, record.requireString("name"));
        }
    }

    @Test
    void anEmptyContainerHasOnlyAHeader() throws IOException {
        byte[] container = new AvroContainerWriter(SCHEMA, Map.of("k", "v")).finish();
        Optional<JsonObject> dumped = dump(container);
        Assumptions.assumeTrue(dumped.isPresent(), "fastavro is not available");

        assertEquals(0, dumped.get().requireArray("records").size());
        assertEquals("v", dumped.get().requireObject("metadata").requireString("k"));
        assertEquals('O', container[0]);
        assertEquals('b', container[1]);
        assertEquals('j', container[2]);
        assertEquals(1, container[3]);
    }

    @Test
    void theSameInputAndSyncMarkerGiveTheSameBytes() {
        byte[] sync = new byte[AvroContainerWriter.SYNC_BYTES];
        Arrays.fill(sync, (byte) 7);

        byte[] first = build(sync);
        byte[] second = build(sync);

        assertArrayEquals(first, second);
        assertFalse(Arrays.equals(first, build(new byte[AvroContainerWriter.SYNC_BYTES])));
    }

    @Test
    void theDefaultSyncMarkerIsRandomPerWriter() {
        byte[] first = new AvroContainerWriter(SCHEMA, Map.of()).finish();
        byte[] second = new AvroContainerWriter(SCHEMA, Map.of()).finish();

        assertFalse(Arrays.equals(first, second));
    }

    @Test
    void theSyncMarkerEndsTheHeaderAndEveryBlock() {
        byte[] sync = new byte[AvroContainerWriter.SYNC_BYTES];
        Arrays.fill(sync, (byte) 0x5A);
        AvroContainerWriter writer = new AvroContainerWriter(SCHEMA, Map.of(), sync);
        writer.append(row(1, "a", true, null, new byte[0]));
        byte[] container = writer.finish();

        assertArrayEquals(sync, Arrays.copyOfRange(container, container.length - sync.length, container.length));
        int headerEnd = indexOf(container, sync, 0);
        assertTrue(headerEnd > 4);
        assertEquals(container.length - sync.length, indexOf(container, sync, headerEnd + sync.length));
    }

    @Test
    void aSyncMarkerOfTheWrongSizeIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new AvroContainerWriter(SCHEMA, Map.of(), new byte[15]));
        assertThrows(IllegalArgumentException.class, () -> new AvroContainerWriter(SCHEMA, Map.of(), new byte[17]));
    }

    @Test
    void finishingTwiceDoesNotDuplicateTheLastBlock() throws IOException {
        AvroContainerWriter writer = new AvroContainerWriter(SCHEMA, Map.of());
        writer.append(row(1, "a", true, null, new byte[0]));
        writer.finish();

        Optional<JsonObject> dumped = dump(writer.finish());
        Assumptions.assumeTrue(dumped.isPresent(), "fastavro is not available");

        assertEquals(1, dumped.get().requireArray("records").size());
        assertTrue(dumped.get().requireArray("records").get(0) instanceof JsonObject);
        assertTrue(((JsonObject) dumped.get().requireArray("records").get(0)).get("name") instanceof JsonString);
    }

    private static byte[] build(byte[] sync) {
        AvroContainerWriter writer = new AvroContainerWriter(SCHEMA, Map.of("a", "b"), sync);
        writer.append(row(1, "a", true, null, new byte[]{1}, 5));
        return writer.finish();
    }

    private static int indexOf(byte[] haystack, byte[] needle, int from) {
        for (int i = from; i <= haystack.length - needle.length; i++) {
            if (Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length)) {
                return i;
            }
        }
        return -1;
    }
}
