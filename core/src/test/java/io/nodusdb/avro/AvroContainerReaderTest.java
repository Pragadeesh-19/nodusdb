package io.nodusdb.avro;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroContainerReaderTest {

    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"v\","
            + "\"type\":\"long\"}]}";

    private static byte[] container(int records, Map<String, String> metadata) {
        AvroContainerWriter writer = new AvroContainerWriter(SCHEMA, metadata);
        for (int i = 0; i < records; i++) {
            writer.append(new AvroEncoder().writeLong(i).toByteArray());
        }
        return writer.finish();
    }

    @Test
    void theWritersOutputIsReadBackWithItsMetadataAndRecords() {
        AvroContainerReader.Container read = AvroContainerReader.read(container(3, Map.of("k", "v")));

        assertEquals(SCHEMA, read.metadataText("avro.schema"));
        assertEquals("null", read.metadataText("avro.codec"));
        assertEquals("v", read.metadataText("k"));
        assertNull(read.metadataText("absent"));
        assertEquals(1, read.blocks().size());
        assertEquals(3, read.blocks().get(0).records());
        AvroDecoder records = new AvroDecoder(read.blocks().get(0).data());
        for (int i = 0; i < 3; i++) {
            assertEquals(i, records.readLong());
        }
        assertTrue(records.atEnd());
    }

    @Test
    void severalBlocksAreAllReturnedInOrder() {
        int total = 40_000;

        AvroContainerReader.Container read = AvroContainerReader.read(container(total, Map.of()));

        assertTrue(read.blocks().size() > 1);
        long expected = 0;
        long count = 0;
        for (AvroContainerReader.Block block : read.blocks()) {
            AvroDecoder records = new AvroDecoder(block.data());
            for (long i = 0; i < block.records(); i++) {
                assertEquals(expected++, records.readLong());
            }
            assertTrue(records.atEnd());
            count += block.records();
        }
        assertEquals(total, count);
    }

    @Test
    void anEmptyContainerHasNoBlocks() {
        assertEquals(0, AvroContainerReader.read(container(0, Map.of())).blocks().size());
    }

    @Test
    void aWrongMagicIsRefused() {
        byte[] bytes = container(1, Map.of());
        bytes[0] = 'X';

        assertThrows(AvroFormatException.class, () -> AvroContainerReader.read(bytes));
        assertThrows(AvroFormatException.class, () -> AvroContainerReader.read(new byte[2]));
    }

    @Test
    void aCodecOtherThanNullIsRefused() {
        byte[] bytes = new AvroContainerWriter(SCHEMA, Map.of("avro.codec", "snappy")).finish();

        AvroFormatException refused = assertThrows(AvroFormatException.class, () -> AvroContainerReader.read(bytes));

        assertTrue(refused.getMessage().contains("null codec"));
    }

    @Test
    void aDamagedSyncMarkerBetweenBlocksIsRefused() {
        byte[] bytes = container(3, Map.of());
        bytes[bytes.length - 1] ^= 0x01;

        assertThrows(AvroFormatException.class, () -> AvroContainerReader.read(bytes));
    }

    @Test
    void everyTruncationOfAContainerIsAFormatError() {
        byte[] bytes = container(5, Map.of("a", "b"));
        int valid = 0;

        for (int length = 0; length < bytes.length; length++) {
            byte[] cut = Arrays.copyOf(bytes, length);
            try {
                assertEquals(0, AvroContainerReader.read(cut).blocks().size(), "length " + length);
                valid++;
            } catch (AvroFormatException refused) {
                continue;
            }
        }

        assertEquals(1, valid, "only the bare header is a valid prefix of a one-block container");
    }

    @Test
    void metadataSurvivesNonAsciiValues() {
        AvroContainerReader.Container read = AvroContainerReader.read(container(0, Map.of("name", "é中")));

        assertArrayEquals("é中".getBytes(StandardCharsets.UTF_8), read.metadata().get("name"));
    }
}
