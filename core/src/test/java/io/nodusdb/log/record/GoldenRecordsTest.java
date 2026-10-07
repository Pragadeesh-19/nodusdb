package io.nodusdb.log.record;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GoldenRecordsTest {

    @Test
    void encodingMatchesTheCheckedInCorpusByteForByte() throws IOException {
        byte[] golden = resource("/golden/v2/records.bin");
        ByteBuffer expected = ByteBuffer.allocate(golden.length);
        expected.put(RecordFixtures.copyOf(RecordFixtures.everyRecordType()));
        expected.put(RecordFixtures.copyOf(RecordFixtures.transaction()));
        expected.put(RecordFixtures.copyOf(RecordFixtures.autocommit(RecordType.TUPLE_ADD, 12, 3, 0, 99)));
        expected.put(RecordFixtures.copyOf(
                RecordFixtures.autocommit(RecordType.TUPLE_REMOVE, 2_147_483_637, 65_535, 65_535, 0)));

        assertEquals(golden.length, expected.position());
        assertArrayEquals(golden, expected.array());
    }

    @Test
    void everyRecordInTheCorpusInspectsAsValid() throws IOException {
        byte[] golden = resource("/golden/v2/records.bin");
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(golden), 0, golden.length);
        int records = 0;

        while (reader.hasRecord()) {
            assertEquals(Verdict.VALID, reader.inspect(), "record " + records + " at " + reader.position());
            reader.advance();
            records++;
        }

        assertEquals(golden.length, reader.position());
        assertEquals(true, records > 10);
    }

    @Test
    void segmentHeaderEncodingMatchesTheCheckedInCorpus() throws IOException {
        byte[] golden = resource("/golden/v2/segment-header.bin");
        ByteBuffer header = ByteBuffer.allocate(SegmentHeader.BYTES);

        SegmentHeader.write(header, 0, 1_700_000_000_000_000L, 1_000L);

        assertArrayEquals(golden, header.array());
        assertEquals(Verdict.VALID, SegmentHeader.inspect(ByteBuffer.wrap(golden), 0, golden.length));
        assertEquals(1_000L, SegmentHeader.baseLsn(ByteBuffer.wrap(golden), 0));
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = GoldenRecordsTest.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("missing test resource " + name);
            }
            return in.readAllBytes();
        }
    }
}
