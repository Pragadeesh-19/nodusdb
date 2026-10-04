package io.nodusdb.kernel.wal;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WalFormatTest {

    @Test
    void frameIsTwentyFourBytesWithChecksumOverOpAndEndpoints() {
        ByteBuffer frame = ByteBuffer.allocate(WalFormat.FRAME_BYTES).order(ByteOrder.BIG_ENDIAN);
        long u = 0x0102030405060708L;
        long v = 0x7FEDCBA987654321L;

        WalFormat.putFrame(frame, new CRC32(), WalFormat.OP_REMOVE_EDGE, u, v);

        assertEquals(WalFormat.FRAME_BYTES, frame.position());
        assertEquals(WalFormat.OP_REMOVE_EDGE, frame.get(0));
        assertEquals(0, frame.get(1));
        assertEquals(0, frame.getShort(2));
        assertEquals(expectedChecksum(WalFormat.OP_REMOVE_EDGE, u, v), frame.getInt(4));
        assertEquals(u, frame.getLong(8));
        assertEquals(v, frame.getLong(16));
    }

    @Test
    void checksumChangesWhenAnyPayloadByteChanges() {
        CRC32 crc = new CRC32();
        int base = WalFormat.checksum(crc, WalFormat.OP_ADD_EDGE, 10L, 20L);

        assertEquals(base, WalFormat.checksum(crc, WalFormat.OP_ADD_EDGE, 10L, 20L));
        assertNotEquals(base, WalFormat.checksum(crc, WalFormat.OP_REMOVE_EDGE, 10L, 20L));
        assertNotEquals(base, WalFormat.checksum(crc, WalFormat.OP_ADD_EDGE, 11L, 20L));
        assertNotEquals(base, WalFormat.checksum(crc, WalFormat.OP_ADD_EDGE, 10L, 21L));
    }

    @Test
    void headerStartsWithAsciiNodu() {
        ByteBuffer header = WalFormat.newHeader(1_700_000_000_000L);

        assertEquals(WalFormat.HEADER_BYTES, header.remaining());
        assertEquals(0x4E, header.get(0));
        assertEquals(0x4F, header.get(1));
        assertEquals(0x44, header.get(2));
        assertEquals(0x55, header.get(3));
        assertEquals(WalFormat.VERSION, header.getShort(4));
        assertEquals(1_700_000_000_000L, header.getLong(8));
    }

    @Test
    void headerWithForeignMagicIsRejected() {
        ByteBuffer header = ByteBuffer.allocate(WalFormat.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(0x12345678).putShort((short) 1).flip();

        assertThrows(IOException.class, () -> WalFormat.checkHeader(header, Path.of("x.wal")));
    }

    private static int expectedChecksum(byte op, long u, long v) {
        CRC32 crc = new CRC32();
        crc.update(op);
        for (long value : new long[] {u, v}) {
            for (int shift = 56; shift >= 0; shift -= 8) {
                crc.update((int) (value >>> shift));
            }
        }
        return (int) crc.getValue();
    }
}
