package io.nodusdb.kernel.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

final class WalFormat {

    static final int MAGIC = 0x4E4F4455;
    static final short VERSION = 1;
    static final int HEADER_BYTES = 16;
    static final int FRAME_BYTES = 24;
    static final byte OP_ADD_EDGE = 0x01;
    static final byte OP_REMOVE_EDGE = 0x02;

    private WalFormat() {
    }

    static ByteBuffer newHeader(long createdMillis) {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(MAGIC).putShort(VERSION).putShort((short) 0).putLong(createdMillis);
        header.flip();
        return header;
    }

    static void checkHeader(ByteBuffer header, Path file) throws IOException {
        if (header.remaining() < HEADER_BYTES) {
            throw new IOException("log header is truncated: " + file);
        }
        ByteBuffer view = header.duplicate().order(ByteOrder.BIG_ENDIAN);
        if (view.getInt() != MAGIC) {
            throw new IOException("not a nodus log: " + file);
        }
        short version = view.getShort();
        if (version != VERSION) {
            throw new IOException("unsupported log version " + version + " in " + file);
        }
    }

    static int checksum(CRC32 crc, byte op, long u, long v) {
        crc.reset();
        crc.update(op);
        updateLong(crc, u);
        updateLong(crc, v);
        return (int) crc.getValue();
    }

    static void putFrame(ByteBuffer destination, CRC32 crc, byte op, long u, long v) {
        destination.put(op)
                .put((byte) 0)
                .putShort((short) 0)
                .putInt(checksum(crc, op, u, v))
                .putLong(u)
                .putLong(v);
    }

    static boolean isKnownOp(byte op) {
        return op == OP_ADD_EDGE || op == OP_REMOVE_EDGE;
    }

    static void writeEmptyLog(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer header = newHeader(System.currentTimeMillis());
            while (header.hasRemaining()) {
                channel.write(header);
            }
            channel.force(true);
        }
    }

    private static void updateLong(CRC32 crc, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            crc.update((int) (value >>> shift));
        }
    }
}
