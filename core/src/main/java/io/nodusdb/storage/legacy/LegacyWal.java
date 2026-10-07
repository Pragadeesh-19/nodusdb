package io.nodusdb.storage.legacy;

import io.nodusdb.storage.io.ChunkReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

final class LegacyWal {

    interface EdgeSink {

        void add(long source, long target);

        void remove(long source, long target);
    }

    private static final int MAGIC = 0x4E4F4455;
    private static final short VERSION = 1;
    private static final int HEADER_BYTES = 16;
    private static final int FRAME_BYTES = 24;
    private static final int CHUNK_BYTES = 1 << 20;
    private static final byte OP_ADD_EDGE = 0x01;
    private static final byte OP_REMOVE_EDGE = 0x02;

    private LegacyWal() {
    }

    static long replay(Path log, EdgeSink sink) throws IOException {
        try (FileChannel channel = FileChannel.open(log, StandardOpenOption.READ)) {
            long size = channel.size();
            checkHeader(channel, size, log);
            ChunkReader reader = new ChunkReader(channel, HEADER_BYTES, size, CHUNK_BYTES);
            CRC32 crc = new CRC32();
            long frames = 0;
            while (reader.ensure(FRAME_BYTES)) {
                ByteBuffer in = reader.readable();
                byte op = in.get();
                in.get();
                in.getShort();
                int stored = in.getInt();
                long source = in.getLong();
                long target = in.getLong();
                if (!isKnown(op) || checksum(crc, op, source, target) != stored) {
                    break;
                }
                if (op == OP_ADD_EDGE) {
                    sink.add(source, target);
                } else {
                    sink.remove(source, target);
                }
                frames++;
            }
            return frames;
        }
    }

    private static void checkHeader(FileChannel channel, long size, Path log) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        if (size < HEADER_BYTES || channel.read(header, 0) < HEADER_BYTES) {
            throw new IOException("log header is truncated: " + log);
        }
        if (header.getInt(0) != MAGIC) {
            throw new IOException("not a nodus log: " + log);
        }
        if (header.getShort(4) != VERSION) {
            throw new IOException("unsupported log version " + header.getShort(4) + " in " + log);
        }
    }

    private static boolean isKnown(byte op) {
        return op == OP_ADD_EDGE || op == OP_REMOVE_EDGE;
    }

    private static int checksum(CRC32 crc, byte op, long source, long target) {
        crc.reset();
        crc.update(op);
        updateLong(crc, source);
        updateLong(crc, target);
        return (int) crc.getValue();
    }

    private static void updateLong(CRC32 crc, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            crc.update((int) (value >>> shift));
        }
    }
}
