package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

final class LogReplayer {

    static final int CHUNK_BYTES = 1 << 20;

    record Result(long framesApplied, long truncatedBytes) {
    }

    private LogReplayer() {
    }

    static Result replay(Path log, GraphKernel kernel) throws IOException {
        try (FileChannel channel = FileChannel.open(log, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long size = channel.size();
            ByteBuffer header = ByteBuffer.allocate(WalFormat.HEADER_BYTES);
            if (size < WalFormat.HEADER_BYTES || channel.read(header, 0) < WalFormat.HEADER_BYTES) {
                throw new IOException("log header is truncated: " + log);
            }
            header.flip();
            WalFormat.checkHeader(header, log);

            ChunkReader reader = new ChunkReader(channel, WalFormat.HEADER_BYTES, size, CHUNK_BYTES);
            CRC32 crc = new CRC32();
            long validEnd = WalFormat.HEADER_BYTES;
            long frames = 0;
            while (reader.ensure(WalFormat.FRAME_BYTES)) {
                ByteBuffer in = reader.readable();
                byte op = in.get();
                in.get();
                in.getShort();
                int stored = in.getInt();
                long u = in.getLong();
                long v = in.getLong();
                if (!WalFormat.isKnownOp(op) || WalFormat.checksum(crc, op, u, v) != stored) {
                    break;
                }
                apply(kernel, op, u, v);
                validEnd = reader.offset();
                frames++;
            }
            long truncated = size - validEnd;
            if (truncated > 0) {
                channel.truncate(validEnd);
                channel.force(true);
            }
            return new Result(frames, truncated);
        }
    }

    private static void apply(GraphKernel kernel, byte op, long u, long v) {
        if (op == WalFormat.OP_ADD_EDGE) {
            kernel.addEdge(u, v);
        } else {
            kernel.removeEdge(u, v);
        }
    }
}
