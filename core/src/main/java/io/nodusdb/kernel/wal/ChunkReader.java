package io.nodusdb.kernel.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

final class ChunkReader {

    private final FileChannel channel;
    private final long end;
    private final ByteBuffer buffer;
    private long bufferStart;

    ChunkReader(FileChannel channel, long start, long end, int chunkBytes) {
        this.channel = channel;
        this.end = end;
        this.buffer = ByteBuffer.allocateDirect(chunkBytes).order(ByteOrder.BIG_ENDIAN);
        this.buffer.limit(0);
        this.bufferStart = start;
    }

    long offset() {
        return bufferStart + buffer.position();
    }

    ByteBuffer readable() {
        return buffer;
    }

    boolean ensure(int bytes) throws IOException {
        if (buffer.remaining() < bytes) {
            refill(bytes);
        }
        return buffer.remaining() >= bytes;
    }

    private void refill(int needed) throws IOException {
        long next = offset();
        buffer.compact();
        bufferStart = next;
        while (buffer.position() < needed) {
            long fileOffset = bufferStart + buffer.position();
            if (fileOffset >= end) {
                break;
            }
            int room = (int) Math.min(buffer.remaining(), end - fileOffset);
            buffer.limit(buffer.position() + room);
            int read = channel.read(buffer, fileOffset);
            buffer.limit(buffer.capacity());
            if (read <= 0) {
                break;
            }
        }
        buffer.flip();
    }
}
