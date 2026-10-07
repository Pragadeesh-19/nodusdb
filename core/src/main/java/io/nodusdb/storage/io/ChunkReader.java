package io.nodusdb.storage.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public final class ChunkReader {

    private final FileChannel channel;
    private final long end;
    private final ByteBuffer buffer;
    private long bufferStart;

    public ChunkReader(FileChannel channel, long start, long end, int chunkBytes) {
        this.channel = channel;
        this.end = end;
        this.buffer = ByteBuffer.allocateDirect(chunkBytes).order(ByteOrder.BIG_ENDIAN);
        this.buffer.limit(0);
        this.bufferStart = start;
    }

    public long offset() {
        return bufferStart + buffer.position();
    }

    public ByteBuffer readable() {
        return buffer;
    }

    public void skip(long bytes) {
        long target = offset() + bytes;
        if (target <= bufferStart + buffer.limit()) {
            buffer.position((int) (target - bufferStart));
            return;
        }
        bufferStart = target;
        buffer.clear();
        buffer.limit(0);
    }

    public boolean ensure(int bytes) throws IOException {
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
