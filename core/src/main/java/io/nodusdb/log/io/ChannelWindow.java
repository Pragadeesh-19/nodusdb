package io.nodusdb.log.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class ChannelWindow {

    private final LogChannel channel;
    private final long limit;
    private ByteBuffer buffer;
    private long bufferOffset;
    private int position;
    private int filled;

    public ChannelWindow(LogChannel channel, long startOffset, long limit, int capacity) {
        this.channel = channel;
        this.limit = limit;
        this.buffer = ByteBuffer.allocate(capacity);
        this.bufferOffset = startOffset;
    }

    public long offset() {
        return bufferOffset + position;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public int position() {
        return position;
    }

    public int available() {
        return filled - position;
    }

    public void advance(int bytes) {
        position += bytes;
    }

    public boolean has(int bytes) throws IOException {
        if (filled - position >= bytes) {
            return true;
        }
        compact(bytes);
        while (filled < bytes) {
            long fileOffset = bufferOffset + filled;
            if (fileOffset >= limit) {
                break;
            }
            int room = (int) Math.min(buffer.capacity() - filled, limit - fileOffset);
            int read = channel.read(ByteBuffer.wrap(buffer.array(), filled, room), fileOffset);
            if (read <= 0) {
                break;
            }
            filled += read;
        }
        return filled >= bytes;
    }

    private void compact(int needed) {
        long next = offset();
        int remaining = filled - position;
        if (needed > buffer.capacity()) {
            ByteBuffer grown = ByteBuffer.allocate(Math.max(needed, buffer.capacity() * 2));
            System.arraycopy(buffer.array(), position, grown.array(), 0, remaining);
            buffer = grown;
        } else {
            System.arraycopy(buffer.array(), position, buffer.array(), 0, remaining);
        }
        bufferOffset = next;
        position = 0;
        filled = remaining;
    }
}
