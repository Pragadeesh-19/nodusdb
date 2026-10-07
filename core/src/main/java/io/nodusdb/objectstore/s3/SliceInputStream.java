package io.nodusdb.objectstore.s3;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

final class SliceInputStream extends InputStream {

    private final FileChannel channel;
    private long position;
    private long remaining;

    SliceInputStream(FileChannel channel, long offset, long length) {
        this.channel = channel;
        this.position = offset;
        this.remaining = length;
    }

    @Override
    public int read() throws IOException {
        byte[] single = new byte[1];
        int read = read(single, 0, 1);
        return read < 0 ? -1 : single[0] & 0xFF;
    }

    @Override
    public int read(byte[] target, int offset, int length) throws IOException {
        if (length == 0) {
            return 0;
        }
        if (remaining == 0) {
            return -1;
        }
        int wanted = (int) Math.min(length, remaining);
        int read = channel.read(ByteBuffer.wrap(target, offset, wanted), position);
        if (read < 0) {
            throw new IOException("the file ended before the slice did");
        }
        position += read;
        remaining -= read;
        return read;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
