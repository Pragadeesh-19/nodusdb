package io.nodusdb.storage.snapshot;

import io.nodusdb.storage.io.ChunkReader;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

final class SectionCursor {

    private final ChunkReader reader;
    private final long end;
    private final Path source;

    SectionCursor(FileChannel channel, long start, long end, Path source) {
        this.reader = new ChunkReader(channel, start, end, SnapshotFormat.CHUNK_BYTES);
        this.end = end;
        this.source = source;
    }

    int getInt() throws IOException {
        require(Integer.BYTES);
        return reader.readable().getInt();
    }

    int getUnsignedShort() throws IOException {
        require(Short.BYTES);
        return reader.readable().getShort() & 0xFFFF;
    }

    int getByte() throws IOException {
        require(Byte.BYTES);
        return reader.readable().get() & 0xFF;
    }

    long getLong() throws IOException {
        require(Long.BYTES);
        return reader.readable().getLong();
    }

    byte[] getBytes(int length) throws IOException {
        if (length < 0 || length > end - reader.offset()) {
            throw new IOException("snapshot field runs past its section: " + source);
        }
        byte[] bytes = new byte[length];
        int copied = 0;
        while (copied < length) {
            require(1);
            int chunk = Math.min(reader.readable().remaining(), length - copied);
            reader.readable().get(bytes, copied, chunk);
            copied += chunk;
        }
        return bytes;
    }

    void requireFullyConsumed() throws IOException {
        if (reader.offset() != end) {
            throw new IOException("snapshot section has unread bytes: " + source);
        }
    }

    private void require(int bytes) throws IOException {
        if (!reader.ensure(bytes)) {
            throw new IOException("snapshot section is truncated: " + source);
        }
    }
}
