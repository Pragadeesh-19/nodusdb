package io.nodusdb.storage.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.zip.CRC32C;

final class SectionTable {

    private final long[] starts = new long[SnapshotFormat.SECTION_COUNT];
    private final long[] lengths = new long[SnapshotFormat.SECTION_COUNT];

    static SectionTable index(FileChannel channel, long size, Path source) throws IOException {
        SectionTable table = new SectionTable();
        long offset = SnapshotFormat.HEADER_BYTES;
        for (int kind = 1; kind <= SnapshotFormat.SECTION_COUNT; kind++) {
            offset = table.register(channel, size, offset, kind, source);
        }
        if (offset != size) {
            throw new IOException("snapshot has bytes after its last section: " + source);
        }
        return table;
    }

    long start(int kind) {
        return starts[kind - 1];
    }

    long length(int kind) {
        return lengths[kind - 1];
    }

    long end(int kind) {
        return start(kind) + length(kind);
    }

    private long register(FileChannel channel, long size, long offset, int kind, Path source) throws IOException {
        if (offset + SnapshotFormat.SECTION_HEADER_BYTES > size) {
            throw new IOException("snapshot is missing section " + kind + ": " + source);
        }
        ByteBuffer header = ByteBuffer.allocate(SnapshotFormat.SECTION_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, header, offset, source);
        if (header.getInt(0) != kind) {
            throw new IOException("snapshot section " + header.getInt(0) + " is out of order, expected "
                    + kind + ": " + source);
        }
        long length = header.getLong(4);
        long bodyStart = offset + SnapshotFormat.SECTION_HEADER_BYTES;
        if (length < 0 || bodyStart + length > size) {
            throw new IOException("snapshot section " + kind + " runs past the end of the file: " + source);
        }
        if ((int) checksum(channel, bodyStart, length, source) != header.getInt(12)) {
            throw new IOException("snapshot section " + kind + " checksum mismatch: " + source);
        }
        starts[kind - 1] = bodyStart;
        lengths[kind - 1] = length;
        return bodyStart + length;
    }

    private static long checksum(FileChannel channel, long start, long length, Path source) throws IOException {
        CRC32C crc = new CRC32C();
        ByteBuffer chunk = ByteBuffer.allocateDirect(SnapshotFormat.CHUNK_BYTES);
        long position = start;
        long end = start + length;
        while (position < end) {
            chunk.clear();
            chunk.limit((int) Math.min(chunk.capacity(), end - position));
            int read = channel.read(chunk, position);
            if (read <= 0) {
                throw new IOException("snapshot ended early: " + source);
            }
            chunk.flip();
            crc.update(chunk);
            position += read;
        }
        return crc.getValue();
    }

    private static void readFully(FileChannel channel, ByteBuffer destination, long position, Path source)
            throws IOException {
        while (destination.hasRemaining()) {
            int read = channel.read(destination, position + destination.position());
            if (read < 0) {
                throw new IOException("snapshot ended early: " + source);
            }
        }
    }
}
