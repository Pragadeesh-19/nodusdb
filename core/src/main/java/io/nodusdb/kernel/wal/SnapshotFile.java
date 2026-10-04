package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

final class SnapshotFile {

    static final int MAGIC = 0x4E4F4453;
    static final short VERSION = 1;
    static final int HEADER_BYTES = 28;
    static final int TRAILER_BYTES = 4;
    static final int CHUNK_BYTES = 1 << 20;
    static final int BATCH_PAIRS = 1 << 16;

    private SnapshotFile() {
    }

    static void write(GraphKernel kernel, Path target) throws IOException {
        int nodeCapacity = kernel.nodeCapacity();
        long edgeCount = 0;
        for (int node = 0; node < nodeCapacity; node++) {
            edgeCount += kernel.getDegree(node);
        }
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ChunkWriter out = new ChunkWriter(channel);
            out.ensure(HEADER_BYTES);
            out.buffer().putInt(MAGIC).putShort(VERSION).putShort((short) 0)
                    .putInt(nodeCapacity).putLong(edgeCount).putLong(System.currentTimeMillis());
            for (int node = 0; node < nodeCapacity; node++) {
                int degree = kernel.getDegree(node);
                out.ensure(Integer.BYTES);
                out.buffer().putInt(degree);
                for (int i = 0; i < degree; i++) {
                    out.ensure(Long.BYTES);
                    out.buffer().putLong(kernel.outgoingNeighbor(node, i));
                }
            }
            out.flush();
            ByteBuffer trailer = ByteBuffer.allocate(TRAILER_BYTES).order(ByteOrder.BIG_ENDIAN);
            trailer.putInt((int) out.checksum()).flip();
            while (trailer.hasRemaining()) {
                channel.write(trailer);
            }
            channel.force(true);
        }
    }

    static void load(Path source, GraphKernel kernel) throws IOException {
        try (FileChannel channel = FileChannel.open(source, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < HEADER_BYTES + TRAILER_BYTES) {
                throw new IOException("snapshot is truncated: " + source);
            }
            long bodyEnd = size - TRAILER_BYTES;
            verifyChecksum(channel, bodyEnd, source);

            ChunkReader in = new ChunkReader(channel, 0, bodyEnd, CHUNK_BYTES);
            require(in, HEADER_BYTES, source);
            ByteBuffer header = in.readable();
            if (header.getInt() != MAGIC) {
                throw new IOException("not a nodus snapshot: " + source);
            }
            short version = header.getShort();
            if (version != VERSION) {
                throw new IOException("unsupported snapshot version " + version + " in " + source);
            }
            header.getShort();
            int nodeCapacity = header.getInt();
            long expectedEdges = header.getLong();
            header.getLong();

            long[] pairs = new long[2 * BATCH_PAIRS];
            int pending = 0;
            long loaded = 0;
            for (int node = 0; node < nodeCapacity; node++) {
                require(in, Integer.BYTES, source);
                int degree = in.readable().getInt();
                if (degree < 0) {
                    throw new IOException("negative degree in snapshot: " + source);
                }
                for (int i = 0; i < degree; i++) {
                    require(in, Long.BYTES, source);
                    pairs[2 * pending] = node;
                    pairs[2 * pending + 1] = in.readable().getLong();
                    pending++;
                    if (pending == BATCH_PAIRS) {
                        kernel.addEdges(pairs, pending);
                        loaded += pending;
                        pending = 0;
                    }
                }
            }
            if (pending > 0) {
                kernel.addEdges(pairs, pending);
                loaded += pending;
            }
            if (loaded != expectedEdges || in.offset() != bodyEnd) {
                throw new IOException("snapshot edge count does not match its header: " + source);
            }
        }
    }

    private static void verifyChecksum(FileChannel channel, long bodyEnd, Path source) throws IOException {
        CRC32 crc = new CRC32();
        ByteBuffer chunk = ByteBuffer.allocateDirect(CHUNK_BYTES);
        long position = 0;
        while (position < bodyEnd) {
            chunk.clear();
            chunk.limit((int) Math.min(CHUNK_BYTES, bodyEnd - position));
            int read = channel.read(chunk, position);
            if (read <= 0) {
                throw new IOException("snapshot ended early: " + source);
            }
            chunk.flip();
            crc.update(chunk);
            position += read;
        }
        ByteBuffer stored = ByteBuffer.allocate(TRAILER_BYTES);
        if (channel.read(stored, bodyEnd) < TRAILER_BYTES || (int) crc.getValue() != stored.getInt(0)) {
            throw new IOException("snapshot checksum mismatch: " + source);
        }
    }

    private static void require(ChunkReader in, int bytes, Path source) throws IOException {
        if (!in.ensure(bytes)) {
            throw new IOException("snapshot body is truncated: " + source);
        }
    }

    private static final class ChunkWriter {

        private final FileChannel channel;
        private final ByteBuffer buffer = ByteBuffer.allocateDirect(CHUNK_BYTES).order(ByteOrder.BIG_ENDIAN);
        private final CRC32 crc = new CRC32();

        ChunkWriter(FileChannel channel) {
            this.channel = channel;
        }

        ByteBuffer buffer() {
            return buffer;
        }

        void ensure(int bytes) throws IOException {
            if (buffer.remaining() < bytes) {
                flush();
            }
        }

        void flush() throws IOException {
            buffer.flip();
            crc.update(buffer.duplicate());
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            buffer.clear();
        }

        long checksum() {
            return crc.getValue();
        }
    }
}
