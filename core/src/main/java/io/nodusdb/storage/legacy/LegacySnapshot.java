package io.nodusdb.storage.legacy;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.storage.io.ChunkReader;
import io.nodusdb.storage.snapshot.AdjacencyLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

final class LegacySnapshot {

    private static final int MAGIC = 0x4E4F4453;
    private static final short FORWARD_ONLY_VERSION = 1;
    private static final short TWO_SECTION_VERSION = 2;
    private static final int HEADER_BYTES = 28;
    private static final int TRAILER_BYTES = 4;
    private static final int CHUNK_BYTES = 1 << 20;
    private static final int BATCH_PAIRS = 1 << 16;

    private record Header(short version, int nodeCapacity, long edgeCount) {
    }

    private LegacySnapshot() {
    }

    static void load(Path source, GraphKernel kernel) throws IOException {
        try (FileChannel channel = FileChannel.open(source, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < HEADER_BYTES + TRAILER_BYTES) {
                throw new IOException("snapshot is truncated: " + source);
            }
            long bodyEnd = size - TRAILER_BYTES;
            verifyChecksum(channel, bodyEnd, source);
            Header header = readHeader(channel, source);
            if (header.version() == FORWARD_ONLY_VERSION) {
                loadForwardEdges(channel, bodyEnd, header, kernel, source);
            } else {
                loadSections(channel, bodyEnd, header, kernel, source);
            }
        }
    }

    private static Header readHeader(FileChannel channel, Path source) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        if (channel.read(bytes, 0) < HEADER_BYTES) {
            throw new IOException("snapshot header is truncated: " + source);
        }
        if (bytes.getInt(0) != MAGIC) {
            throw new IOException("not a nodus snapshot: " + source);
        }
        short version = bytes.getShort(4);
        if (version != FORWARD_ONLY_VERSION && version != TWO_SECTION_VERSION) {
            throw new IOException("unsupported snapshot version " + version + " in " + source);
        }
        int nodeCapacity = bytes.getInt(8);
        if (nodeCapacity < 0) {
            throw new IOException("negative node capacity in snapshot: " + source);
        }
        return new Header(version, nodeCapacity, bytes.getLong(12));
    }

    private static void loadSections(FileChannel channel, long bodyEnd, Header header, GraphKernel kernel,
                                     Path source) throws IOException {
        if (header.nodeCapacity() == 0) {
            return;
        }
        long forwardEnd = endOfSection(channel, HEADER_BYTES, bodyEnd, header, source);
        AdjacencyLoader.Extent forward = new AdjacencyLoader.Extent(HEADER_BYTES, forwardEnd);
        AdjacencyLoader.Extent backward = new AdjacencyLoader.Extent(forwardEnd, bodyEnd);
        new AdjacencyLoader(channel, kernel, RelationCatalog.EMPTY, header.nodeCapacity(), source)
                .load(Partition.DIRECT, forward, backward);
    }

    private static long endOfSection(FileChannel channel, long start, long bodyEnd, Header header, Path source)
            throws IOException {
        ChunkReader reader = new ChunkReader(channel, start, bodyEnd, CHUNK_BYTES);
        long edges = 0;
        for (int node = 0; node < header.nodeCapacity(); node++) {
            if (!reader.ensure(Integer.BYTES)) {
                throw new IOException("snapshot body is truncated: " + source);
            }
            int degree = reader.readable().getInt();
            if (degree < 0 || degree > header.nodeCapacity()) {
                throw new IOException("degree out of range in snapshot: " + source);
            }
            edges += degree;
            reader.skip((long) Long.BYTES * degree);
        }
        if (edges != header.edgeCount()) {
            throw new IOException("snapshot section edge count does not match its header: " + source);
        }
        return reader.offset();
    }

    private static void loadForwardEdges(FileChannel channel, long bodyEnd, Header header, GraphKernel kernel,
                                         Path source) throws IOException {
        ChunkReader reader = new ChunkReader(channel, HEADER_BYTES, bodyEnd, CHUNK_BYTES);
        long[] pairs = new long[2 * BATCH_PAIRS];
        int pending = 0;
        long loaded = 0;
        for (int node = 0; node < header.nodeCapacity(); node++) {
            if (!reader.ensure(Integer.BYTES)) {
                throw new IOException("snapshot body is truncated: " + source);
            }
            int degree = reader.readable().getInt();
            if (degree < 0) {
                throw new IOException("negative degree in snapshot: " + source);
            }
            for (int i = 0; i < degree; i++) {
                if (!reader.ensure(Long.BYTES)) {
                    throw new IOException("snapshot body is truncated: " + source);
                }
                pairs[2 * pending] = node;
                pairs[2 * pending + 1] = reader.readable().getLong();
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
        if (loaded != header.edgeCount() || reader.offset() != bodyEnd) {
            throw new IOException("snapshot edge count does not match its header: " + source);
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
}
