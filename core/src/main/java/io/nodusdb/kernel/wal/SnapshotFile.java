package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

/*
 * Snapshot layout, version 2. Each section lists every node in id order:
 *
 *   header    magic "NODS" | version 2 | reserved | nodeCapacity | edgeCount | created
 *   forward   for node in [0, nodeCapacity):  degree (int) | neighbors (long x degree)
 *   backward  for node in [0, nodeCapacity):  degree (int) | neighbors (long x degree)
 *   trailer   CRC32 over every byte before it
 *
 * Both sections are written from the live kernel, so the loader builds each
 * node at its exact degree and never has to scatter edges into place.
 * Version 1 files hold only the forward section and still load through the
 * edge-by-edge path.
 */
final class SnapshotFile {

    static final int MAGIC = 0x4E4F4453;
    static final short VERSION = 2;
    static final short LEGACY_VERSION = 1;
    static final int HEADER_BYTES = 28;
    static final int TRAILER_BYTES = 4;
    static final int CHUNK_BYTES = 1 << 20;
    static final int BATCH_PAIRS = 1 << 16;
    private static final int INITIAL_NEIGHBORS = 64;
    private static final int FILL_CHUNK_BYTES = 1 << 18;
    private static final int FILL_TILES_PER_THREAD = 4;
    private static final int THREADS = Math.max(1, Runtime.getRuntime().availableProcessors());
    private static final int FORWARD = 0;
    private static final int BACKWARD = 1;

    private record Header(short version, int nodeCapacity, long edgeCount) {
    }

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
            writeSection(out, kernel, nodeCapacity, true);
            writeSection(out, kernel, nodeCapacity, false);
            out.flush();
            ByteBuffer trailer = ByteBuffer.allocate(TRAILER_BYTES).order(ByteOrder.BIG_ENDIAN);
            trailer.putInt((int) out.checksum()).flip();
            while (trailer.hasRemaining()) {
                channel.write(trailer);
            }
            channel.force(true);
        }
    }

    private static void writeSection(ChunkWriter out, GraphKernel kernel, int nodeCapacity, boolean forward)
            throws IOException {
        for (int node = 0; node < nodeCapacity; node++) {
            int degree = forward ? kernel.getDegree(node) : kernel.getInDegree(node);
            out.ensure(Integer.BYTES);
            out.buffer().putInt(degree);
            for (int i = 0; i < degree; i++) {
                out.ensure(Long.BYTES);
                long neighbor = forward ? kernel.outgoingNeighbor(node, i) : kernel.incomingNeighbor(node, i);
                out.buffer().putLong(neighbor);
            }
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
            Header header = readHeader(in.readable(), source);
            if (header.version() == LEGACY_VERSION) {
                loadForwardEdges(in, header, kernel, source);
                if (in.offset() != bodyEnd) {
                    throw new IOException("snapshot has bytes after its last section: " + source);
                }
            } else {
                loadSections(channel, bodyEnd, header, kernel, source);
            }
        }
    }

    private static Header readHeader(ByteBuffer header, Path source) throws IOException {
        if (header.getInt() != MAGIC) {
            throw new IOException("not a nodus snapshot: " + source);
        }
        short version = header.getShort();
        if (version != VERSION && version != LEGACY_VERSION) {
            throw new IOException("unsupported snapshot version " + version + " in " + source);
        }
        header.getShort();
        int nodeCapacity = header.getInt();
        long edgeCount = header.getLong();
        header.getLong();
        if (nodeCapacity < 0) {
            throw new IOException("negative node capacity in snapshot: " + source);
        }
        return new Header(version, nodeCapacity, edgeCount);
    }

    private static void loadSections(FileChannel channel, long bodyEnd, Header header, GraphKernel kernel, Path source)
            throws IOException {
        int nodes = header.nodeCapacity();
        int[][] degrees = {new int[nodes], new int[nodes]};
        long[][] starts = {new long[nodes], new long[nodes]};
        scanSections(channel, bodyEnd, header, degrees, starts, source);
        kernel.prepareBulkLoad(degrees[FORWARD], degrees[BACKWARD]);
        if (nodes > 0) {
            fillInParallel(channel, bodyEnd, header, kernel, degrees, starts, source);
        }
    }

    private static void scanSections(FileChannel channel, long bodyEnd, Header header, int[][] degrees,
                                     long[][] starts, Path source) throws IOException {
        ChunkReader in = new ChunkReader(channel, HEADER_BYTES, bodyEnd, CHUNK_BYTES);
        for (int section = 0; section < 2; section++) {
            long edges = 0;
            for (int node = 0; node < header.nodeCapacity(); node++) {
                starts[section][node] = in.offset();
                require(in, Integer.BYTES, source);
                int degree = in.readable().getInt();
                if (degree < 0 || degree > header.nodeCapacity()) {
                    throw new IOException("degree out of range in snapshot: " + source);
                }
                degrees[section][node] = degree;
                in.skip((long) Long.BYTES * degree);
                edges += degree;
            }
            if (edges != header.edgeCount()) {
                throw new IOException("snapshot section edge count does not match its header: " + source);
            }
        }
        if (in.offset() != bodyEnd) {
            throw new IOException("snapshot has bytes after its last section: " + source);
        }
    }

    private static void fillInParallel(FileChannel channel, long bodyEnd, Header header, GraphKernel kernel,
                                       int[][] degrees, long[][] starts, Path source) throws IOException {
        int nodes = header.nodeCapacity();
        int tiles = Math.min(nodes, FILL_TILES_PER_THREAD * THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<Void>> pending = new ArrayList<>();
            for (int section = 0; section < 2; section++) {
                long sectionEnd = section == FORWARD ? starts[BACKWARD][0] : bodyEnd;
                for (int tile = 0; tile < tiles; tile++) {
                    int from = (int) ((long) nodes * tile / tiles);
                    int to = (int) ((long) nodes * (tile + 1) / tiles);
                    long regionEnd = to < nodes ? starts[section][to] : sectionEnd;
                    pending.add(pool.submit(new FillTask(channel, section == FORWARD, from, to,
                            starts[section][from], regionEnd, degrees[section], nodes, kernel, source)));
                }
            }
            for (Future<Void> future : pending) {
                await(future);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static void await(Future<Void> future) throws IOException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("snapshot load was interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException("snapshot load failed", cause);
        }
    }

    private record FillTask(FileChannel channel, boolean forward, int from, int to, long regionStart,
                            long regionEnd, int[] expectedDegrees, int nodeCapacity, GraphKernel kernel,
                            Path source) implements Callable<Void> {

        @Override
        public Void call() throws IOException {
            ChunkReader in = new ChunkReader(channel, regionStart, regionEnd, FILL_CHUNK_BYTES);
            long[] neighbors = new long[INITIAL_NEIGHBORS];
            for (int node = from; node < to; node++) {
                require(in, Integer.BYTES, source);
                int degree = in.readable().getInt();
                if (degree != expectedDegrees[node]) {
                    throw new IOException("snapshot record does not match its scan: " + source);
                }
                if (degree > neighbors.length) {
                    neighbors = new long[degree];
                }
                for (int i = 0; i < degree; i++) {
                    require(in, Long.BYTES, source);
                    long neighbor = in.readable().getLong();
                    if (neighbor < 0 || neighbor >= nodeCapacity) {
                        throw new IOException("neighbor out of range in snapshot: " + source);
                    }
                    neighbors[i] = neighbor;
                }
                if (degree > 0) {
                    kernel.loadBulkNode(forward, node, neighbors, degree);
                }
            }
            return null;
        }
    }

    private static void loadForwardEdges(ChunkReader in, Header header, GraphKernel kernel, Path source)
            throws IOException {
        long[] pairs = new long[2 * BATCH_PAIRS];
        int pending = 0;
        long loaded = 0;
        for (int node = 0; node < header.nodeCapacity(); node++) {
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
        if (loaded != header.edgeCount()) {
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
