package io.nodusdb.storage.snapshot;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.storage.io.ChunkReader;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class AdjacencyLoader {

    private static final int INITIAL_NEIGHBORS = 64;
    private static final int FILL_CHUNK_BYTES = 1 << 18;
    private static final int TILES_PER_THREAD = 4;
    private static final int THREADS = Math.max(1, Runtime.getRuntime().availableProcessors());

    public record Extent(long start, long end) {

        public long length() {
            return end - start;
        }
    }

    private record Direction(Extent extent, int[] degrees, long[] starts) {
    }

    private final FileChannel channel;
    private final GraphKernel kernel;
    private final RelationCatalog catalog;
    private final int nodes;
    private final Path source;

    public AdjacencyLoader(FileChannel channel, GraphKernel kernel, RelationCatalog catalog, int nodes, Path source) {
        this.channel = channel;
        this.kernel = kernel;
        this.catalog = catalog;
        this.nodes = nodes;
        this.source = source;
    }

    public void load(Partition partition, Extent outgoing, Extent incoming) throws IOException {
        Direction out = scan(outgoing);
        Direction in = scan(incoming);
        if (edgeCount(out) != edgeCount(in)) {
            throw new IOException("snapshot outgoing and incoming edge counts differ: " + source);
        }
        kernel.prepareBulkLoad(partition, out.degrees(), in.degrees());
        fill(partition, out, true);
        fill(partition, in, false);
    }

    private static long edgeCount(Direction direction) {
        long edges = 0;
        for (int degree : direction.degrees()) {
            edges += degree;
        }
        return edges;
    }

    private Direction scan(Extent extent) throws IOException {
        int[] degrees = new int[nodes];
        long[] starts = new long[nodes];
        ChunkReader reader = new ChunkReader(channel, extent.start(), extent.end(), SnapshotFormat.CHUNK_BYTES);
        for (int node = 0; node < nodes; node++) {
            starts[node] = reader.offset();
            if (!reader.ensure(Integer.BYTES)) {
                throw new IOException("snapshot adjacency section is truncated: " + source);
            }
            int degree = reader.readable().getInt();
            if (degree < 0 || degree > nodes) {
                throw new IOException("degree out of range in snapshot: " + source);
            }
            degrees[node] = degree;
            reader.skip((long) Long.BYTES * degree);
        }
        if (reader.offset() != extent.end()) {
            throw new IOException("snapshot adjacency section has unread bytes: " + source);
        }
        return new Direction(extent, degrees, starts);
    }

    private void fill(Partition partition, Direction direction, boolean forward) throws IOException {
        int tiles = Math.min(nodes, TILES_PER_THREAD * THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<Void>> pending = new ArrayList<>();
            for (int tile = 0; tile < tiles; tile++) {
                int from = (int) ((long) nodes * tile / tiles);
                int to = (int) ((long) nodes * (tile + 1) / tiles);
                long regionEnd = to < nodes ? direction.starts()[to] : direction.extent().end();
                pending.add(pool.submit(new FillTask(partition, forward, from, to, direction.starts()[from],
                        regionEnd, direction.degrees())));
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

    private boolean isValidKey(long key) {
        if (key < 0 || EdgeKey.id(key) >= nodes) {
            return false;
        }
        int relation = EdgeKey.relation(key);
        int subjectRelation = EdgeKey.subjectRelation(key);
        return (relation == 0 || catalog.has(relation)) && (subjectRelation == 0 || catalog.has(subjectRelation));
    }

    private final class FillTask implements Callable<Void> {

        private final Partition partition;
        private final boolean forward;
        private final int from;
        private final int to;
        private final long regionStart;
        private final long regionEnd;
        private final int[] expectedDegrees;

        FillTask(Partition partition, boolean forward, int from, int to, long regionStart, long regionEnd,
                 int[] expectedDegrees) {
            this.partition = partition;
            this.forward = forward;
            this.from = from;
            this.to = to;
            this.regionStart = regionStart;
            this.regionEnd = regionEnd;
            this.expectedDegrees = expectedDegrees;
        }

        @Override
        public Void call() throws IOException {
            ChunkReader reader = new ChunkReader(channel, regionStart, regionEnd, FILL_CHUNK_BYTES);
            long[] keys = new long[INITIAL_NEIGHBORS];
            for (int node = from; node < to; node++) {
                int degree = readDegree(reader);
                if (degree != expectedDegrees[node]) {
                    throw new IOException("snapshot record does not match its scan: " + source);
                }
                if (degree > keys.length) {
                    keys = new long[degree];
                }
                for (int i = 0; i < degree; i++) {
                    keys[i] = readKey(reader);
                }
                if (degree > 0) {
                    kernel.loadBulkNode(partition, forward, node, keys, degree);
                }
            }
            return null;
        }

        private int readDegree(ChunkReader reader) throws IOException {
            if (!reader.ensure(Integer.BYTES)) {
                throw new IOException("snapshot body is truncated: " + source);
            }
            return reader.readable().getInt();
        }

        private long readKey(ChunkReader reader) throws IOException {
            if (!reader.ensure(Long.BYTES)) {
                throw new IOException("snapshot body is truncated: " + source);
            }
            long key = reader.readable().getLong();
            if (!isValidKey(key)) {
                throw new IOException("snapshot key is out of range or names an unknown relation: " + source);
            }
            return key;
        }
    }
}
