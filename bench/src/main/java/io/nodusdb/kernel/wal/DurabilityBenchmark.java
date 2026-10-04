package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public final class DurabilityBenchmark {

    private static final int BATCH = 1 << 20;
    private static final int NODES = 1_700_000;
    private static final int REMOVE_HEAD = 1_000_000;
    private static final int REMOVE_TAIL = 500_000;
    private static final int QUERY_STARTS = 300;
    private static final int QUERY_DEPTH = 3;
    private static final int SINGLE_CALL_SYNC = 5_000;
    private static final int SINGLE_CALL_ASYNC = 1_000_000;

    private final PrintStream out;
    private final Path edges;
    private final Path work;

    private DurabilityBenchmark(PrintStream out, Path edges, Path work) {
        this.out = out;
        this.edges = edges;
        this.work = work;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: <phase> <work-dir> [dataset.txt.gz]");
        }
        Path work = Path.of(args[1]);
        Files.createDirectories(work);
        DurabilityBenchmark benchmark = new DurabilityBenchmark(System.out, work.resolve("edges.bin"), work);
        switch (args[0]) {
            case "prepare" -> benchmark.prepare(Path.of(args[2]));
            case "memory" -> benchmark.inMemory();
            case "durable-async" -> benchmark.durableAsync();
            case "recover" -> benchmark.recover(work.resolve(args.length > 2 ? args[2] : "durable-async"));
            case "durable-sync-batch" -> benchmark.durableSyncBatch();
            case "single-call" -> benchmark.singleCall();
            default -> throw new IllegalArgumentException("unknown phase " + args[0]);
        }
        System.out.flush();
    }

    private void prepare(Path dataset) throws IOException {
        long count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(Files.newInputStream(dataset), 1 << 16), StandardCharsets.US_ASCII), 1 << 16);
             OutputStream sink = new BufferedOutputStream(Files.newOutputStream(edges), 1 << 20)) {
            ByteBuffer pair = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                int tab = line.indexOf('\t');
                pair.clear();
                pair.putLong(Long.parseLong(line, 0, tab, 10));
                pair.putLong(Long.parseLong(line, tab + 1, line.length(), 10));
                sink.write(pair.array());
                count++;
            }
        }
        out.printf("prepared edges=%d file=%s%n", count, edges);
    }

    private void inMemory() throws IOException {
        GraphKernel graph = GraphKernel.openInMemory();
        long[] starts = sampleStarts();
        long ingestStart = System.nanoTime();
        long edgeCount = ingestAll(graph);
        report("memory", "ingest", edgeCount, ingestStart);
        long removed = removeHead(graph, REMOVE_HEAD + REMOVE_TAIL);
        out.printf("RESULT phase=memory removed=%d%n", removed);
        out.printf("DIGEST phase=memory value=%016x%n", digest(graph, starts));
    }

    private void durableAsync() throws IOException {
        Path directory = freshDirectory("durable-async");
        RecoveryManager.Opened opened = RecoveryManager.open(directory, WalConfig.DEFAULT);
        GraphKernel graph = opened.kernel();
        long[] starts = sampleStarts();
        long ingestStart = System.nanoTime();
        long edgeCount = ingestAll(graph);
        report("durable-async", "ingest", edgeCount, ingestStart);
        long headStart = System.nanoTime();
        removeHead(graph, REMOVE_HEAD);
        report("durable-async", "remove-head", REMOVE_HEAD, headStart);

        long checkpointStart = System.nanoTime();
        graph.checkpoint();
        long checkpointNanos = System.nanoTime() - checkpointStart;
        out.printf("RESULT phase=durable-async metric=checkpoint seconds=%.3f snapshot-bytes=%d%n",
                checkpointNanos / 1e9, Files.size(directory.resolve(DurableStore.SNAPSHOT)));

        long tailStart = System.nanoTime();
        long tailRemoved = removeRange(graph, REMOVE_HEAD, REMOVE_TAIL);
        graph.sync();
        report("durable-async", "remove-tail", tailRemoved, tailStart);
        out.printf("DIGEST phase=durable-async-before-crash value=%016x%n", digest(graph, starts));
        out.printf("STATE phase=durable-async directory=%s%n", directory);
        opened.store().abandon();
    }

    private void recover(Path directory) throws IOException {
        long[] starts = sampleStarts();
        long recoveryStart = System.nanoTime();
        RecoveryManager.Recovery recovery = RecoveryManager.recover(directory, WalConfig.DEFAULT);
        long recoveryNanos = System.nanoTime() - recoveryStart;
        out.printf("RESULT phase=recover metric=recovery seconds=%.3f frames-replayed=%d truncated-bytes=%d%n",
                recoveryNanos / 1e9, recovery.framesApplied(), recovery.truncatedBytes());
        out.printf("DIGEST phase=recover value=%016x%n", digest(recovery.kernel(), starts));
        recovery.kernel().close();
    }

    private void durableSyncBatch() throws IOException {
        Path directory = freshDirectory("durable-sync-batch");
        GraphKernel graph = GraphKernel.open(directory, WalConfig.withSyncMode(SyncMode.SYNC));
        long[] starts = sampleStarts();
        long ingestStart = System.nanoTime();
        long edgeCount = ingestAll(graph);
        report("durable-sync-batch", "ingest", edgeCount, ingestStart);

        long checkpointStart = System.nanoTime();
        graph.checkpoint();
        out.printf("RESULT phase=durable-sync-batch metric=checkpoint seconds=%.3f%n",
                (System.nanoTime() - checkpointStart) / 1e9);
        out.printf("DIGEST phase=durable-sync-batch-before-close value=%016x%n", digest(graph, starts));
        graph.close();

        out.printf("STATE phase=durable-sync-batch directory=%s%n", directory);
    }

    private void singleCall() throws IOException {
        long[] pairs = readPrefix(SINGLE_CALL_ASYNC);

        Path syncDirectory = freshDirectory("single-call-sync");
        GraphKernel syncGraph = GraphKernel.open(syncDirectory, WalConfig.withSyncMode(SyncMode.SYNC));
        long[] latencies = new long[SINGLE_CALL_SYNC];
        long syncStart = System.nanoTime();
        for (int i = 0; i < SINGLE_CALL_SYNC; i++) {
            long callStart = System.nanoTime();
            syncGraph.addEdge(pairs[2 * i], pairs[2 * i + 1]);
            latencies[i] = System.nanoTime() - callStart;
        }
        long syncNanos = System.nanoTime() - syncStart;
        syncGraph.close();
        Arrays.sort(latencies);
        out.printf("RESULT phase=single-call metric=sync-per-call edges=%d edges-per-second=%.0f "
                + "median-us=%.1f p99-us=%.1f%n",
                SINGLE_CALL_SYNC, SINGLE_CALL_SYNC / (syncNanos / 1e9),
                latencies[SINGLE_CALL_SYNC / 2] / 1e3, latencies[(int) (SINGLE_CALL_SYNC * 0.99)] / 1e3);

        Path asyncDirectory = freshDirectory("single-call-async");
        GraphKernel asyncGraph = GraphKernel.open(asyncDirectory, WalConfig.DEFAULT);
        long asyncStart = System.nanoTime();
        for (int i = 0; i < SINGLE_CALL_ASYNC; i++) {
            asyncGraph.addEdge(pairs[2 * i], pairs[2 * i + 1]);
        }
        report("single-call", "async-per-call", SINGLE_CALL_ASYNC, asyncStart);
        asyncGraph.close();
    }

    private long ingestAll(GraphKernel graph) throws IOException {
        long[] batch = new long[2 * BATCH];
        long total = 0;
        try (FileChannel channel = FileChannel.open(edges, StandardOpenOption.READ)) {
            ByteBuffer raw = ByteBuffer.allocate(16 * BATCH).order(ByteOrder.BIG_ENDIAN);
            while (channel.read(raw) > 0) {
                raw.flip();
                int pairs = raw.remaining() / 16;
                raw.asLongBuffer().get(batch, 0, 2 * pairs);
                graph.addEdges(batch, pairs);
                total += pairs;
                raw.clear();
            }
        }
        return total;
    }

    private long removeHead(GraphKernel graph, int count) throws IOException {
        return removeRange(graph, 0, count);
    }

    private long removeRange(GraphKernel graph, long skip, long count) throws IOException {
        long[] batch = new long[2 * BATCH];
        long removed = 0;
        long consumed = 0;
        try (FileChannel channel = FileChannel.open(edges, StandardOpenOption.READ)) {
            ByteBuffer raw = ByteBuffer.allocate(16 * BATCH).order(ByteOrder.BIG_ENDIAN);
            long position = skip * 16;
            while (consumed < count) {
                raw.clear();
                raw.limit((int) Math.min(raw.capacity(), (count - consumed) * 16));
                int read = channel.read(raw, position);
                if (read <= 0) {
                    break;
                }
                raw.flip();
                int pairs = raw.remaining() / 16;
                raw.asLongBuffer().get(batch, 0, 2 * pairs);
                removed += graph.removeEdges(batch, pairs);
                consumed += pairs;
                position += (long) pairs * 16;
            }
        }
        return removed;
    }

    private long[] readPrefix(int count) throws IOException {
        long[] pairs = new long[2 * count];
        try (FileChannel channel = FileChannel.open(edges, StandardOpenOption.READ)) {
            ByteBuffer raw = ByteBuffer.allocate(16 * count).order(ByteOrder.BIG_ENDIAN);
            while (raw.hasRemaining() && channel.read(raw) > 0) {
                continue;
            }
            raw.flip();
            raw.asLongBuffer().get(pairs);
        }
        return pairs;
    }

    private long[] sampleStarts() throws IOException {
        long[] sample = new long[QUERY_STARTS];
        long total = Files.size(edges) / 16;
        long step = total / QUERY_STARTS;
        try (FileChannel channel = FileChannel.open(edges, StandardOpenOption.READ)) {
            ByteBuffer one = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < QUERY_STARTS; i++) {
                one.clear();
                channel.read(one, i * step * 16);
                sample[i] = one.getLong(0);
            }
        }
        return sample;
    }

    private static long digest(GraphKernel graph, long[] starts) {
        long hash = 0xcbf29ce484222325L;
        for (int node = 0; node < NODES; node++) {
            hash = mix(hash, graph.getDegree(node));
            hash = mix(hash, graph.getInDegree(node));
        }
        long[] buffer = new long[NODES];
        for (long start : starts) {
            int count = graph.kHop(start, QUERY_DEPTH, buffer);
            long[] reached = Arrays.copyOf(buffer, count);
            Arrays.sort(reached);
            for (long node : reached) {
                hash = mix(hash, node);
            }
        }
        return hash;
    }

    private static long mix(long hash, long value) {
        return (hash ^ value) * 0x100000001b3L;
    }

    private void report(String phase, String metric, long edgeCount, long startNanos) {
        double seconds = (System.nanoTime() - startNanos) / 1e9;
        out.printf("RESULT phase=%s metric=%s edges=%d seconds=%.3f edges-per-second=%.0f%n",
                phase, metric, edgeCount, seconds, edgeCount / seconds);
    }

    private Path freshDirectory(String name) throws IOException {
        Path directory = work.resolve(name);
        if (Files.exists(directory)) {
            try (Stream<Path> files = Files.walk(directory)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(file);
                }
            }
        }
        Files.createDirectories(directory);
        return directory;
    }
}
