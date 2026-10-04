package io.nodusdb.bench;

import io.nodusdb.lake.LakeSchema;
import io.nodusdb.lake.LakeTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class LakeTableBench {

    private static final LakeSchema SCHEMA = LakeSchema.parse("amount:INT64,score:DOUBLE,status:INT32,label:UTF8");
    private static final LakeTable.Config CONFIG = new LakeTable.Config(1 << 16, 1 << 26, 1 << 16, 1 << 20, 0L);
    private static final int KEYSPACE = 1 << 20;
    private static final long KEY_STRIDE = 0x9E3779B97F4A7C15L;
    private static final byte[] LABEL = "payload-000016B".getBytes(StandardCharsets.UTF_8);
    private static final int[] LABEL_LENGTHS = {LABEL.length};

    private Path directory;
    private LakeTable table;
    private final long[] longValues = new long[2];
    private final int[] intValues = new int[1];
    private long cursor;
    private long iterationStartCursor;
    private long iterationStartBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = Files.createTempDirectory("nodus-lake-bench");
        table = LakeTable.open(directory, SCHEMA, CONFIG);
    }

    @Setup(Level.Iteration)
    public void startIteration() {
        iterationStartCursor = cursor;
        iterationStartBytes = ingestionThreadAllocatedBytes();
    }

    @TearDown(Level.Iteration)
    public void reportIngestionAllocation() {
        long operations = cursor - iterationStartCursor;
        long bytes = ingestionThreadAllocatedBytes() - iterationStartBytes;
        System.out.printf("ingestion thread allocated %.6f B/op over %d operations%n",
                (double) bytes / operations, operations);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        table.close();
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(file);
            }
        }
    }

    private static long ingestionThreadAllocatedBytes() {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        return ((com.sun.management.ThreadMXBean) threads).getThreadAllocatedBytes(Thread.currentThread().threadId());
    }

    @Benchmark
    public void upsertWhileFlushing() {
        long row = cursor++;
        long key = ((row % KEYSPACE) + 1) * KEY_STRIDE;
        longValues[0] = row;
        longValues[1] = Double.doubleToRawLongBits(row * 0.25);
        intValues[0] = (int) row;
        table.upsert(key, longValues, intValues, LABEL, LABEL_LENGTHS);
    }
}
