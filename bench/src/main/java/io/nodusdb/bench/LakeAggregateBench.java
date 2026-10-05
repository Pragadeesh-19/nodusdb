package io.nodusdb.bench;

import io.nodusdb.lake.LakeSchema;
import io.nodusdb.lake.LakeTable;
import io.nodusdb.lake.ParquetCodec;
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
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class LakeAggregateBench {

    private static final int ROWS = 1 << 20;
    private static final LakeSchema SCHEMA = LakeSchema.parse("amount:INT64,score:DOUBLE,status:INT32,label:UTF8");
    private static final LakeTable.Config CONFIG = new LakeTable.Config(1 << 21, 1 << 28, 1 << 21, 1 << 24, 0L,
            ParquetCodec.SNAPPY);
    private static final long KEY_STRIDE = 0x9E3779B97F4A7C15L;
    private static final byte[] LABEL = "payload".getBytes(StandardCharsets.UTF_8);

    private Path directory;
    private LakeTable table;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = Files.createTempDirectory("nodus-lake-aggregate");
        table = LakeTable.open(directory, SCHEMA, CONFIG);
        long[] longs = new long[2];
        int[] ints = new int[1];
        int[] lengths = {LABEL.length};
        MemorySegment longSegment = MemorySegment.ofArray(longs);
        MemorySegment intSegment = MemorySegment.ofArray(ints);
        MemorySegment labelSegment = MemorySegment.ofArray(LABEL);
        MemorySegment lengthSegment = MemorySegment.ofArray(lengths);
        for (int row = 0; row < ROWS; row++) {
            longs[0] = row;
            longs[1] = Double.doubleToRawLongBits(row * 0.25);
            ints[0] = row & 1023;
            table.upsert(((long) row + 1) * KEY_STRIDE, longSegment, intSegment, labelSegment, lengthSegment);
        }
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

    @Benchmark
    public double sumDoubles() {
        return table.aggregate(1).sum();
    }

    @Benchmark
    public double sumLongs() {
        return table.aggregate(0).sum();
    }

    @Benchmark
    public double sumInts() {
        return table.aggregate(2).sum();
    }
}
