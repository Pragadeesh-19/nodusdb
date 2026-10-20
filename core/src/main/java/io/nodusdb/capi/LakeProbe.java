package io.nodusdb.capi;

import io.nodusdb.lake.LakeTable;
import io.nodusdb.lake.buffer.ColumnarRows;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.lake.model.Aggregate;
import io.nodusdb.lake.model.LakeSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class LakeProbe {

    public record Timing(long ingestNanos, long sumNanos, long flushNanos) {
    }

    private static final int DEFAULT_ROWS = 250_000;
    private static final int DEFAULT_RUNS = 5;
    private static final int DEFAULT_WARMUPS = 2;
    private static final byte[] PAID = "paid".getBytes(StandardCharsets.UTF_8);
    private static final byte[] OPEN = "open".getBytes(StandardCharsets.UTF_8);
    private static final LakeSchema SCHEMA = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("label", LakeSchema.Type.UTF8)));

    private LakeProbe() {
    }

    public static Timing measure(int rows) {
        if (rows < 1) {
            throw new IllegalArgumentException("the lake probe needs at least one row: " + rows);
        }
        ColumnarRows batch = batch(rows);
        LakeTable.Config config = new LakeTable.Config(1 << 22, 1 << 28, 1 << 18, 1 << 22, 0L, ParquetCodec.SNAPPY);
        Path directory = null;
        try {
            directory = Files.createTempDirectory("nodus-lake-probe");
            try (LakeTable table = LakeTable.open(directory, SCHEMA, config)) {
                long start = System.nanoTime();
                table.upsertColumns(batch);
                long ingest = System.nanoTime() - start;
                start = System.nanoTime();
                Aggregate amounts = table.aggregate(0);
                long sum = System.nanoTime() - start;
                double expected = 3.0 * rows * (rows - 1L) / 2.0;
                if (amounts.count() != rows || amounts.sum() != expected) {
                    throw new IllegalStateException("the lake probe summed " + amounts + ", expected " + expected);
                }
                start = System.nanoTime();
                table.flush();
                long flush = System.nanoTime() - start;
                return new Timing(ingest, sum, flush);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            delete(directory);
        }
    }

    public static void main(String[] arguments) {
        int rows = arguments.length > 0 ? Integer.parseInt(arguments[0]) : DEFAULT_ROWS;
        int runs = arguments.length > 1 ? Integer.parseInt(arguments[1]) : DEFAULT_RUNS;
        int warmups = arguments.length > 2 ? Integer.parseInt(arguments[2]) : DEFAULT_WARMUPS;
        for (int i = 0; i < warmups; i++) {
            measure(rows);
        }
        long[] ingests = new long[runs];
        long[] sums = new long[runs];
        long[] flushes = new long[runs];
        for (int i = 0; i < runs; i++) {
            Timing timing = measure(rows);
            ingests[i] = timing.ingestNanos();
            sums[i] = timing.sumNanos();
            flushes[i] = timing.flushNanos();
        }
        System.out.println("lake_ingest_ns_per_row " + (double) median(ingests) / rows);
        System.out.println("lake_sum_ns_per_row " + (double) median(sums) / rows);
        System.out.println("lake_flush_ns_per_row " + (double) median(flushes) / rows);
    }

    private static ColumnarRows batch(int rows) {
        long[] keys = new long[rows];
        long[] amounts = new long[rows];
        long[] scores = new long[rows];
        int[] statuses = new int[rows];
        int[] offsets = new int[rows + 1];
        byte[] labels = new byte[rows * PAID.length];
        for (int i = 0; i < rows; i++) {
            keys[i] = i;
            amounts[i] = 3L * i;
            scores[i] = Double.doubleToRawLongBits(i / 7.0);
            statuses[i] = i % 3;
            System.arraycopy((i & 1) == 0 ? OPEN : PAID, 0, labels, i * PAID.length, PAID.length);
            offsets[i + 1] = (i + 1) * PAID.length;
        }
        Arena arena = Arena.ofAuto();
        return new ColumnarRows(rows, nativeCopy(arena, keys),
                List.of(nativeCopy(arena, amounts), nativeCopy(arena, scores)),
                List.of(nativeCopy(arena, statuses)),
                List.of(nativeCopy(arena, offsets)),
                List.of(nativeCopy(arena, labels)));
    }

    private static MemorySegment nativeCopy(Arena arena, long[] values) {
        MemorySegment segment = arena.allocate((long) values.length * Long.BYTES, Long.BYTES);
        MemorySegment.copy(values, 0, segment, ValueLayout.JAVA_LONG, 0, values.length);
        return segment;
    }

    private static MemorySegment nativeCopy(Arena arena, int[] values) {
        MemorySegment segment = arena.allocate((long) values.length * Integer.BYTES, Integer.BYTES);
        MemorySegment.copy(values, 0, segment, ValueLayout.JAVA_INT, 0, values.length);
        return segment;
    }

    private static MemorySegment nativeCopy(Arena arena, byte[] values) {
        MemorySegment segment = arena.allocate(Math.max(1, values.length));
        MemorySegment.copy(values, 0, segment, ValueLayout.JAVA_BYTE, 0, values.length);
        return segment.asSlice(0, values.length);
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    private static void delete(Path directory) {
        if (directory == null) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
