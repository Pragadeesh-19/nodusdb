package io.nodusdb.bench;

import io.nodusdb.lake.DeltaMemTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class DeltaMemTableBench {

    private static final int ROWS = 1_000_000;
    private static final int PAYLOAD_BYTES = 16;
    private static final DeltaMemTable.Schema SCHEMA = new DeltaMemTable.Schema(2, 1, 1);
    private static final long KEY_STRIDE = 0x9E3779B97F4A7C15L;

    private DeltaMemTable table;
    private final long[] longValues = new long[2];
    private final int[] intValues = new int[1];
    private final int[] varCharLengths = {PAYLOAD_BYTES};
    private final byte[] payload = new byte[PAYLOAD_BYTES];
    private long cursor;

    @Setup(Level.Trial)
    public void setup() {
        table = new DeltaMemTable(SCHEMA, 1 << 20, 1 << 25);
        for (long row = 0; row < ROWS; row++) {
            longValues[0] = row;
            table.upsert(keyFor(row), longValues, intValues, payload, varCharLengths);
        }
        cursor = 0;
    }

    @Benchmark
    public boolean sustainedUpsert() {
        long row = cursor;
        cursor = row + 1 == ROWS ? 0 : row + 1;
        longValues[0] = row;
        longValues[1] = Double.doubleToRawLongBits(row * 0.5);
        intValues[0] = (int) row;
        payload[0] = (byte) row;
        return table.upsert(keyFor(row), longValues, intValues, payload, varCharLengths);
    }

    private static long keyFor(long row) {
        return (row + 1) * KEY_STRIDE;
    }
}
