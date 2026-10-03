package io.nodusdb.bench;

import io.nodusdb.kernel.IndexedSparseSet;
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
public class ChurnBench {

    private static final int SIZE = 1024;

    private IndexedSparseSet set;
    private int cursor;

    @Setup(Level.Trial)
    public void setup() {
        set = new IndexedSparseSet();
        for (long k = 0; k < SIZE; k++) {
            set.add(k);
        }
    }

    @Benchmark
    public boolean removeThenAdd() {
        long key = cursor;
        cursor = (cursor + 1) & (SIZE - 1);
        boolean removed = set.remove(key);
        boolean added = set.add(key);
        return removed & added;
    }
}
