package io.nodusdb.bench;

import io.nodusdb.kernel.IndexedSparseSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class IntersectionBench {

    private static final int LARGE_DEGREE = 65_536;

    @Param({"16", "256", "4096", "65536"})
    public int smallDegree;

    private IndexedSparseSet small;
    private IndexedSparseSet large;
    private long[] out;

    @Setup(Level.Trial)
    public void setup() {
        large = new IndexedSparseSet(LARGE_DEGREE);
        for (long k = 0; k < LARGE_DEGREE; k++) {
            large.add(2 * k);
        }
        small = new IndexedSparseSet(smallDegree);
        for (long k = 0; k < smallDegree; k++) {
            small.add(k % 2 == 0 ? 2 * k : 2 * k + 1);
        }
        out = new long[smallDegree];
    }

    @Benchmark
    public int intersect() {
        return small.intersect(large, out);
    }
}
