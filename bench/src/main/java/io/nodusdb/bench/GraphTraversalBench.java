package io.nodusdb.bench;

import io.nodusdb.kernel.GraphKernel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class GraphTraversalBench {

    private static final int NODES = 100_000;
    private static final int OUT_DEGREE = 8;
    private static final int STARTS = 1024;
    private static final int OUT_CAPACITY = 1 << 14;
    private static final int DEPTH = 3;

    private GraphKernel kernel;
    private long[] out;
    private long[] starts;
    private long[] partners;
    private int cursor;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(7L);
        kernel = new GraphKernel();
        for (int u = 0; u < NODES; u++) {
            for (int e = 0; e < OUT_DEGREE; e++) {
                kernel.addEdge(u, random.nextInt(NODES));
            }
        }
        out = new long[OUT_CAPACITY];
        starts = new long[STARTS];
        partners = new long[STARTS];
        for (int i = 0; i < STARTS; i++) {
            starts[i] = random.nextInt(NODES);
            partners[i] = random.nextInt(NODES);
        }
    }

    @Benchmark
    public int kHopDepthThree() {
        long start = starts[cursor];
        cursor = (cursor + 1) & (STARTS - 1);
        return kernel.kHop(start, DEPTH, out);
    }

    @Benchmark
    public int commonNeighbors() {
        int i = cursor;
        cursor = (cursor + 1) & (STARTS - 1);
        return kernel.commonNeighbors(starts[i], partners[i], out);
    }
}
