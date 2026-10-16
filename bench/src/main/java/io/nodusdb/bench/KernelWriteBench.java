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
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class KernelWriteBench {

    private static final int NODES = 1024;

    private GraphKernel kernel;
    private int cursor;

    @Setup(Level.Trial)
    public void setup() {
        kernel = new GraphKernel();
        for (int node = 0; node < NODES; node++) {
            kernel.addEdge(node, (node + 1) % NODES);
        }
    }

    @TearDown(Level.Trial)
    public void close() {
        kernel.close();
    }

    @Benchmark
    public boolean removeThenAddAnEdge() {
        int node = cursor;
        cursor = (cursor + 1) & (NODES - 1);
        boolean removed = kernel.removeEdge(node, (node + 1) % NODES);
        boolean added = kernel.addEdge(node, (node + 1) % NODES);
        return removed & added;
    }
}
