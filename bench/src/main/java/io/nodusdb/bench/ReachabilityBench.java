package io.nodusdb.bench;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.closure.ClosureLimits;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ReachabilityBench {

    private static final String SCHEMA = """
            schema 1
            type user
            type group {
              relation member: user | group#member
            }
            """;

    private static final int CHAIN = 30;
    private static final int FAN_OUT = 10;
    private static final int TREE_LEVELS = 3;
    private static final int UNBOUNDED_DEPTH = 10_000;
    private static final long SETTLE_NANOS = TimeUnit.SECONDS.toNanos(60);

    @Param({"true", "false"})
    public boolean indexed;

    private TupleStore store;
    private String deepestLeaf;

    @Setup(Level.Trial)
    public void setup() {
        ClosureLimits limits = indexed ? ClosureLimits.DEFAULT : ClosureLimits.DEFAULT.disabled();
        store = TupleStore.open(new GraphKernel(GraphKernel.NO_MEMORY_LIMIT, limits), UNBOUNDED_DEPTH);
        store.applySchema(SCHEMA);
        TupleTransaction graph = new TupleTransaction();
        for (int i = 0; i < CHAIN; i++) {
            graph.add("group:c" + i, "member", "group:c" + (i + 1), "member");
        }
        graph.add("group:c" + CHAIN, "member", "user:alice");
        deepestLeaf = addTree(graph, "group:t", 0);
        graph.add(deepestLeaf, "member", "user:alice");
        graph.add("group:elsewhere", "member", "user:bob");
        store.write(graph);
        settle();
    }

    @TearDown(Level.Trial)
    public void close() {
        store.kernel().close();
    }

    private static String addTree(TupleTransaction graph, String parent, int level) {
        if (level == TREE_LEVELS) {
            return parent;
        }
        String last = parent;
        for (int child = 0; child < FAN_OUT; child++) {
            String name = parent + "_" + child;
            graph.add(parent, "member", name, "member");
            last = addTree(graph, name, level + 1);
        }
        return last;
    }

    private void settle() {
        long deadline = System.nanoTime() + SETTLE_NANOS;
        store.check("group:c0", "member", "user:alice");
        while (store.kernel().closureStats().building()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("the index never settled: " + store.kernel().closureStats());
            }
            Thread.onSpinWait();
        }
    }

    @Benchmark
    public boolean grantThroughThirtyNestedGroups() {
        return store.check("group:c0", "member", "user:alice");
    }

    @Benchmark
    public boolean denyThroughThirtyNestedGroups() {
        return store.check("group:c0", "member", "user:bob");
    }

    @Benchmark
    public boolean grantInATreeOfAThousandGroups() {
        return store.check("group:t", "member", "user:alice");
    }

    @Benchmark
    public boolean denyInATreeOfAThousandGroups() {
        return store.check("group:t", "member", "user:bob");
    }

    @Benchmark
    public void nestAndUnnestALeafGroup() {
        store.add(deepestLeaf, "member", "group:elsewhere", "member");
        store.remove(deepestLeaf, "member", "group:elsewhere", "member");
    }

    @Benchmark
    public void addAndRemoveADirectMember() {
        store.add("group:elsewhere", "member", "user:carol");
        store.remove("group:elsewhere", "member", "user:carol");
    }
}
