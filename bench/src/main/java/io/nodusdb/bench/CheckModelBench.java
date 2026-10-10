package io.nodusdb.bench;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.kernel.GraphKernel;

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
public class CheckModelBench {

    private static final String SCHEMA = """
            schema 1
            type user
            type group {
              relation member: user | group#member
            }
            type folder {
              relation viewer: user | group#member
              relation parent: folder
              permission view = viewer + parent->view
            }
            type document {
              relation parent: folder
              relation editor: user | group#member
              relation viewer: user | group#member
              permission view = viewer + editor + parent->view
            }
            """;

    private static final int DEPTH = 6;

    private TupleStore store;

    @Setup(Level.Trial)
    public void setup() {
        store = TupleStore.open(new GraphKernel());
        store.applySchema(SCHEMA);
        store.add("group:eng", "member", "user:alice");
        store.add("folder:f0", "viewer", "group:eng", "member");
        for (int level = 1; level < DEPTH; level++) {
            store.add("folder:f" + level, "parent", "folder:f" + (level - 1));
        }
        store.add("document:spec", "parent", "folder:f" + (DEPTH - 1));
        store.add("document:spec", "editor", "user:carol");
    }

    @Benchmark
    public boolean checkThroughSixFolders() {
        return store.check("document:spec", "view", "user:alice");
    }

    @Benchmark
    public boolean checkDirectRelation() {
        return store.check("document:spec", "editor", "user:carol");
    }

    @Benchmark
    public boolean checkDenied() {
        return store.check("document:spec", "view", "user:bob");
    }
}
