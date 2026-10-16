package io.nodusdb.bench;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;

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

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class EraseBench {

    private static final String SCHEMA = """
            schema 1
            type user
            type document {
              relation viewer: user
              permission view = viewer
            }
            """;
    private static final int GRANTS_PER_TRANSACTION = 10_000;

    @Param({"1000", "100000"})
    public int grants;

    private GraphKernel kernel;
    private TupleStore store;

    @Setup(Level.Invocation)
    public void grantOneSubjectEverything() {
        kernel = new GraphKernel();
        store = TupleStore.open(kernel);
        store.applySchema(SCHEMA);
        for (int first = 0; first < grants; first += GRANTS_PER_TRANSACTION) {
            TupleTransaction transaction = new TupleTransaction();
            for (int i = first; i < Math.min(grants, first + GRANTS_PER_TRANSACTION); i++) {
                transaction.add("document:d" + i, "viewer", "user:alice", null);
            }
            store.write(transaction);
        }
    }

    @TearDown(Level.Invocation)
    public void close() {
        kernel.close();
    }

    @Benchmark
    public Token eraseTheSubject() {
        return store.erase("user:alice");
    }
}
