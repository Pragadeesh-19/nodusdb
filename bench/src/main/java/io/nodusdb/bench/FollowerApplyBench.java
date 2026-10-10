package io.nodusdb.bench;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class FollowerApplyBench {

    private static final int TRANSACTIONS = 2_000;
    private static final int TUPLES_PER_TRANSACTION = 8;
    private static final long FIRST_MICROS = 1_700_000_000_000_000L;

    private byte[] chunk;
    private long lastLsn;
    private GraphKernel replica;

    @Setup(Level.Trial)
    public void buildChunk() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        long lsn = 1;
        int object = 0;
        for (int t = 0; t < TRANSACTIONS; t++) {
            RecordBatch batch = new RecordBatch();
            for (int i = 0; i < TUPLES_PER_TRANSACTION; i++) {
                batch.tuple(RecordType.TUPLE_ADD, object, 0, 0, object + 1);
                object++;
            }
            batch.commit();
            batch.seal(lsn, FIRST_MICROS + t);
            bytes.write(batch.bytes().array(), 0, batch.size());
            lsn += batch.count();
        }
        chunk = bytes.toByteArray();
        lastLsn = lsn - 1;
    }

    @Setup(Level.Invocation)
    public void freshReplica() {
        replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
    }

    @Benchmark
    @OperationsPerInvocation(TRANSACTIONS)
    public long applyTransactionsOnAFollower() {
        replica.applyReplicated(chunk, 1, lastLsn);
        return replica.appliedLsn();
    }
}
