package io.nodusdb.bench;

import io.nodusdb.log.LogStore;
import io.nodusdb.log.VolatileLog;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.ship.ShipState;
import io.nodusdb.ship.ShippingLogStore;

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
public class ShippingGateBench {

    private static final long BACKLOG_CAP_BYTES = 1L << 30;

    private LogStore bare;
    private LogStore gated;
    private RecordBatch batch;

    @Setup(Level.Trial)
    public void setup() {
        bare = new VolatileLog();
        ShipState state = new ShipState(1, 0, 0, BACKLOG_CAP_BYTES);
        state.active();
        gated = new ShippingLogStore(new VolatileLog(), state);
        batch = new RecordBatch();
        batch.tuple(RecordType.TUPLE_ADD, 1, 1, 0, 2);
        batch.commit();
    }

    @Benchmark
    public long appendWithoutShipping() {
        return bare.append(batch);
    }

    @Benchmark
    public long appendThroughTheShippingGate() {
        return gated.append(batch);
    }
}
