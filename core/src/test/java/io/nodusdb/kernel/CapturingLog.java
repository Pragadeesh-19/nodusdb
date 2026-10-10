package io.nodusdb.kernel;

import io.nodusdb.log.LogStore;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFixtures;

import java.io.ByteArrayOutputStream;

public final class CapturingLog implements LogStore {

    public record Drained(byte[] records, long first, long last) {

        public boolean isEmpty() {
            return records.length == 0;
        }
    }

    private static final long MICROS_STEP = 1_000;

    private final long epoch;
    private final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    private long lastLsn;
    private long drainedThrough;
    private long micros = RecordFixtures.COMMIT_MICROS;

    public CapturingLog() {
        this(1);
    }

    public CapturingLog(long epoch) {
        this.epoch = epoch;
    }

    public static DurableStorage noStorage() {
        return new DurableStorage() {
            @Override
            public void checkpoint(GraphKernel kernel, LogStore log) {
            }

            @Override
            public void close() {
            }
        };
    }

    public synchronized Drained drain() {
        Drained drained = new Drained(captured.toByteArray(), drainedThrough + 1, lastLsn);
        captured.reset();
        drainedThrough = lastLsn;
        return drained;
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    public synchronized long lastLsn() {
        return lastLsn;
    }

    @Override
    public synchronized long durableLsn() {
        return lastLsn;
    }

    @Override
    public synchronized long lastCommitMicros() {
        return micros;
    }

    @Override
    public synchronized long append(RecordBatch batch) {
        micros += MICROS_STEP;
        batch.seal(lastLsn + 1, micros);
        captured.write(batch.bytes().array(), batch.bytes().arrayOffset(), batch.size());
        lastLsn += batch.count();
        return lastLsn;
    }

    @Override
    public void awaitDurable(long lsn) {
    }

    @Override
    public void force() {
    }

    @Override
    public synchronized long rollSegment() {
        return lastLsn;
    }

    @Override
    public void trim(long throughLsn) {
    }

    @Override
    public void close() {
    }
}
