package io.nodusdb.ship;

import io.nodusdb.error.LogBacklogException;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.record.RecordBatch;

public final class ShippingLogStore implements LogStore {

    public interface Gate {

        long shippedLsn();

        String refusal();
    }

    private final LogStore delegate;
    private final Gate gate;

    public ShippingLogStore(LogStore delegate, Gate gate) {
        this.delegate = delegate;
        this.gate = gate;
    }

    @Override
    public long epoch() {
        return delegate.epoch();
    }

    @Override
    public long lastLsn() {
        return delegate.lastLsn();
    }

    @Override
    public long durableLsn() {
        return delegate.durableLsn();
    }

    @Override
    public long lastCommitMicros() {
        return delegate.lastCommitMicros();
    }

    @Override
    public long append(RecordBatch batch) {
        String refusal = gate.refusal();
        if (refusal != null) {
            throw new LogBacklogException(refusal);
        }
        return delegate.append(batch);
    }

    @Override
    public void awaitDurable(long lsn) {
        delegate.awaitDurable(lsn);
    }

    @Override
    public void force() {
        delegate.force();
    }

    @Override
    public long rollSegment() {
        return delegate.rollSegment();
    }

    @Override
    public void trim(long throughLsn) {
        delegate.trim(Math.min(throughLsn, gate.shippedLsn()));
    }

    @Override
    public void close() {
        delegate.close();
    }
}
