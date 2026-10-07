package io.nodusdb.log;

import io.nodusdb.log.record.RecordBatch;

public final class VolatileLog implements LogStore {

    private final long epoch;
    private long lastLsn;

    public VolatileLog(long epoch, long lastLsn) {
        this.epoch = epoch;
        this.lastLsn = lastLsn;
    }

    public VolatileLog() {
        this(1, 0);
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
    public synchronized long append(RecordBatch batch) {
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
    public long rollSegment() {
        return lastLsn();
    }

    @Override
    public void trim(long throughLsn) {
    }

    @Override
    public void close() {
    }
}
