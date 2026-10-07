package io.nodusdb.log;

import io.nodusdb.log.record.RecordBatch;

public interface LogStore extends AutoCloseable {

    long epoch();

    long lastLsn();

    long durableLsn();

    long append(RecordBatch batch);

    void awaitDurable(long lsn);

    void force();

    long rollSegment();

    void trim(long throughLsn);

    @Override
    void close();
}
