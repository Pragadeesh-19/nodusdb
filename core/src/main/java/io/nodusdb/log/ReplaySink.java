package io.nodusdb.log;

import io.nodusdb.log.record.RecordReader;

public interface ReplaySink {

    void apply(RecordReader record);

    void committed(long commitLsn, long commitMicros);
}
