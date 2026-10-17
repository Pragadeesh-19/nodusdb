package io.nodusdb.kernel.changes;

public interface EntrySink {

    void entry(long lsn, long meta, long payload);
}
