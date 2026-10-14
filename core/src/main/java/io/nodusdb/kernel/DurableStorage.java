package io.nodusdb.kernel;

import io.nodusdb.log.LogStore;

public interface DurableStorage {

    void checkpoint(GraphKernel kernel, LogStore log);

    long checkpointLsn();

    byte[] salt();

    void close();
}
