package io.nodusdb.replica;

import java.io.IOException;
import java.nio.file.Path;

public interface ReplicaSink {

    long load(Path snapshotFile) throws IOException;

    void apply(byte[] records, long lsnFirst, long lsnLast);
}
