package io.nodusdb.replica;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.nio.file.Path;

final class RestoreSink implements ReplicaSink {

    private final GraphKernel kernel = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
    private SnapshotMeta loaded;

    GraphKernel kernel() {
        return kernel;
    }

    SnapshotMeta loaded() {
        if (loaded == null) {
            throw new IllegalStateException("no snapshot was loaded");
        }
        return loaded;
    }

    @Override
    public long load(Path snapshotFile) throws IOException {
        SnapshotMeta meta = SnapshotReader.load(snapshotFile, kernel);
        kernel.restorePosition(meta.lsn(), meta.lastCommitMicros());
        loaded = meta;
        return meta.lsn();
    }

    @Override
    public void apply(byte[] records, long lsnFirst, long lsnLast) {
        kernel.applyReplicated(records, lsnFirst, lsnLast);
    }
}
