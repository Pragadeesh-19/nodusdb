package io.nodusdb.replica;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.nio.file.Path;

final class KernelSink implements ReplicaSink {

    final GraphKernel kernel = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);

    @Override
    public long load(Path snapshotFile) throws IOException {
        SnapshotMeta meta = SnapshotReader.load(snapshotFile, kernel);
        kernel.restorePosition(meta.lsn(), meta.lastCommitMicros());
        return meta.lsn();
    }

    @Override
    public void apply(byte[] records, long lsnFirst, long lsnLast) {
        kernel.applyReplicated(records, lsnFirst, lsnLast);
    }
}
