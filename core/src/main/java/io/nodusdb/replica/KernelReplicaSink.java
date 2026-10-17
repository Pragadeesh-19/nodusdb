package io.nodusdb.replica;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public final class KernelReplicaSink implements ReplicaSink {

    private final GraphKernel kernel;
    private SnapshotMeta loaded;

    KernelReplicaSink(long maxMemoryBytes) {
        this(GraphKernel.openReplica(maxMemoryBytes));
    }

    public KernelReplicaSink(GraphKernel kernel) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        if (!kernel.isReplica()) {
            throw new IllegalArgumentException("a replica sink applies records to a replica kernel");
        }
    }

    public GraphKernel kernel() {
        return kernel;
    }

    public SnapshotMeta loaded() {
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
