package io.nodusdb.replica;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

final class KernelSink implements ReplicaSink {

    final GraphKernel kernel = new GraphKernel();

    @Override
    public long load(Path snapshotFile) throws IOException {
        return SnapshotReader.load(snapshotFile, kernel).lsn();
    }

    @Override
    public void apply(byte[] records, long lsnFirst, long lsnLast) {
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(records), 0, records.length);
        while (reader.hasRecord()) {
            kernel.replay(reader);
            reader.advance();
        }
    }
}
