package io.nodusdb.watch;

import io.nodusdb.kernel.changes.ChangeBuffer;
import io.nodusdb.kernel.changes.ChangeExtractor;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.RecordStream;
import io.nodusdb.log.record.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;

final class FallbackCursor implements AutoCloseable {

    static final int BATCH_BYTES = 1 << 20;

    private final int sourceIndex;
    private final String sourceName;
    private final RecordStream stream;
    private final RecordReader reader = new RecordReader();
    private byte[] pending;
    private ByteBuffer view;
    private int offset;
    private long position;
    private long lastUsedNanos;

    FallbackCursor(int sourceIndex, String sourceName, RecordStream stream, long afterLsn) {
        this.sourceIndex = sourceIndex;
        this.sourceName = sourceName;
        this.stream = stream;
        this.position = afterLsn;
    }

    int sourceIndex() {
        return sourceIndex;
    }

    String sourceName() {
        return sourceName;
    }

    long position() {
        return position;
    }

    long lastUsedNanos() {
        return lastUsedNanos;
    }

    void touch(long nanos) {
        lastUsedNanos = nanos;
    }

    void fill(long boundLsn, int maxChanges, ChangeBuffer out) throws IOException {
        int kept = out.size();
        int keptOffset = offset;
        while (true) {
            if (pending == null || offset >= pending.length) {
                if (out.size() != kept) {
                    throw new IllegalStateException("a batch from the " + sourceName + " ended inside a transaction");
                }
                LogTailReader.Batch batch = stream.next(BATCH_BYTES);
                if (batch == null) {
                    return;
                }
                pending = batch.records();
                view = ByteBuffer.wrap(pending);
                offset = 0;
                keptOffset = 0;
            }
            reader.wrap(view, offset, pending.length);
            boolean commitPoint = ChangeExtractor.extractSealed(reader, out);
            long lsn = reader.lsn();
            reader.advance();
            offset = reader.position();
            if (!commitPoint) {
                continue;
            }
            if (lsn <= position) {
                out.truncate(kept);
                keptOffset = offset;
                continue;
            }
            if (lsn > boundLsn) {
                out.truncate(kept);
                offset = keptOffset;
                return;
            }
            kept = out.size();
            keptOffset = offset;
            position = lsn;
            if (out.changes() >= maxChanges) {
                return;
            }
        }
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
