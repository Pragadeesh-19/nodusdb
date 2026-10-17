package io.nodusdb.storage.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class SnapshotHeaders {

    public static final int BYTES = SnapshotFormat.HEADER_BYTES;

    public record Position(long lsn, long epoch, long lastCommitMicros) {
    }

    private SnapshotHeaders() {
    }

    public static Position parse(byte[] header, String source) throws IOException {
        SnapshotHeader parsed = SnapshotHeader.parse(ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN), source);
        return new Position(parsed.lsn(), parsed.epoch(), parsed.lastCommitMicros());
    }
}
