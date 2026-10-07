package io.nodusdb.storage.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.zip.CRC32C;

record SnapshotHeader(long lsn, long epoch, long lastCommitMicros, int nodeCapacity) {

    ByteBuffer encode() {
        ByteBuffer header = ByteBuffer.allocate(SnapshotFormat.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(SnapshotFormat.MAGIC).putShort(SnapshotFormat.VERSION).putShort((short) 0)
                .putLong(lsn).putLong(epoch).putLong(lastCommitMicros)
                .putInt(nodeCapacity).putInt(SnapshotFormat.SECTION_COUNT);
        header.putInt((int) checksum(header)).putInt(0).flip();
        return header;
    }

    static SnapshotHeader read(FileChannel channel, long size, Path source) throws IOException {
        if (size < SnapshotFormat.HEADER_BYTES) {
            throw new IOException("snapshot is truncated: " + source);
        }
        ByteBuffer bytes = ByteBuffer.allocate(SnapshotFormat.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        while (bytes.hasRemaining()) {
            if (channel.read(bytes, bytes.position()) < 0) {
                throw new IOException("snapshot is truncated: " + source);
            }
        }
        if (bytes.getInt(0) != SnapshotFormat.MAGIC) {
            throw new IOException("not a nodus snapshot: " + source);
        }
        if (bytes.getShort(4) != SnapshotFormat.VERSION) {
            throw new IOException("unsupported snapshot version " + bytes.getShort(4) + " in " + source);
        }
        if (bytes.getShort(6) != 0) {
            throw new IOException("snapshot uses flags this version does not know: " + source);
        }
        if ((int) checksum(bytes) != bytes.getInt(SnapshotFormat.HEADER_CHECKSUMMED_BYTES)) {
            throw new IOException("snapshot header checksum mismatch: " + source);
        }
        if (bytes.getInt(36) != SnapshotFormat.SECTION_COUNT || bytes.getInt(32) < 0) {
            throw new IOException("snapshot header is inconsistent: " + source);
        }
        return new SnapshotHeader(bytes.getLong(8), bytes.getLong(16), bytes.getLong(24), bytes.getInt(32));
    }

    private static long checksum(ByteBuffer bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes.array(), 0, SnapshotFormat.HEADER_CHECKSUMMED_BYTES);
        return crc.getValue();
    }
}
