package io.nodusdb.storage.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32C;

final class SectionOutput {

    private final FileChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(SnapshotFormat.CHUNK_BYTES)
            .order(ByteOrder.BIG_ENDIAN);
    private final CRC32C crc = new CRC32C();
    private long position;
    private long headerPosition;
    private long bodyBytes;
    private int kind;

    SectionOutput(FileChannel channel, long position) {
        this.channel = channel;
        this.position = position;
    }

    void begin(int sectionKind) throws IOException {
        kind = sectionKind;
        headerPosition = position;
        position += SnapshotFormat.SECTION_HEADER_BYTES;
        crc.reset();
        bodyBytes = 0;
        buffer.clear();
    }

    void putInt(int value) throws IOException {
        ensure(Integer.BYTES);
        buffer.putInt(value);
    }

    void putShort(short value) throws IOException {
        ensure(Short.BYTES);
        buffer.putShort(value);
    }

    void putByte(byte value) throws IOException {
        ensure(Byte.BYTES);
        buffer.put(value);
    }

    void putLong(long value) throws IOException {
        ensure(Long.BYTES);
        buffer.putLong(value);
    }

    void putBytes(byte[] bytes) throws IOException {
        int written = 0;
        while (written < bytes.length) {
            ensure(1);
            int chunk = Math.min(buffer.remaining(), bytes.length - written);
            buffer.put(bytes, written, chunk);
            written += chunk;
        }
    }

    void end() throws IOException {
        flush();
        ByteBuffer header = ByteBuffer.allocate(SnapshotFormat.SECTION_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(kind).putLong(bodyBytes).putInt((int) crc.getValue()).putInt(0).flip();
        writeFully(header, headerPosition);
        position += bodyBytes;
    }

    private void ensure(int bytes) throws IOException {
        if (buffer.remaining() < bytes) {
            flush();
        }
    }

    private void flush() throws IOException {
        buffer.flip();
        int length = buffer.remaining();
        crc.update(buffer.duplicate());
        writeFully(buffer, position + bodyBytes);
        bodyBytes += length;
        buffer.clear();
    }

    private void writeFully(ByteBuffer source, long target) throws IOException {
        long at = target;
        while (source.hasRemaining()) {
            at += channel.write(source, at);
        }
    }
}
