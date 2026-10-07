package io.nodusdb.storage.legacy;

import io.nodusdb.kernel.KeyKind;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

final class LegacySymbols {

    record Contents(KeyKind kind, List<byte[]> strings) {
    }

    private static final byte[] MAGIC = {'N', 'S', 'Y', 'M'};
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 8;
    private static final int RECORD_HEADER_BYTES = 8;
    private static final long MAX_RECORD_BYTES = Integer.MAX_VALUE - 8;

    private LegacySymbols() {
    }

    static Contents read(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < HEADER_BYTES) {
                throw new IOException("symbol log header is truncated: " + file);
            }
            KeyKind kind = readHeader(channel, file);
            return new Contents(kind, readStrings(channel, size, file));
        }
    }

    private static KeyKind readHeader(FileChannel channel, Path file) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        readFully(channel, header, 0);
        header.flip();
        byte[] magic = new byte[MAGIC.length];
        header.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new IOException("not a symbol log: " + file);
        }
        int version = header.get() & 0xFF;
        if (version != VERSION) {
            throw new IOException("unsupported symbol log version " + version + " in " + file);
        }
        try {
            return KeyKind.fromCode(header.get() & 0xFF);
        } catch (IllegalArgumentException e) {
            throw new IOException("symbol log names an unknown key kind: " + file, e);
        }
    }

    private static List<byte[]> readStrings(FileChannel channel, long size, Path file) throws IOException {
        List<byte[]> strings = new ArrayList<>();
        long position = HEADER_BYTES;
        ByteBuffer recordHeader = ByteBuffer.allocate(RECORD_HEADER_BYTES);
        while (position < size && size - position >= RECORD_HEADER_BYTES) {
            recordHeader.clear();
            readFully(channel, recordHeader, position);
            recordHeader.flip();
            long length = Integer.toUnsignedLong(recordHeader.getInt());
            long expectedChecksum = Integer.toUnsignedLong(recordHeader.getInt());
            long end = position + RECORD_HEADER_BYTES + length;
            if (length > MAX_RECORD_BYTES) {
                throw new IOException("symbol record at offset " + position + " is too long in " + file);
            }
            if (end > size) {
                break;
            }
            byte[] bytes = new byte[(int) length];
            readFully(channel, ByteBuffer.wrap(bytes), position + RECORD_HEADER_BYTES);
            if (checksum(bytes) != expectedChecksum) {
                if (end == size) {
                    break;
                }
                throw new IOException("symbol record at offset " + position + " fails its checksum in " + file);
            }
            strings.add(bytes);
            position = end;
        }
        return strings;
    }

    private static long checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes);
        return crc.getValue();
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long at) throws IOException {
        long offset = at;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, offset);
            if (read < 0) {
                throw new EOFException("symbol log ends at offset " + offset);
            }
            offset += read;
        }
    }
}
