package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.symbols.StringInterner;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

public final class SymbolLog implements AutoCloseable {

    public static final String FILE = "symbols.nodus";

    static final byte[] MAGIC = {'N', 'S', 'Y', 'M'};
    static final int VERSION = 1;
    static final int HEADER_BYTES = 8;
    static final int RECORD_HEADER_BYTES = 8;
    private static final int KIND_OFFSET = 5;
    private static final long MAX_RECORD_BYTES = Integer.MAX_VALUE - 8;

    private final FileChannel channel;
    private long position;
    private KeyKind kind;

    private SymbolLog(FileChannel channel, long position, KeyKind kind) {
        this.channel = channel;
        this.position = position;
        this.kind = kind;
    }

    public static SymbolLog open(Path file, StringInterner interner) throws IOException {
        if (!Files.exists(file)) {
            create(file);
        }
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            long size = channel.size();
            if (size < HEADER_BYTES) {
                throw new IOException("symbol log header is truncated: " + file);
            }
            KeyKind kind = readHeader(channel, file);
            long position = replay(channel, size, interner, file);
            if (position < size) {
                channel.truncate(position);
                channel.force(true);
            }
            return new SymbolLog(channel, position, kind);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    public KeyKind kind() {
        return kind;
    }

    public void setKind(KeyKind newKind) throws IOException {
        ByteBuffer code = ByteBuffer.wrap(new byte[] {(byte) newKind.code()});
        writeFully(channel, code, KIND_OFFSET);
        channel.force(true);
        kind = newKind;
    }

    public void append(StringInterner interner, long fromId, long toId) throws IOException {
        List<byte[]> strings = new ArrayList<>((int) (toId - fromId));
        long total = 0;
        for (long id = fromId; id < toId; id++) {
            byte[] bytes = interner.resolve(id);
            strings.add(bytes);
            total += RECORD_HEADER_BYTES + bytes.length;
        }
        if (total == 0) {
            return;
        }
        ByteBuffer records = ByteBuffer.allocate(Math.toIntExact(total));
        for (byte[] bytes : strings) {
            records.putInt(bytes.length);
            records.putInt((int) checksum(bytes));
            records.put(bytes);
        }
        records.flip();
        writeFully(channel, records, position);
        channel.force(true);
        position += total;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static void create(Path file) throws IOException {
        Path temp = file.resolveSibling(FILE + ".tmp");
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        header.put(MAGIC).put((byte) VERSION).put((byte) KeyKind.UNSET.code()).put(new byte[2]).flip();
        try (FileChannel created = FileChannel.open(temp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            writeFully(created, header, 0);
            created.force(true);
        }
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
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
        return KeyKind.fromCode(header.get() & 0xFF);
    }

    private static long replay(FileChannel channel, long size, StringInterner interner, Path file)
            throws IOException {
        long position = HEADER_BYTES;
        ByteBuffer recordHeader = ByteBuffer.allocate(RECORD_HEADER_BYTES);
        while (position < size) {
            if (size - position < RECORD_HEADER_BYTES) {
                return position;
            }
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
                return position;
            }
            byte[] bytes = new byte[(int) length];
            readFully(channel, ByteBuffer.wrap(bytes), position + RECORD_HEADER_BYTES);
            if (checksum(bytes) != expectedChecksum) {
                if (end == size) {
                    return position;
                }
                throw new IOException("symbol record at offset " + position + " fails its checksum in " + file);
            }
            long expectedId = interner.size();
            if (interner.intern(bytes, 0, bytes.length) != expectedId) {
                throw new IOException("symbol record at offset " + position + " repeats a string in " + file);
            }
            position = end;
        }
        return position;
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

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long at) throws IOException {
        long offset = at;
        while (buffer.hasRemaining()) {
            offset += channel.write(buffer, offset);
        }
    }
}
