package io.nodusdb.erasure;

import io.nodusdb.io.FileSync;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

public final class ErasureCoverage {

    public record Covered(int erasedCount, long eraseLsn, long firstKeptSeq, long snapshotLsn) {

        public static final Covered NONE = new Covered(0, 0, 0, 0);
    }

    public static final String FILE = "erasure.coverage";

    private static final String TEMP = FILE + ".tmp";
    private static final int MAGIC = 0x4E455243;
    private static final short VERSION = 1;
    private static final int CHECKSUMMED_BYTES = 4 + 2 + 2 + 4 + 8 + 8 + 8;
    private static final int BYTES = CHECKSUMMED_BYTES + 4;

    private final Path directory;
    private volatile Covered covered;

    private ErasureCoverage(Path directory, Covered covered) {
        this.directory = directory;
        this.covered = covered;
    }

    public static ErasureCoverage open(Path directory) {
        return new ErasureCoverage(directory, read(directory.resolve(FILE)));
    }

    public Covered covered() {
        return covered;
    }

    public void record(Covered next) {
        ByteBuffer content = encode(next);
        Path temp = directory.resolve(TEMP);
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (content.hasRemaining()) {
                channel.write(content);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            Files.move(temp, directory.resolve(FILE), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        FileSync.directory(directory);
        covered = next;
    }

    private static ByteBuffer encode(Covered value) {
        ByteBuffer out = ByteBuffer.allocate(BYTES);
        out.putInt(MAGIC).putShort(VERSION).putShort((short) 0).putInt(value.erasedCount())
                .putLong(value.eraseLsn()).putLong(value.firstKeptSeq()).putLong(value.snapshotLsn());
        out.putInt((int) checksum(out.array()));
        return out.flip();
    }

    private static Covered read(Path file) {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (NoSuchFileException absent) {
            return Covered.NONE;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (bytes.length != BYTES) {
            return Covered.NONE;
        }
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt() != MAGIC || in.getShort() != VERSION || in.getShort() != 0) {
            return Covered.NONE;
        }
        Covered value = new Covered(in.getInt(), in.getLong(), in.getLong(), in.getLong());
        if (in.getInt() != (int) checksum(bytes) || value.erasedCount() < 0) {
            return Covered.NONE;
        }
        return value;
    }

    private static long checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, CHECKSUMMED_BYTES);
        return crc.getValue();
    }
}
