package io.nodusdb.storage;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.io.FileSync;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

final class DirectoryFormat {

    enum Layout {
        NEW,
        LEGACY,
        CURRENT,
        INTERRUPTED_UPGRADE
    }

    static final int CURRENT_VERSION = 2;
    static final int LEGACY_VERSION = 1;
    static final int TRIPWIRE_BYTES = 16;

    private static final String FORMAT_PREFIX = "nodus-format ";
    private static final int TRIPWIRE_MAGIC = 0x4E4F4455;
    private static final int NO_TRIPWIRE = 0;

    private DirectoryFormat() {
    }

    static Layout detect(Path directory) throws IOException {
        Path format = directory.resolve(GraphFiles.FORMAT);
        if (Files.isRegularFile(format)) {
            return requireKnown(readFormat(format));
        }
        if (Files.isDirectory(directory.resolve(GraphFiles.BACKUP_DIRECTORY))
                || Files.isDirectory(directory.resolve(GraphFiles.STAGING_DIRECTORY))) {
            return Layout.INTERRUPTED_UPGRADE;
        }
        int tripwire = readTripwireVersion(directory.resolve(GraphFiles.TRIPWIRE));
        if (tripwire == CURRENT_VERSION) {
            return Layout.NEW;
        }
        if (tripwire == LEGACY_VERSION || hasLegacyFiles(directory)) {
            return Layout.LEGACY;
        }
        if (tripwire != NO_TRIPWIRE) {
            throw new UnsupportedFeatureException("log version " + tripwire + " needs a newer nodusdb");
        }
        return Layout.NEW;
    }

    static void writeFormat(Path directory) throws IOException {
        Path temp = directory.resolve(GraphFiles.FORMAT + ".tmp");
        byte[] content = (FORMAT_PREFIX + CURRENT_VERSION + "\n").getBytes(StandardCharsets.US_ASCII);
        writeDurably(temp, ByteBuffer.wrap(content));
        Files.move(temp, directory.resolve(GraphFiles.FORMAT),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        FileSync.directory(directory);
    }

    static void writeTripwire(Path directory) throws IOException {
        Path temp = directory.resolve(GraphFiles.TRIPWIRE_TEMP);
        ByteBuffer header = ByteBuffer.allocate(TRIPWIRE_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(TRIPWIRE_MAGIC).putShort((short) CURRENT_VERSION).putShort((short) 0)
                .putLong(System.currentTimeMillis()).flip();
        writeDurably(temp, header);
        Files.move(temp, directory.resolve(GraphFiles.TRIPWIRE),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        FileSync.directory(directory);
    }

    static int readTripwireVersion(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            return NO_TRIPWIRE;
        }
        ByteBuffer header = ByteBuffer.allocate(TRIPWIRE_BYTES).order(ByteOrder.BIG_ENDIAN);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (header.hasRemaining() && channel.read(header, header.position()) > 0) {
                continue;
            }
        }
        if (header.hasRemaining() || header.getInt(0) != TRIPWIRE_MAGIC) {
            throw new IOException("not a nodus log: " + file);
        }
        return header.getShort(4);
    }

    private static Layout requireKnown(int version) {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedFeatureException("graph directory format " + version + " needs a newer nodusdb");
        }
        return Layout.CURRENT;
    }

    private static int readFormat(Path format) throws IOException {
        String text = new String(Files.readAllBytes(format), StandardCharsets.US_ASCII).trim();
        if (!text.startsWith(FORMAT_PREFIX)) {
            throw new IOException("unreadable format marker: " + format);
        }
        try {
            return Integer.parseInt(text.substring(FORMAT_PREFIX.length()));
        } catch (NumberFormatException e) {
            throw new IOException("unreadable format marker: " + format, e);
        }
    }

    private static boolean hasLegacyFiles(Path directory) {
        return Files.isRegularFile(directory.resolve(GraphFiles.SNAPSHOT))
                || Files.isRegularFile(directory.resolve(GraphFiles.LEGACY_SYMBOLS));
    }

    private static void writeDurably(Path file, ByteBuffer content) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (content.hasRemaining()) {
                channel.write(content);
            }
            channel.force(true);
        }
    }
}
