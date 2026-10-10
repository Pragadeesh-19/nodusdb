package io.nodusdb.replica;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.error.CorruptLogException;
import io.nodusdb.io.FileSync;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

final class MarkerFile {

    static final int BYTES = 68;

    private static final int MAGIC = 0x4E4F4446;
    private static final short VERSION = 1;
    private static final int SEQ_OFFSET = 8;
    private static final int DIGEST_OFFSET = 16;
    private static final int EPOCH_OFFSET = 48;
    private static final int LSN_OFFSET = 56;
    private static final int CHECKSUM_OFFSET = 64;
    private static final String TEMP_SUFFIX = ".tmp";

    private MarkerFile() {
    }

    static Optional<MarkerPosition> read(Path file) throws IOException {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length != BYTES) {
            throw corrupt(file, "it holds " + bytes.length + " bytes instead of " + BYTES);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (buffer.getInt(CHECKSUM_OFFSET) != checksum(bytes)) {
            throw corrupt(file, "its checksum does not match");
        }
        if (buffer.getInt(0) != MAGIC) {
            throw corrupt(file, "it is not a follower marker");
        }
        if (buffer.getShort(4) != VERSION) {
            throw corrupt(file, "its version " + buffer.getShort(4) + " needs a different nodusdb");
        }
        return Optional.of(decode(file, buffer));
    }

    static void write(Path file, MarkerPosition position) throws IOException {
        write(file, position, step -> { });
    }

    static void write(Path file, MarkerPosition position, Consumer<MarkerStep> observer) throws IOException {
        Path directory = file.toAbsolutePath().getParent();
        Files.createDirectories(directory);
        Path temp = directory.resolve(file.getFileName() + TEMP_SUFFIX);
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer content = ByteBuffer.wrap(encode(position));
            while (content.hasRemaining()) {
                channel.write(content);
            }
            channel.force(true);
        }
        observer.accept(MarkerStep.TEMP_WRITTEN);
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        FileSync.directory(directory);
        observer.accept(MarkerStep.RENAMED);
    }

    private static byte[] encode(MarkerPosition position) {
        byte[] bytes = new byte[BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(0, MAGIC);
        buffer.putShort(4, VERSION);
        buffer.putLong(SEQ_OFFSET, position.seq());
        buffer.put(DIGEST_OFFSET, position.digest().toBytes());
        buffer.putLong(EPOCH_OFFSET, position.epoch());
        buffer.putLong(LSN_OFFSET, position.lsn());
        buffer.putInt(CHECKSUM_OFFSET, checksum(bytes));
        return bytes;
    }

    private static MarkerPosition decode(Path file, ByteBuffer buffer) {
        byte[] digest = new byte[ChainHash.BYTES];
        buffer.get(DIGEST_OFFSET, digest);
        try {
            return new MarkerPosition(buffer.getLong(SEQ_OFFSET), ChainHash.of(digest), buffer.getLong(EPOCH_OFFSET),
                    buffer.getLong(LSN_OFFSET));
        } catch (IllegalArgumentException invalid) {
            throw corrupt(file, invalid.getMessage());
        }
    }

    private static int checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, CHECKSUM_OFFSET);
        return (int) crc.getValue();
    }

    private static CorruptLogException corrupt(Path file, String reason) {
        return new CorruptLogException("the follower marker " + file + " is damaged: " + reason
                + "; delete the file to accept the loss of rollback protection");
    }
}
