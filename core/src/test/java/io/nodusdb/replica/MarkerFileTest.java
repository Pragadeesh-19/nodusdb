package io.nodusdb.replica;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.error.CorruptLogException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarkerFileTest {

    private static final MarkerPosition POSITION = new MarkerPosition(42, ChainHash.sha256(new byte[] {1, 2, 3}), 3,
            1_234);
    private static final MarkerPosition NEWER = new MarkerPosition(43, ChainHash.sha256(new byte[] {4}), 3, 1_300);

    private static final class SimulatedCrash extends Error {

        private static final long serialVersionUID = 1L;
    }

    @TempDir
    Path root;

    private static byte[] withValidChecksum(byte[] bytes) {
        byte[] copy = bytes.clone();
        CRC32C crc = new CRC32C();
        crc.update(copy, 0, copy.length - Integer.BYTES);
        ByteBuffer.wrap(copy).order(ByteOrder.BIG_ENDIAN).putInt(copy.length - Integer.BYTES, (int) crc.getValue());
        return copy;
    }

    @Test
    void aMissingFileReadsAsNoMarker() throws IOException {
        assertEquals(Optional.empty(), MarkerFile.read(root.resolve("follower.marker")));
    }

    @Test
    void aWrittenPositionReadsBackExactly() throws IOException {
        Path file = root.resolve("follower.marker");

        MarkerFile.write(file, POSITION);

        assertEquals(Optional.of(POSITION), MarkerFile.read(file));
        assertEquals(MarkerFile.BYTES, Files.size(file));
    }

    @Test
    void overwritingReplacesThePositionAndLeavesNoTemporaryFile() throws IOException {
        Path file = root.resolve("follower.marker");
        MarkerFile.write(file, POSITION);

        MarkerFile.write(file, NEWER);

        assertEquals(Optional.of(NEWER), MarkerFile.read(file));
        try (var entries = Files.list(root)) {
            assertEquals(List.of("follower.marker"), entries.map(p -> p.getFileName().toString()).toList());
        }
    }

    @Test
    void aMissingDirectoryIsCreated() throws IOException {
        Path file = root.resolve("state").resolve("deeper").resolve("follower.marker");

        MarkerFile.write(file, POSITION);

        assertEquals(Optional.of(POSITION), MarkerFile.read(file));
    }

    @Test
    void everySingleByteFlipIsRejectedAndNamesTheFile() throws IOException {
        Path file = root.resolve("follower.marker");
        MarkerFile.write(file, POSITION);
        byte[] good = Files.readAllBytes(file);

        for (int index = 0; index < good.length; index++) {
            byte[] damaged = good.clone();
            damaged[index] ^= 0x01;
            Files.write(file, damaged);

            CorruptLogException refused = assertThrows(CorruptLogException.class, () -> MarkerFile.read(file),
                    "byte " + index);

            assertTrue(refused.getMessage().contains(file.toString()), "byte " + index + ": " + refused.getMessage());
        }
    }

    @Test
    void everyTruncationIsRejected() throws IOException {
        Path file = root.resolve("follower.marker");
        MarkerFile.write(file, POSITION);
        byte[] good = Files.readAllBytes(file);

        for (int length = 0; length < good.length; length++) {
            Files.write(file, Arrays.copyOf(good, length));

            assertThrows(CorruptLogException.class, () -> MarkerFile.read(file), "length " + length);
        }
    }

    @Test
    void trailingBytesAreRejected() throws IOException {
        Path file = root.resolve("follower.marker");
        MarkerFile.write(file, POSITION);
        byte[] good = Files.readAllBytes(file);
        byte[] longer = Arrays.copyOf(good, good.length + 1);

        Files.write(file, longer);

        assertThrows(CorruptLogException.class, () -> MarkerFile.read(file));
    }

    @Test
    void aNewerVersionWithAValidChecksumIsRejectedAsUnsupported() throws IOException {
        Path file = root.resolve("follower.marker");
        MarkerFile.write(file, POSITION);
        byte[] bytes = Files.readAllBytes(file);
        bytes[5] = 2;

        Files.write(file, withValidChecksum(bytes));

        CorruptLogException refused = assertThrows(CorruptLogException.class, () -> MarkerFile.read(file));
        assertTrue(refused.getMessage().contains("version"), refused.getMessage());
    }

    @Test
    void aValidChecksumOverNonsenseFieldsIsRejected() throws IOException {
        Path file = root.resolve("follower.marker");
        MarkerFile.write(file, POSITION);
        byte[] bytes = Files.readAllBytes(file);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putLong(8, 0);

        Files.write(file, withValidChecksum(bytes));

        assertThrows(CorruptLogException.class, () -> MarkerFile.read(file));
    }

    @Test
    void aCrashAtEveryStepLeavesTheOldPositionOrTheNewOneNeverAPartialFile() throws IOException {
        for (MarkerStep crashAt : MarkerStep.values()) {
            Path file = root.resolve(crashAt + ".marker");
            MarkerFile.write(file, POSITION);
            List<MarkerStep> seen = new ArrayList<>();

            assertThrows(SimulatedCrash.class, () -> MarkerFile.write(file, NEWER, step -> {
                seen.add(step);
                if (step == crashAt) {
                    throw new SimulatedCrash();
                }
            }), crashAt.toString());

            Optional<MarkerPosition> after = MarkerFile.read(file);
            assertTrue(after.isPresent(), crashAt.toString());
            MarkerPosition expected = crashAt == MarkerStep.RENAMED ? NEWER : POSITION;
            assertEquals(expected, after.get(), crashAt.toString());
        }
    }

    @Test
    void aCrashBeforeTheFirstRenameLeavesNoMarker() throws IOException {
        Path file = root.resolve("first.marker");

        assertThrows(SimulatedCrash.class, () -> MarkerFile.write(file, POSITION, step -> {
            if (step == MarkerStep.TEMP_WRITTEN) {
                throw new SimulatedCrash();
            }
        }));

        assertEquals(Optional.empty(), MarkerFile.read(file));
        assertFalse(Files.exists(file));
    }
}
