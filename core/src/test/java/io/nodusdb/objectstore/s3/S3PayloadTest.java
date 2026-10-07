package io.nodusdb.objectstore.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3PayloadTest {

    private static final String EMPTY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    @TempDir
    Path scratch;

    private byte[] fileOf(int size) throws IOException {
        byte[] content = new byte[size];
        new Random(size).nextBytes(content);
        Files.write(scratch.resolve("data.bin"), content);
        return content;
    }

    @Test
    void bytesReportTheirLengthAndHash() {
        S3Payload.Bytes payload = new S3Payload.Bytes("abc".getBytes());

        assertEquals(3, payload.length());
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", payload.sha256Hex());
        assertEquals(3, payload.publisher().contentLength());
    }

    @Test
    void anEmptyPayloadHasTheWellKnownHashAndNoBody() {
        assertEquals(EMPTY_HASH, S3Payload.Bytes.EMPTY.sha256Hex());
        assertEquals(0, S3Payload.Bytes.EMPTY.publisher().contentLength());
        assertEquals(EMPTY_HASH, new S3Payload.FileSlice(scratch.resolve("none"), 0, 0).sha256Hex());
    }

    @Test
    void aSliceHashesExactlyItsBytes() throws IOException {
        byte[] content = fileOf(10_000);
        Path file = scratch.resolve("data.bin");

        S3Payload.FileSlice middle = new S3Payload.FileSlice(file, 1_234, 4_321);

        assertEquals(SigV4Signer.sha256Hex(Arrays.copyOfRange(content, 1_234, 1_234 + 4_321)), middle.sha256Hex());
        assertEquals(4_321, middle.publisher().contentLength());
    }

    @Test
    void theWholeFileAndTheLastByteAreValidSlices() throws IOException {
        byte[] content = fileOf(5_000);
        Path file = scratch.resolve("data.bin");

        assertEquals(SigV4Signer.sha256Hex(content), new S3Payload.FileSlice(file, 0, 5_000).sha256Hex());
        assertEquals(SigV4Signer.sha256Hex(new byte[]{content[4_999]}),
                new S3Payload.FileSlice(file, 4_999, 1).sha256Hex());
    }

    @Test
    void aSliceStreamDeliversTheBytesInAnyReadSizeAndThenEnds() throws IOException {
        byte[] content = fileOf(3_000);
        try (FileChannel channel = FileChannel.open(scratch.resolve("data.bin"), StandardOpenOption.READ);
             InputStream stream = new SliceInputStream(channel, 100, 1_000)) {
            byte[] first = new byte[7];
            assertEquals(7, stream.read(first));
            assertArrayEquals(Arrays.copyOfRange(content, 100, 107), first);
            assertEquals(content[107] & 0xFF, stream.read());
            assertEquals(0, stream.read(new byte[4], 0, 0));
            byte[] rest = stream.readAllBytes();
            assertEquals(1_000 - 8, rest.length);
            assertArrayEquals(Arrays.copyOfRange(content, 108, 1_100), rest);
            assertEquals(-1, stream.read());
            assertEquals(-1, stream.read(new byte[4]));
        }
    }

    @Test
    void aSliceLongerThanTheFileFailsWhenItIsRead() throws IOException {
        fileOf(100);
        S3Payload.FileSlice tooLong = new S3Payload.FileSlice(scratch.resolve("data.bin"), 50, 200);

        assertThrows(UncheckedIOException.class, tooLong::sha256Hex);
    }

    @Test
    void aMissingFileFailsWhenOpened() {
        S3Payload.FileSlice missing = new S3Payload.FileSlice(scratch.resolve("absent.bin"), 0, 10);

        assertThrows(UncheckedIOException.class, missing::sha256Hex);
        assertThrows(UncheckedIOException.class, missing::open);
    }

    @Test
    void aSliceNeedsNonNegativeBounds() {
        assertThrows(IllegalArgumentException.class, () -> new S3Payload.FileSlice(scratch, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new S3Payload.FileSlice(scratch, 0, -1));
    }
}
