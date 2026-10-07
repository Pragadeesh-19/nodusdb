package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.symbols.StringInterner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SymbolLogTest {

    @TempDir
    Path directory;

    @Test
    void freshLogStartsUnsetAndEmpty() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);

        try (SymbolLog log = SymbolLog.open(file, new StringInterner())) {
            assertEquals(KeyKind.UNSET, log.kind());
        }
        assertEquals(SymbolLog.HEADER_BYTES, Files.size(file));
    }

    @Test
    void appendedStringsReplayToTheSameIds() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);
        StringInterner written = new StringInterner();
        try (SymbolLog log = SymbolLog.open(file, written)) {
            log.setKind(KeyKind.STRING);
            written.intern(bytes("user:alice"), 0, 10);
            written.intern(bytes("role:admin"), 0, 10);
            written.intern(bytes("中文"), 0, bytes("中文").length);
            log.append(written, 0, written.size());
        }

        StringInterner replayed = new StringInterner();
        try (SymbolLog log = SymbolLog.open(file, replayed)) {
            assertEquals(KeyKind.STRING, log.kind());
        }
        assertEquals(3L, replayed.size());
        assertArrayEquals(bytes("user:alice"), replayed.resolve(0L));
        assertArrayEquals(bytes("role:admin"), replayed.resolve(1L));
        assertArrayEquals(bytes("中文"), replayed.resolve(2L));
    }

    @Test
    void groupedAppendsContinueTheIdSequence() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);
        StringInterner strings = new StringInterner();
        try (SymbolLog log = SymbolLog.open(file, strings)) {
            strings.intern(bytes("a"), 0, 1);
            log.append(strings, 0, strings.size());
            long before = strings.size();
            strings.intern(bytes("b"), 0, 1);
            strings.intern(bytes("c"), 0, 1);
            log.append(strings, before, strings.size());
        }

        StringInterner replayed = new StringInterner();
        SymbolLog.open(file, replayed).close();
        assertEquals(3L, replayed.size());
        assertEquals(2L, replayed.lookup(bytes("c"), 0, 1));
    }

    @Test
    void tornTailIsTruncatedAndTheLogKeepsAppending() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);
        writeLog(file, "one", "two");
        long goodLength = Files.size(file);
        byte[] complete = recordBytes("three-that-is-cut-short");
        Files.write(file, Arrays.copyOf(complete, complete.length - 5), StandardOpenOption.APPEND);

        StringInterner replayed = new StringInterner();
        try (SymbolLog log = SymbolLog.open(file, replayed)) {
            assertEquals(2L, replayed.size());
            assertEquals(goodLength, Files.size(file));
            StringInterner next = replayed;
            next.intern(bytes("four"), 0, 4);
            log.append(next, 2, 3);
        }

        StringInterner reopened = new StringInterner();
        SymbolLog.open(file, reopened).close();
        assertEquals(3L, reopened.size());
        assertArrayEquals(bytes("four"), reopened.resolve(2L));
    }

    @Test
    void corruptFinalRecordWithCompleteLengthIsTreatedAsTorn() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);
        writeLog(file, "one", "two");
        flipLastByte(file);

        StringInterner replayed = new StringInterner();
        SymbolLog.open(file, replayed).close();
        assertEquals(1L, replayed.size());
    }

    @Test
    void corruptRecordBeforeTheEndRefusesToOpen() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);
        writeLog(file, "one", "two", "three");
        byte[] bytes = Files.readAllBytes(file);
        bytes[SymbolLog.HEADER_BYTES + SymbolLog.RECORD_HEADER_BYTES] ^= 0x01;
        Files.write(file, bytes);

        assertThrows(IOException.class, () -> SymbolLog.open(file, new StringInterner()));
    }

    @Test
    void kindIsRewrittenInPlace() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);
        try (SymbolLog log = SymbolLog.open(file, new StringInterner())) {
            log.setKind(KeyKind.INTEGER);
        }

        try (SymbolLog log = SymbolLog.open(file, new StringInterner())) {
            assertEquals(KeyKind.INTEGER, log.kind());
        }
    }

    @Test
    void foreignFileIsRejected() throws IOException {
        Path file = directory.resolve(SymbolLog.FILE);
        Files.write(file, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        assertThrows(IOException.class, () -> SymbolLog.open(file, new StringInterner()));
    }

    private void writeLog(Path file, String... values) throws IOException {
        StringInterner strings = new StringInterner();
        try (SymbolLog log = SymbolLog.open(file, strings)) {
            for (String value : values) {
                long before = strings.size();
                strings.intern(bytes(value), 0, bytes(value).length);
                log.append(strings, before, strings.size());
            }
        }
    }

    private static byte[] recordBytes(String value) {
        byte[] payload = bytes(value);
        CRC32C crc = new CRC32C();
        crc.update(payload);
        ByteBuffer record = ByteBuffer.allocate(SymbolLog.RECORD_HEADER_BYTES + payload.length);
        record.putInt(payload.length).putInt((int) crc.getValue()).put(payload);
        return record.array();
    }

    private static void flipLastByte(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(file, bytes);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
