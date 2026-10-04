package io.nodusdb.lake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LakeTableTest {

    private static final LakeSchema SCHEMA = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("name", LakeSchema.Type.UTF8)));
    private static final LakeTable.Config SMALL = new LakeTable.Config(2, 1 << 20, 4, 16, 0L, ParquetCodec.SNAPPY);

    @TempDir
    Path directory;

    @Test
    void writesContinueWhileFrozenTableIsPending() throws IOException {
        ManualExecutor flusher = new ManualExecutor();
        LakeTable table = new LakeTable(directory, SCHEMA, SMALL, flusher, null, () -> { });

        put(table, 1L, 10L, 1.5, 1, "frozen-one");
        put(table, 2L, 20L, 2.5, 2, "frozen-two");
        assertEquals(1, flusher.pending());

        put(table, 3L, 30L, 3.5, 3, "active-three");
        assertRow(table.get(1L), 1L, 10L, 1.5, 1, "frozen-one");
        assertRow(table.get(3L), 3L, 30L, 3.5, 3, "active-three");

        flusher.runAll();
        assertRow(table.get(1L), 1L, 10L, 1.5, 1, "frozen-one");
        assertRow(table.get(2L), 2L, 20L, 2.5, 2, "frozen-two");
        assertEquals(1, table.dataFiles().size());
    }

    @Test
    void inPlaceUpdateInActiveShadowsFrozenRow() throws IOException {
        ManualExecutor flusher = new ManualExecutor();
        LakeTable table = new LakeTable(directory, SCHEMA, SMALL, flusher, null, () -> { });
        put(table, 1L, 10L, 1.0, 1, "old");
        put(table, 2L, 20L, 2.0, 2, "filler");
        assertEquals(1, flusher.pending());

        put(table, 1L, 99L, 9.0, 9, "new");

        assertRow(table.get(1L), 1L, 99L, 9.0, 9, "new");
        flusher.runAll();
        assertRow(table.get(1L), 1L, 99L, 9.0, 9, "new");
    }

    @Test
    void explicitFlushEmptiesMemoryAndWritesReadableFile() throws IOException {
        try (LakeTable table = LakeTable.open(directory, SCHEMA, LakeTable.Config.DEFAULT)) {
            put(table, 5L, 50L, 5.5, 5, "five");
            put(table, 6L, 60L, 6.5, 6, "six");

            table.flush();

            List<Path> files = table.dataFiles();
            assertEquals(1, files.size());
            assertTrue(Files.size(files.get(0)) > 0);
            assertRow(table.get(5L), 5L, 50L, 5.5, 5, "five");
            assertRow(table.get(6L), 6L, 60L, 6.5, 6, "six");
        }
    }

    @Test
    void tombstoneHidesCommittedRowBeforeAndAfterReopen() throws IOException {
        try (LakeTable table = LakeTable.open(directory, SCHEMA, LakeTable.Config.DEFAULT)) {
            put(table, 7L, 70L, 7.0, 7, "seven");
            table.flush();
            assertRow(table.get(7L), 7L, 70L, 7.0, 7, "seven");

            table.delete(7L);
            assertFalse(table.get(7L).isPresent());

            table.flush();
            assertFalse(table.get(7L).isPresent());
            assertTrue(hasFileNamed(directory, "delete-"));
        }
        try (LakeTable reopened = LakeTable.open(directory, SCHEMA, LakeTable.Config.DEFAULT)) {
            assertFalse(reopened.get(7L).isPresent());
        }
    }

    @Test
    void deleteOfUnflushedRowIsRecordedAsTombstone() throws IOException {
        try (LakeTable table = LakeTable.open(directory, SCHEMA, LakeTable.Config.DEFAULT)) {
            put(table, 8L, 80L, 8.0, 8, "eight");
            table.delete(8L);
            assertFalse(table.get(8L).isPresent());

            table.flush();

            assertTrue(table.dataFiles().isEmpty());
            assertTrue(hasFileNamed(directory, "delete-"));
        }
    }

    @Test
    void committedRowsSurviveReopen() throws IOException {
        try (LakeTable table = LakeTable.open(directory, SCHEMA, LakeTable.Config.DEFAULT)) {
            put(table, 9L, 90L, 9.25, 9, "nine");
            table.flush();
        }
        try (LakeTable reopened = LakeTable.open(directory, SCHEMA, LakeTable.Config.DEFAULT)) {
            assertRow(reopened.get(9L), 9L, 90L, 9.25, 9, "nine");
        }
    }

    @Test
    void reopenWithDifferentSchemaIsRejected() throws IOException {
        try (LakeTable table = LakeTable.open(directory, SCHEMA, LakeTable.Config.DEFAULT)) {
            put(table, 1L, 1L, 1.0, 1, "x");
            table.flush();
        }
        LakeSchema other = new LakeSchema(List.of(new LakeSchema.Field("amount", LakeSchema.Type.INT64)));

        assertThrows(IllegalStateException.class,
                () -> LakeTable.open(directory, other, LakeTable.Config.DEFAULT));
    }

    @Test
    void automaticFreezeFlushesWithoutExplicitCall() throws IOException {
        ManualExecutor flusher = new ManualExecutor();
        LakeTable table = new LakeTable(directory, SCHEMA, SMALL, flusher, null, () -> { });
        put(table, 1L, 1L, 1.0, 1, "a");
        assertEquals(0, flusher.pending());
        put(table, 2L, 2L, 2.0, 2, "b");
        assertEquals(1, flusher.pending());
        flusher.runAll();

        assertEquals(1, table.dataFiles().size());
        assertRow(table.get(2L), 2L, 2L, 2.0, 2, "b");
    }

    @Test
    void writersBlockOnlyWhenFlushIsTwoBuffersBehind() throws Exception {
        ManualExecutor flusher = new ManualExecutor();
        LakeTable table = new LakeTable(directory, SCHEMA, SMALL, flusher, null, () -> { });
        put(table, 1L, 1L, 1.0, 1, "a");
        put(table, 2L, 2L, 2.0, 2, "b");
        assertEquals(1, flusher.pending());

        Thread writer = new Thread(() -> {
            for (long key = 3; key <= 8; key++) {
                put(table, key, key, key, (int) key, "w" + key);
            }
        });
        writer.start();
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (writer.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(Thread.State.WAITING, writer.getState());

        flusher.runAll();
        writer.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
        assertFalse(writer.isAlive());
        for (long key = 3; key <= 8; key++) {
            assertRow(table.get(key), key, key, key, (int) key, "w" + key);
        }
    }

    @Test
    void periodicFlushCommitsDirtyBufferWithoutExplicitCall() throws Exception {
        LakeTable.Config periodic = new LakeTable.Config(1 << 20, 1 << 26, 16, 64, 20L, ParquetCodec.SNAPPY);
        try (LakeTable table = LakeTable.open(directory, SCHEMA, periodic)) {
            put(table, 3L, 30L, 3.0, 3, "tick");
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (table.dataFiles().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, table.dataFiles().size());
            assertRow(table.get(3L), 3L, 30L, 3.0, 3, "tick");
        }
    }

    @Test
    void negativeFlushIntervalIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new LakeTable.Config(1, 1, 1, 1, -1L, ParquetCodec.SNAPPY));
    }

    private static void put(LakeTable table, long key, long amount, double score, int status, String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        table.upsert(key, new long[] {amount, Double.doubleToRawLongBits(score)},
                new int[] {status}, bytes, new int[] {bytes.length});
    }

    private static void assertRow(Optional<LakeRow> row, long key, long amount, double score, int status,
                                  String name) {
        assertTrue(row.isPresent(), "row " + key + " should be present");
        LakeRow actual = row.get();
        assertEquals(key, actual.keyHash());
        assertEquals(amount, actual.longValues()[0]);
        assertEquals(Double.doubleToRawLongBits(score), actual.longValues()[1]);
        assertEquals(status, actual.intValues()[0]);
        assertEquals(name, new String(actual.varCharValues()[0], StandardCharsets.UTF_8));
    }

    private static boolean hasFileNamed(Path directory, String prefix) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.anyMatch(file -> file.getFileName().toString().startsWith(prefix));
        }
    }
}
