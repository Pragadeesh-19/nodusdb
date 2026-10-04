package io.nodusdb.lake;

import io.nodusdb.lake.DeltaMemTable.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeltaMemTableTest {

    private static final Schema SCHEMA = new Schema(2, 1, 1);
    private static final int NAME = 0;

    @Test
    void emptyTableLookupsAndDeletesAreAbsent() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);

        assertEquals(0, table.size());
        assertEquals(DeltaMemTable.ABSENT, table.getRow(42L));
        assertFalse(table.delete(42L));
        table.assertInvariant();
    }

    @Test
    void singleRowLifecycleInsertDeleteReinsert() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);

        assertTrue(upsert(table, 7L, 100L, 200L, 3, "alpha"));
        table.assertInvariant();
        assertEquals(0, table.getRow(7L));
        assertEquals(100L, table.longAt(0, 0));
        assertEquals(200L, table.longAt(1, 0));
        assertEquals(3, table.intAt(0, 0));
        assertEquals("alpha", readVarChar(table, NAME, 0));

        assertTrue(table.delete(7L));
        table.assertInvariant();
        assertEquals(DeltaMemTable.ABSENT, table.getRow(7L));
        assertEquals(0, table.size());

        assertTrue(upsert(table, 7L, 1L, 2L, 9, "beta"));
        table.assertInvariant();
        assertEquals(0, table.getRow(7L));
        assertEquals("beta", readVarChar(table, NAME, 0));
    }

    @Test
    void inPlaceOverwriteKeepsRowCountAndChangesColumns() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        upsert(table, 5L, 1L, 2L, 3, "short");

        assertFalse(upsert(table, 5L, 10L, 20L, 30, "a much longer replacement value"));
        table.assertInvariant();

        assertEquals(1, table.size());
        assertEquals(0, table.getRow(5L));
        assertEquals(10L, table.longAt(0, 0));
        assertEquals(20L, table.longAt(1, 0));
        assertEquals(30, table.intAt(0, 0));
        assertEquals("a much longer replacement value", readVarChar(table, NAME, 0));
    }

    @Test
    void overwriteWithShorterValueShrinksLength() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        upsert(table, 5L, 0L, 0L, 0, "long value here");
        upsert(table, 5L, 0L, 0L, 0, "x");

        table.assertInvariant();
        assertEquals(1, table.varCharLength(NAME, 0));
        assertEquals("x", readVarChar(table, NAME, 0));
    }

    @Test
    void deletingFinalRowLeavesEarlierRowsUntouched() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        upsert(table, 1L, 11L, 12L, 1, "one");
        upsert(table, 2L, 21L, 22L, 2, "two");
        upsert(table, 3L, 31L, 32L, 3, "three");

        assertTrue(table.delete(3L));
        table.assertInvariant();

        assertEquals(2, table.size());
        assertEquals(DeltaMemTable.ABSENT, table.getRow(3L));
        assertRow(table, 0, 1L, 11L, 12L, 1, "one");
        assertRow(table, 1, 2L, 21L, 22L, 2, "two");
    }

    @Test
    void deletingEarlierRowMovesEveryColumnWithTheMovedKey() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        upsert(table, 1L, 11L, 12L, 1, "one");
        upsert(table, 2L, 21L, 22L, 2, "two-two");
        upsert(table, 3L, 31L, 32L, 3, "three");

        assertTrue(table.delete(1L));
        table.assertInvariant();

        assertEquals(2, table.size());
        assertEquals(DeltaMemTable.ABSENT, table.getRow(1L));
        assertEquals(0, table.getRow(3L));
        assertEquals(1, table.getRow(2L));
        assertRow(table, 0, 3L, 31L, 32L, 3, "three");
        assertRow(table, 1, 2L, 21L, 22L, 2, "two-two");
    }

    @Test
    void deletingMiddleRowMovesLastRowWithDifferentLengthPayloads() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 8, 256);
        upsert(table, 10L, 1L, 1L, 1, "");
        upsert(table, 20L, 2L, 2L, 2, "middle payload");
        upsert(table, 30L, 3L, 3L, 3, "last");

        assertTrue(table.delete(20L));
        table.assertInvariant();

        assertRow(table, 0, 10L, 1L, 1L, 1, "");
        assertRow(table, 1, 30L, 3L, 3L, 3, "last");
    }

    @Test
    void doubleColumnsRoundTripBitForBit() {
        DeltaMemTable table = new DeltaMemTable(new Schema(1, 0, 0), 4, 16);
        double[] samples = {
            -0.0, Double.NaN, Double.MIN_VALUE, Double.MAX_VALUE,
            Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.5
        };
        for (int i = 0; i < samples.length; i++) {
            table.upsert(i, new long[] {Double.doubleToRawLongBits(samples[i])}, new int[0],
                    new byte[0], new int[0]);
        }
        for (int i = 0; i < samples.length; i++) {
            int row = table.getRow(i);
            assertEquals(Double.doubleToRawLongBits(samples[i]),
                    Double.doubleToRawLongBits(table.doubleAt(0, row)));
        }
        table.assertInvariant();
    }

    @Test
    void capacityExpandsPastInitialAllocation() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 16);

        for (long key = 0; key < 1_000; key++) {
            assertTrue(upsert(table, key * 0x9E3779B97F4A7C15L, key, -key, (int) key, "v" + key));
            table.assertInvariant();
        }

        assertEquals(1_000, table.size());
        for (long key = 0; key < 1_000; key++) {
            int row = table.getRow(key * 0x9E3779B97F4A7C15L);
            assertEquals(key, table.longAt(0, row));
            assertEquals(-key, table.longAt(1, row));
            assertEquals((int) key, table.intAt(0, row));
            assertEquals("v" + key, readVarChar(table, NAME, row));
        }
    }

    @Test
    void repeatedUpdatesDoNotGrowSlabWithoutBound() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        String payload = "x".repeat(64);
        upsert(table, 99L, 0L, 0L, 0, payload);

        for (int i = 0; i < 10_000; i++) {
            upsert(table, 99L, i, i, i, payload);
        }

        table.assertInvariant();
        assertTrue(table.slabCapacity() <= 1_024, "slab capacity " + table.slabCapacity());
        assertEquals(payload, readVarChar(table, NAME, 0));
    }

    @Test
    void deleteAbsentKeyLeavesStateUnchanged() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        upsert(table, 1L, 1L, 1L, 1, "a");

        assertFalse(table.delete(2L));
        table.assertInvariant();
        assertEquals(1, table.size());
        assertEquals("a", readVarChar(table, NAME, 0));
    }

    @Test
    void schemaWithoutVarCharColumnsAcceptsEmptyPayloads() {
        DeltaMemTable table = new DeltaMemTable(new Schema(1, 1, 0), 2, 1);

        assertTrue(table.upsert(5L, new long[] {8L}, new int[] {13}, new byte[0], new int[0]));
        table.assertInvariant();
        assertEquals(8L, table.longAt(0, 0));
        assertEquals(13, table.intAt(0, 0));
    }

    @Test
    void copyVarCharWritesIntoDestinationAtOffset() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        upsert(table, 1L, 0L, 0L, 0, "hello");

        byte[] destination = new byte[8];
        int copied = table.copyVarChar(NAME, 0, destination, 2);

        assertEquals(5, copied);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(destination, 2, 7));
    }

    @Test
    void rejectsArityMismatchWithoutMutating() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);

        assertThrows(IllegalArgumentException.class,
                () -> table.upsert(1L, new long[] {1L}, new int[] {1}, new byte[0], new int[] {0}));
        assertThrows(IllegalArgumentException.class,
                () -> table.upsert(1L, new long[] {1L, 2L}, new int[] {1, 2}, new byte[0], new int[] {0}));
        assertEquals(0, table.size());
        table.assertInvariant();
    }

    @Test
    void rejectsVarCharLengthsExceedingSuppliedBytesWithoutMutating() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);

        assertThrows(IllegalArgumentException.class,
                () -> table.upsert(1L, new long[2], new int[1], new byte[2], new int[] {5}));
        assertThrows(IllegalArgumentException.class,
                () -> table.upsert(1L, new long[2], new int[1], new byte[2], new int[] {-1}));
        assertEquals(0, table.size());
        table.assertInvariant();
    }

    @Test
    void rejectsInvalidConstructorArguments() {
        assertThrows(IllegalArgumentException.class, () -> new DeltaMemTable(SCHEMA, 3, 64));
        assertThrows(IllegalArgumentException.class, () -> new DeltaMemTable(SCHEMA, 0, 64));
        assertThrows(IllegalArgumentException.class, () -> new DeltaMemTable(SCHEMA, 4, 48));
        assertThrows(IllegalArgumentException.class, () -> new Schema(-1, 0, 0));
    }

    @Test
    void readingOutOfRangeRowIsRejected() {
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 64);
        upsert(table, 1L, 1L, 1L, 1, "a");

        assertThrows(IndexOutOfBoundsException.class, () -> table.longAt(0, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> table.keyHashAt(-1));
    }

    private static boolean upsert(DeltaMemTable table, long key, long a, long b, int status, String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return table.upsert(key, new long[] {a, b}, new int[] {status}, bytes, new int[] {bytes.length});
    }

    private static void assertRow(DeltaMemTable table, int row, long key, long a, long b, int status, String name) {
        assertEquals(key, table.keyHashAt(row));
        assertEquals(a, table.longAt(0, row));
        assertEquals(b, table.longAt(1, row));
        assertEquals(status, table.intAt(0, row));
        assertEquals(name, readVarChar(table, NAME, row));
    }

    private static String readVarChar(DeltaMemTable table, int column, int row) {
        byte[] buffer = new byte[table.varCharLength(column, row)];
        table.copyVarChar(column, row, buffer, 0);
        return new String(buffer, StandardCharsets.UTF_8);
    }
}
