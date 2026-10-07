package io.nodusdb.lake.buffer;

import io.nodusdb.lake.buffer.DeltaMemTable.Schema;
import io.nodusdb.lake.UpsertArrays;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class DeltaMemTableOracleTest {

    private static final int OPERATIONS = 100_000;
    private static final int KEYSPACE = 512;
    private static final int FULL_CHECK_INTERVAL = 1_000;
    private static final int MAX_NAME_BYTES = 24;
    private static final Schema SCHEMA = new Schema(2, 1, 1);

    private record Expected(long first, long second, int status, byte[] name) {
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L})
    void mixedOperationsAgreeWithHashMap(long seed) {
        Random random = new Random(seed);
        long[] keys = new long[KEYSPACE];
        for (int i = 0; i < KEYSPACE; i++) {
            keys[i] = random.nextLong();
        }
        DeltaMemTable table = new DeltaMemTable(SCHEMA, 4, 16);
        Map<Long, Expected> oracle = new HashMap<>();

        for (int op = 0; op < OPERATIONS; op++) {
            long key = keys[random.nextInt(KEYSPACE)];
            String where = "seed=" + seed + " op=" + op + " key=" + key;
            int roll = random.nextInt(100);

            if (roll < 55) {
                Expected value = randomValue(random);
                boolean inserted = UpsertArrays.upsert(table, key, new long[] {value.first(), value.second()},
                        new int[] {value.status()}, value.name(), new int[] {value.name().length});
                boolean expectedInsert = oracle.put(key, value) == null;
                assertEquals(expectedInsert, inserted, where);
            } else if (roll < 85) {
                boolean expected = oracle.remove(key) != null;
                assertEquals(expected, table.delete(key), where);
            } else {
                assertEquals(oracle.containsKey(key), table.getRow(key) != DeltaMemTable.ABSENT, where);
            }

            table.assertInvariant();
            assertEquals(oracle.size(), table.size(), where);
            verifyKey(table, oracle, key, where);
            if (op % FULL_CHECK_INTERVAL == 0) {
                verifyAll(table, oracle, "seed=" + seed + " op=" + op);
            }
        }
        verifyAll(table, oracle, "seed=" + seed + " final");
    }

    private static Expected randomValue(Random random) {
        byte[] name = new byte[random.nextInt(MAX_NAME_BYTES + 1)];
        random.nextBytes(name);
        return new Expected(random.nextLong(), random.nextLong(), random.nextInt(), name);
    }

    private static void verifyKey(DeltaMemTable table, Map<Long, Expected> oracle, long key, String where) {
        Expected expected = oracle.get(key);
        int row = table.getRow(key);
        if (expected == null) {
            assertEquals(DeltaMemTable.ABSENT, row, where);
            return;
        }
        assertRowMatches(table, row, key, expected, where);
    }

    private static void verifyAll(DeltaMemTable table, Map<Long, Expected> oracle, String where) {
        assertEquals(oracle.size(), table.size(), where);
        for (Map.Entry<Long, Expected> entry : oracle.entrySet()) {
            long key = entry.getKey();
            int row = table.getRow(key);
            assertNotEquals(DeltaMemTable.ABSENT, row, where + " key=" + key + " missing");
            assertRowMatches(table, row, key, entry.getValue(), where + " key=" + key);
        }
    }

    private static void assertRowMatches(DeltaMemTable table, int row, long key, Expected expected, String where) {
        assertEquals(key, table.keyHashAt(row), where);
        assertEquals(expected.first(), table.longAt(0, row), where);
        assertEquals(expected.second(), table.longAt(1, row), where);
        assertEquals(expected.status(), table.intAt(0, row), where);
        byte[] actual = new byte[table.varCharLength(0, row)];
        table.copyVarChar(0, row, actual, 0);
        assertArrayEquals(expected.name(), actual, where);
    }
}
