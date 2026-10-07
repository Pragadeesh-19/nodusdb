package io.nodusdb.kernel.symbols;

import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SymbolTableTest {

    private static SymbolTable unlimited() {
        return new SymbolTable(Arena.ofAuto(), MemoryBudget.unlimited());
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static void define(SymbolTable table, int id, String text) {
        byte[] utf8 = bytes(text);
        table.append(id, utf8, 0, utf8.length);
    }

    private static int lookup(SymbolTable table, String text) {
        byte[] utf8 = bytes(text);
        return table.lookup(utf8, 0, utf8.length);
    }

    @Test
    void definedSymbolsResolveBackAndLookUpToTheirIds() {
        SymbolTable table = unlimited();

        define(table, 0, "user:alice");
        define(table, 1, "role:admin");

        assertEquals(2, table.size());
        assertEquals(0, lookup(table, "user:alice"));
        assertEquals(1, lookup(table, "role:admin"));
        assertEquals(-1, lookup(table, "user:bob"));
        assertArrayEquals(bytes("role:admin"), table.resolve(1));
    }

    @Test
    void theEmptyStringAndUnicodeNamesAreOrdinarySymbols() {
        SymbolTable table = unlimited();

        define(table, 0, "");
        define(table, 1, "café ☃ 😀");

        assertEquals(0, lookup(table, ""));
        assertEquals(1, lookup(table, "café ☃ 😀"));
        assertArrayEquals(new byte[0], table.resolve(0));
        assertArrayEquals(bytes("café ☃ 😀"), table.resolve(1));
    }

    @Test
    void idsMustBeDenseAndNamesUnique() {
        SymbolTable table = unlimited();
        define(table, 0, "a");

        assertThrows(IllegalStateException.class, () -> define(table, 2, "b"));
        assertThrows(IllegalStateException.class, () -> define(table, 1, "a"));
        assertThrows(IllegalStateException.class, () -> define(table, 0, "b"));
        assertEquals(1, table.size());
    }

    @Test
    void anUnknownIdIsRejected() {
        SymbolTable table = unlimited();
        define(table, 0, "a");

        assertThrows(IllegalArgumentException.class, () -> table.resolve(1));
        assertThrows(IllegalArgumentException.class, () -> table.resolve(-1));
    }

    @Test
    void aNameLargerThanASlabChunkIsStoredWhole() {
        SymbolTable table = unlimited();
        String large = "x".repeat(3 << 20);

        define(table, 0, "small");
        define(table, 1, large);
        define(table, 2, "after");

        assertEquals(1, lookup(table, large));
        assertEquals(2, lookup(table, "after"));
        assertEquals(large.length(), table.resolve(1).length);
    }

    @Test
    void manyThousandsOfSymbolsSurviveEveryTableGrowth() {
        SymbolTable table = unlimited();
        int total = 200_000;

        for (int i = 0; i < total; i++) {
            define(table, i, "document:" + i);
        }

        assertEquals(total, table.size());
        for (int i = 0; i < total; i += 7) {
            assertEquals(i, lookup(table, "document:" + i));
        }
        assertEquals(-1, lookup(table, "document:" + total));
        assertArrayEquals(bytes("document:123456"), table.resolve(123_456));
    }

    @Test
    void aRandomHistoryAgreesWithAHashMap() {
        SymbolTable table = unlimited();
        Map<String, Integer> oracle = new HashMap<>();
        Random random = new Random(17L);
        for (int step = 0; step < 50_000; step++) {
            String name = "n" + random.nextInt(8_000);
            Integer known = oracle.get(name);
            if (known == null) {
                int id = oracle.size();
                define(table, id, name);
                oracle.put(name, id);
            } else {
                assertEquals(known, lookup(table, name));
            }
        }
        for (Map.Entry<String, Integer> entry : oracle.entrySet()) {
            assertEquals(entry.getValue(), lookup(table, entry.getKey()));
        }
    }

    @Test
    void reservingBeyondTheBudgetFailsAndLeavesTheTableUsable() {
        SymbolTable table = new SymbolTable(Arena.ofAuto(), MemoryBudget.limitedTo(1_500_000));
        define(table, 0, "kept");

        assertThrows(MemoryLimitExceededException.class, () -> table.reserve(10_000_000, 10));
        assertThrows(MemoryLimitExceededException.class, () -> table.reserve(1, 50L << 20));

        assertEquals(1, table.size());
        assertEquals(0, lookup(table, "kept"));
        define(table, 1, "still works");
        assertEquals(1, lookup(table, "still works"));
    }

    @Test
    void readersNeverMissASymbolThatWasAlreadyPublished() throws InterruptedException {
        SymbolTable table = unlimited();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger published = new AtomicInteger(-1);
        AtomicInteger failures = new AtomicInteger();
        Thread[] readers = new Thread[4];
        for (int r = 0; r < readers.length; r++) {
            readers[r] = new Thread(() -> {
                Random random = new Random();
                while (!stop.get()) {
                    int known = published.get();
                    if (known < 0) {
                        continue;
                    }
                    int id = random.nextInt(known + 1);
                    if (lookup(table, "name-" + id) != id) {
                        failures.incrementAndGet();
                    }
                }
            });
            readers[r].start();
        }
        for (int i = 0; i < 100_000; i++) {
            define(table, i, "name-" + i);
            published.set(i);
        }
        stop.set(true);
        for (Thread reader : readers) {
            reader.join();
        }

        assertEquals(0, failures.get());
    }
}
