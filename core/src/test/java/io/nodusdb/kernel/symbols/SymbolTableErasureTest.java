package io.nodusdb.kernel.symbols;

import io.nodusdb.kernel.memory.MemoryBudget;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolTableErasureTest {

    private static final byte[] SALT = new byte[32];

    private static SymbolTable table() {
        return new SymbolTable(Arena.ofAuto(), MemoryBudget.unlimited());
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static int define(SymbolTable table, String text) {
        byte[] utf8 = bytes(text);
        int id = table.size();
        table.append(id, utf8, 0, utf8.length);
        return id;
    }

    private static int lookup(SymbolTable table, String text) {
        byte[] utf8 = bytes(text);
        return table.lookup(utf8, 0, utf8.length);
    }

    private static byte[] pseudonymOf(String text) {
        return Pseudonym.of(SALT, bytes(text));
    }

    private static boolean slabHolds(SymbolTable table, byte[] needle) throws ReflectiveOperationException {
        Field field = SymbolTable.class.getDeclaredField("slab");
        field.setAccessible(true);
        for (MemorySegment chunk : (MemorySegment[]) field.get(table)) {
            byte[] content = chunk.toArray(ValueLayout.JAVA_BYTE);
            for (int start = 0; start + needle.length <= content.length; start++) {
                if (Arrays.equals(content, start, start + needle.length, needle, 0, needle.length)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Test
    void anErasedNameNoLongerLooksUpAndResolvesToItsPseudonym() {
        SymbolTable table = table();
        define(table, "user:bob");
        int alice = define(table, "user:alice");

        assertTrue(table.erase(alice, pseudonymOf("user:alice")));

        assertEquals(-1, lookup(table, "user:alice"));
        assertArrayEquals(pseudonymOf("user:alice"), table.resolve(alice));
        assertTrue(table.isErased(alice));
        assertFalse(table.isErased(0));
        assertEquals(1, table.erasedCount());
        assertEquals(0, lookup(table, "user:bob"));
    }

    @Test
    void erasingTwiceChangesNothing() {
        SymbolTable table = table();
        int alice = define(table, "user:alice");
        table.erase(alice, pseudonymOf("user:alice"));

        assertFalse(table.erase(alice, pseudonymOf("other")));

        assertArrayEquals(pseudonymOf("user:alice"), table.resolve(alice));
        assertEquals(1, table.erasedCount());
    }

    @Test
    void thePlaintextBytesAreZeroedInMemory() throws ReflectiveOperationException {
        SymbolTable table = table();
        int alice = define(table, "user:alice-the-unique");
        assertTrue(slabHolds(table, bytes("user:alice-the-unique")));

        table.erase(alice, pseudonymOf("user:alice-the-unique"));

        assertFalse(slabHolds(table, bytes("user:alice-the-unique")));
    }

    @Test
    void theSameNameWrittenAgainGetsANewId() {
        SymbolTable table = table();
        int first = define(table, "user:alice");
        table.erase(first, pseudonymOf("user:alice"));

        int second = define(table, "user:alice");

        assertEquals(first + 1, second);
        assertEquals(second, lookup(table, "user:alice"));
        assertTrue(table.isErased(first));
        assertFalse(table.isErased(second));
    }

    @Test
    void thePseudonymItselfDoesNotLookUpTheErasedId() {
        SymbolTable table = table();
        int alice = define(table, "user:alice");
        byte[] pseudonym = pseudonymOf("user:alice");
        table.erase(alice, pseudonym);

        assertEquals(-1, table.lookup(pseudonym, 0, pseudonym.length));
    }

    @Test
    void probeChainsSurviveErasuresAndBucketRebuilds() {
        SymbolTable table = table();
        int names = 5_000;
        for (int i = 0; i < names; i++) {
            define(table, "user:u" + i);
        }
        for (int i = 0; i < names; i += 3) {
            table.erase(i, pseudonymOf("user:u" + i));
        }
        for (int i = names; i < 4 * names; i++) {
            define(table, "user:u" + i);
        }

        for (int i = 0; i < names; i++) {
            if (i % 3 == 0) {
                assertEquals(-1, lookup(table, "user:u" + i), "erased " + i);
                byte[] pseudonym = pseudonymOf("user:u" + i);
                assertEquals(-1, table.lookup(pseudonym, 0, pseudonym.length), "pseudonym " + i);
            } else {
                assertEquals(i, lookup(table, "user:u" + i), "kept " + i);
            }
        }
        for (int i = names; i < 4 * names; i++) {
            assertEquals(i, lookup(table, "user:u" + i));
        }
        assertEquals((names + 2) / 3, table.erasedCount());
    }

    @Test
    void anErasedEntryCanBeRestoredFromASnapshot() {
        SymbolTable table = table();
        define(table, "type");
        byte[] pseudonym = pseudonymOf("user:alice");

        table.appendErased(1, pseudonym);
        int next = define(table, "user:alice");

        assertTrue(table.isErased(1));
        assertArrayEquals(pseudonym, table.resolve(1));
        assertEquals(1, table.erasedCount());
        assertEquals(2, next);
        assertEquals(2, lookup(table, "user:alice"));
        assertThrows(IllegalStateException.class, () -> table.appendErased(1, pseudonym));
    }

    @Test
    void erasingAnUnknownIdIsRefused() {
        SymbolTable table = table();
        define(table, "user:alice");

        assertThrows(IllegalArgumentException.class, () -> table.erase(1, pseudonymOf("x")));
        assertThrows(IllegalArgumentException.class, () -> table.erase(-1, pseudonymOf("x")));
    }

    @Test
    void readersSeeTheNameOrThePseudonymAndNeverATornValue() throws InterruptedException {
        SymbolTable table = table();
        int rounds = 2_000;
        for (int i = 0; i < rounds; i++) {
            define(table, "user:some-rather-long-name-" + i);
        }
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<String> failure = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            while (!done.get() && failure.get() == null) {
                for (int i = 0; i < rounds; i++) {
                    String seen = new String(table.resolve(i), StandardCharsets.UTF_8);
                    if (!seen.equals("user:some-rather-long-name-" + i)
                            && !Arrays.equals(table.resolve(i), pseudonymOf("user:some-rather-long-name-" + i))) {
                        failure.set("id " + i + " resolved to " + seen);
                    }
                }
            }
        });
        reader.start();
        for (int i = 0; i < rounds; i++) {
            table.erase(i, pseudonymOf("user:some-rather-long-name-" + i));
        }
        done.set(true);
        reader.join();

        assertNull(failure.get());
    }
}
