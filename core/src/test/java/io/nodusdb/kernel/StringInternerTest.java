package io.nodusdb.kernel;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StringInternerTest {

    private static final int UUID_COUNT = 100_000;

    @Test
    void emptyAndMultiByteStringsRoundTrip() {
        StringInterner strings = new StringInterner();
        byte[] empty = new byte[0];
        byte[] mixed = "naïve-ü-中文-🙂".getBytes(StandardCharsets.UTF_8);

        long emptyId = intern(strings, empty);
        long mixedId = intern(strings, mixed);

        assertEquals(0L, emptyId);
        assertEquals(1L, mixedId);
        assertArrayEquals(empty, strings.resolve(emptyId));
        assertArrayEquals(mixed, strings.resolve(mixedId));
    }

    @Test
    void repeatedStringsKeepTheirIdAndLookupNeverCreates() {
        StringInterner strings = new StringInterner();
        byte[] alice = bytes("user:alice");

        long first = intern(strings, alice);
        long again = intern(strings, alice);

        assertEquals(first, again);
        assertEquals(1L, strings.size());
        assertEquals(-1L, lookup(strings, bytes("user:bob")));
        assertEquals(first, lookup(strings, alice));
        assertEquals(1L, strings.size());
    }

    @Test
    void sliceOfALargerArrayInternsOnlyTheRequestedBytes() {
        StringInterner strings = new StringInterner();
        byte[] buffer = bytes("xxuser:alicezz");

        long id = strings.intern(buffer, 2, 10);

        assertArrayEquals(bytes("user:alice"), strings.resolve(id));
    }

    @Test
    void unknownIdsAreRejected() {
        StringInterner strings = new StringInterner();
        intern(strings, bytes("a"));

        assertThrows(IllegalArgumentException.class, () -> strings.resolve(1L));
        assertThrows(IllegalArgumentException.class, () -> strings.resolve(-1L));
    }

    @Test
    void outOfRangeSliceIsRejected() {
        StringInterner strings = new StringInterner();

        assertThrows(IndexOutOfBoundsException.class, () -> strings.intern(bytes("abc"), 1, 5));
    }

    @Test
    void hundredThousandUuidsMapToDenseIdsAndResolveExactly() {
        StringInterner strings = new StringInterner();
        Random random = new Random(20_260_101L);
        List<String> seen = new ArrayList<>(UUID_COUNT);
        Map<String, Long> expected = new HashMap<>();
        for (int i = 0; i < UUID_COUNT; i++) {
            String uuid = new UUID(random.nextLong(), random.nextLong()).toString();
            seen.add(uuid);
            long id = intern(strings, bytes(uuid));
            Long previous = expected.putIfAbsent(uuid, id);
            if (previous == null) {
                assertEquals(expected.size() - 1L, id, "new string must take the next dense id");
            } else {
                assertEquals(previous.longValue(), id, "repeat of " + uuid);
            }
        }

        assertEquals(expected.size(), strings.size());
        for (Map.Entry<String, Long> entry : expected.entrySet()) {
            assertArrayEquals(bytes(entry.getKey()), strings.resolve(entry.getValue()),
                    "resolve of id " + entry.getValue());
            assertEquals(entry.getValue().longValue(), lookup(strings, bytes(entry.getKey())));
        }
        for (String uuid : seen) {
            assertEquals(expected.get(uuid).longValue(), intern(strings, bytes(uuid)));
        }
    }

    @Test
    void lookupsOfKnownStringsAllocateNothing() {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported());
        StringInterner strings = new StringInterner();
        byte[][] known = new byte[1_000][];
        for (int i = 0; i < known.length; i++) {
            known[i] = bytes("node:" + i);
            intern(strings, known[i]);
        }
        long threadId = Thread.currentThread().threadId();
        long sink = 0;
        for (int round = 0; round < 5_000; round++) {
            for (byte[] value : known) {
                sink += strings.intern(value, 0, value.length);
            }
        }
        long before = bean.getThreadAllocatedBytes(threadId);
        for (int round = 0; round < 5_000; round++) {
            for (byte[] value : known) {
                sink += strings.intern(value, 0, value.length);
            }
        }
        long allocated = bean.getThreadAllocatedBytes(threadId) - before;

        assertEquals(0L, allocated, "bytes allocated by 5 million known-string interns (sink " + sink + ")");
    }

    private static long intern(StringInterner strings, byte[] value) {
        return strings.intern(value, 0, value.length);
    }

    private static long lookup(StringInterner strings, byte[] value) {
        return strings.lookup(value, 0, value.length);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
