package io.nodusdb.objectstore;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemoryObjectStoreTest extends ObjectStoreContractTest {

    @Override
    ObjectStore create() {
        return new MemoryObjectStore();
    }

    @Test
    void modificationTimesFollowTheInjectedClock() {
        AtomicLong clock = new AtomicLong(1_000);
        MemoryObjectStore memory = new MemoryObjectStore(clock::get);

        memory.put("a/one", bytes("1"));
        clock.set(5_000);
        memory.put("a/two", bytes("2"));
        memory.putIfAbsent("a/three", bytes("3"));

        assertEquals(1_000, memory.head("a/one").orElseThrow().lastModifiedMillis());
        assertEquals(5_000, memory.head("a/two").orElseThrow().lastModifiedMillis());
        assertEquals(5_000, memory.head("a/three").orElseThrow().lastModifiedMillis());
    }

    @Test
    void aLostCreateDoesNotRefreshTheModificationTime() {
        AtomicLong clock = new AtomicLong(1_000);
        MemoryObjectStore memory = new MemoryObjectStore(clock::get);
        memory.putIfAbsent("a/one", bytes("1"));

        clock.set(9_000);
        memory.putIfAbsent("a/one", bytes("2"));

        assertEquals(1_000, memory.head("a/one").orElseThrow().lastModifiedMillis());
    }

    @Test
    void theSizeCountsEveryObject() {
        MemoryObjectStore memory = new MemoryObjectStore();
        memory.put("a/one", bytes("1"));
        memory.put("a/two", bytes("2"));
        memory.delete("a/one");

        assertEquals(1, memory.size());
    }

    @Test
    void aKeyMayBeBothAnObjectAndAPrefixLikeOnObjectStorage() {
        MemoryObjectStore memory = new MemoryObjectStore();

        memory.put("a", bytes("1"));
        memory.put("a/b", bytes("2"));

        assertEquals(2, memory.list("", "", 10).entries().size());
    }
}
