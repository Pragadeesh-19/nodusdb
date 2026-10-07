package io.nodusdb.objectstore;

import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionalWriteProbeTest {

    @TempDir
    Path scratch;

    @Test
    void aConformingStorePassesAndKeepsNothing() {
        MemoryObjectStore memory = new MemoryObjectStore();

        ConditionalWriteProbe.verify(memory);

        assertEquals(0, memory.size());
    }

    @Test
    void theDirectoryStorePasses() {
        DirectoryObjectStore directory = new DirectoryObjectStore(scratch.resolve("store"));

        ConditionalWriteProbe.verify(directory);

        assertTrue(directory.list(ConditionalWriteProbe.PREFIX, "", 10).entries().isEmpty());
    }

    @Test
    void aStoreThatIgnoresIfNoneMatchIsRefused() {
        MemoryObjectStore memory = new MemoryObjectStore();
        FaultyObjectStore ignoring = new FaultyObjectStore(memory).ignoreConditionalWrites();

        UnsupportedStoreException refused = assertThrows(UnsupportedStoreException.class,
                () -> ConditionalWriteProbe.verify(ignoring));

        assertTrue(refused.getMessage().contains("ignores If-None-Match"), refused.getMessage());
        assertEquals(0, memory.size());
    }

    @Test
    void aStoreThatAnswersAlreadyExistsButOverwritesIsRefused() {
        MemoryObjectStore memory = new MemoryObjectStore();
        ObjectStore lying = new ForwardingStore(memory) {
            @Override
            public PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata) {
                PutResult result = super.putIfAbsent(key, content, metadata);
                if (result == PutResult.ALREADY_EXISTS) {
                    memory.put(key, content);
                }
                return result;
            }
        };

        UnsupportedStoreException refused = assertThrows(UnsupportedStoreException.class,
                () -> ConditionalWriteProbe.verify(lying));

        assertTrue(refused.getMessage().contains("replaced an existing object"), refused.getMessage());
        assertEquals(0, memory.size());
    }

    @Test
    void aConflictAnswerToASequentialWriteIsRefused() {
        FaultyObjectStore conflicting = new FaultyObjectStore(new MemoryObjectStore())
                .failCall(2, Fault.CONFLICT);

        assertThrows(UnsupportedStoreException.class, () -> ConditionalWriteProbe.verify(conflicting));
    }

    @Test
    void aTransientFailureIsPassedOnNotMistakenForAnUnsupportedStore() {
        MemoryObjectStore memory = new MemoryObjectStore();
        FaultyObjectStore failing = new FaultyObjectStore(memory).failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> ConditionalWriteProbe.verify(failing));
        assertEquals(0, memory.size());
    }

    @Test
    void aFailureAfterTheFirstWriteStillRemovesTheProbeObject() {
        MemoryObjectStore memory = new MemoryObjectStore();
        FaultyObjectStore failing = new FaultyObjectStore(memory).failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_AFTER);

        assertThrows(TransientStoreException.class, () -> ConditionalWriteProbe.verify(failing));

        assertEquals(0, memory.size());
    }

    @Test
    void repeatedProbesDoNotCollide() {
        MemoryObjectStore memory = new MemoryObjectStore();

        ConditionalWriteProbe.verify(memory);
        ConditionalWriteProbe.verify(memory);
        ConditionalWriteProbe.verify(memory);

        assertEquals(0, memory.size());
    }

    private static class ForwardingStore implements ObjectStore {

        private final ObjectStore delegate;

        ForwardingStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata) {
            return delegate.putIfAbsent(key, content, metadata);
        }

        @Override
        public void put(String key, byte[] content) {
            delegate.put(key, content);
        }

        @Override
        public void putFile(String key, Path file, Map<String, String> metadata) {
            delegate.putFile(key, file, metadata);
        }

        @Override
        public Optional<byte[]> get(String key) {
            return delegate.get(key);
        }

        @Override
        public Optional<byte[]> getRange(String key, long offset, int length) {
            return delegate.getRange(key, offset, length);
        }

        @Override
        public Optional<ObjectInfo> head(String key) {
            return delegate.head(key);
        }

        @Override
        public ListPage list(String prefix, String startAfter, int maxKeys) {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(String key) {
            delegate.delete(key);
        }
    }
}
