package io.nodusdb.objectstore;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class ForwardingObjectStore implements ObjectStore {

    private final ObjectStore delegate;

    public ForwardingObjectStore(ObjectStore delegate) {
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

    @Override
    public void deleteAll(Collection<String> keys) {
        delegate.deleteAll(keys);
    }

    @Override
    public int abortStaleUploads(String prefix, Duration olderThan, Instant now) {
        return delegate.abortStaleUploads(prefix, olderThan, now);
    }
}
