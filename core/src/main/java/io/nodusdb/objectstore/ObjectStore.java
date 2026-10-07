package io.nodusdb.objectstore;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public interface ObjectStore {

    PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata);

    default PutResult putIfAbsent(String key, byte[] content) {
        return putIfAbsent(key, content, Map.of());
    }

    void put(String key, byte[] content);

    void putFile(String key, Path file, Map<String, String> metadata);

    Optional<byte[]> get(String key);

    Optional<byte[]> getRange(String key, long offset, int length);

    Optional<ObjectInfo> head(String key);

    ListPage list(String prefix, String startAfter, int maxKeys);

    void delete(String key);

    default void deleteAll(Collection<String> keys) {
        for (String key : keys) {
            delete(key);
        }
    }

    default boolean exists(String key) {
        return head(key).isPresent();
    }
}
