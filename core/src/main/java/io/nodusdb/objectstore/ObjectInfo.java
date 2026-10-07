package io.nodusdb.objectstore;

import java.util.Map;

public record ObjectInfo(String key, long size, long lastModifiedMillis, Map<String, String> metadata) {

    public ObjectInfo {
        ObjectKeys.requireKey(key);
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative: " + size);
        }
        metadata = Map.copyOf(metadata);
    }
}
