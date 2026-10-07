package io.nodusdb.objectstore;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

public final class ConditionalWriteProbe {

    public static final String PREFIX = "_nodus/probe/";

    private static final byte[] FIRST = "first".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SECOND = "second".getBytes(StandardCharsets.US_ASCII);

    private ConditionalWriteProbe() {
    }

    public static void verify(ObjectStore store) {
        String key = PREFIX + UUID.randomUUID();
        try {
            requireResult(store.putIfAbsent(key, FIRST), PutResult.CREATED,
                    "the first create of a new probe object did not report creation");
            PutResult second = store.putIfAbsent(key, SECOND);
            if (second == PutResult.CREATED) {
                throw new UnsupportedStoreException("the store ignores If-None-Match: a second create of the same "
                        + "key reported creation, so a writer cannot be fenced");
            }
            requireResult(second, PutResult.ALREADY_EXISTS,
                    "the second create of the same key did not report that it already exists");
            byte[] stored = store.get(key).orElseThrow(() -> new UnsupportedStoreException(
                    "the probe object disappeared after it was created"));
            if (!Arrays.equals(stored, FIRST)) {
                throw new UnsupportedStoreException("the store replaced an existing object although the write was "
                        + "conditional on its absence");
            }
        } finally {
            removeQuietly(store, key);
        }
    }

    private static void requireResult(PutResult actual, PutResult expected, String problem) {
        if (actual != expected) {
            throw new UnsupportedStoreException(problem + " (got " + actual + ")");
        }
    }

    private static void removeQuietly(ObjectStore store, String key) {
        try {
            store.delete(key);
        } catch (ObjectStoreException ignored) {
            return;
        }
    }
}
