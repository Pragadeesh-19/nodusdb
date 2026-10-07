package io.nodusdb.objectstore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;

public final class MemoryObjectStore implements ObjectStore {

    public static final int MAX_LIST_KEYS = 1000;
    public static final int RANGE_NOT_SATISFIABLE = 416;

    private record Entry(byte[] content, long lastModifiedMillis, Map<String, String> metadata) {
    }

    private final ConcurrentSkipListMap<String, Entry> objects = new ConcurrentSkipListMap<>();
    private final LongSupplier clockMillis;

    public MemoryObjectStore() {
        this(System::currentTimeMillis);
    }

    public MemoryObjectStore(LongSupplier clockMillis) {
        this.clockMillis = clockMillis;
    }

    @Override
    public PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata) {
        Entry entry = entryOf(key, content, metadata);
        return objects.putIfAbsent(key, entry) == null ? PutResult.CREATED : PutResult.ALREADY_EXISTS;
    }

    @Override
    public void put(String key, byte[] content) {
        objects.put(key, entryOf(key, content, Map.of()));
    }

    @Override
    public void putFile(String key, Path file, Map<String, String> metadata) {
        byte[] content;
        try {
            content = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        objects.put(key, entryOf(key, content, metadata));
    }

    @Override
    public Optional<byte[]> get(String key) {
        ObjectKeys.requireKey(key);
        Entry entry = objects.get(key);
        return entry == null ? Optional.empty() : Optional.of(entry.content().clone());
    }

    @Override
    public Optional<byte[]> getRange(String key, long offset, int length) {
        ObjectKeys.requireKey(key);
        if (offset < 0 || length <= 0) {
            throw new IllegalArgumentException("a range needs a non-negative offset and a positive length");
        }
        Entry entry = objects.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        byte[] content = entry.content();
        if (offset >= content.length) {
            throw new FatalStoreException("range starts at or past the end of the object", RANGE_NOT_SATISFIABLE);
        }
        int from = (int) offset;
        return Optional.of(Arrays.copyOfRange(content, from, (int) Math.min(content.length, offset + length)));
    }

    @Override
    public Optional<ObjectInfo> head(String key) {
        ObjectKeys.requireKey(key);
        Entry entry = objects.get(key);
        return entry == null ? Optional.empty() : Optional.of(infoOf(key, entry));
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) {
        ObjectKeys.requirePrefix(prefix);
        ObjectKeys.requirePrefix(startAfter);
        if (maxKeys < 1 || maxKeys > MAX_LIST_KEYS) {
            throw new IllegalArgumentException("maxKeys must be between 1 and " + MAX_LIST_KEYS);
        }
        boolean afterPrefix = !startAfter.isEmpty() && startAfter.compareTo(prefix) >= 0;
        NavigableMap<String, Entry> candidates = afterPrefix
                ? objects.tailMap(startAfter, false)
                : objects.tailMap(prefix, true);
        List<ObjectInfo> entries = new ArrayList<>();
        boolean truncated = false;
        for (Map.Entry<String, Entry> candidate : candidates.entrySet()) {
            if (!candidate.getKey().startsWith(prefix)) {
                break;
            }
            if (entries.size() == maxKeys) {
                truncated = true;
                break;
            }
            entries.add(listingOf(candidate.getKey(), candidate.getValue()));
        }
        return new ListPage(entries, truncated);
    }

    @Override
    public void delete(String key) {
        ObjectKeys.requireKey(key);
        objects.remove(key);
    }

    public int size() {
        return objects.size();
    }

    private Entry entryOf(String key, byte[] content, Map<String, String> metadata) {
        ObjectKeys.requireKey(key);
        ObjectKeys.requireMetadata(metadata);
        return new Entry(content.clone(), clockMillis.getAsLong(), Map.copyOf(metadata));
    }

    private static ObjectInfo infoOf(String key, Entry entry) {
        return new ObjectInfo(key, entry.content().length, entry.lastModifiedMillis(), entry.metadata());
    }

    private static ObjectInfo listingOf(String key, Entry entry) {
        return ObjectInfo.listing(key, entry.content().length, entry.lastModifiedMillis());
    }
}
