package io.nodusdb.objectstore;

import io.nodusdb.io.FileSync;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public final class DirectoryObjectStore implements ObjectStore {

    private static final String TEMPORARY_DIRECTORY = ".tmp";
    private static final String SIDECAR_DIRECTORY = ".meta";
    private static final long STALE_TEMPORARY_MILLIS = 60L * 60L * 1000L;
    private static final int RANGE_NOT_SATISFIABLE = 416;
    private static final int REPLACE_ATTEMPTS = 200;
    private static final long REPLACE_BACKOFF_NANOS = 5_000_000L;

    private record Child(String sortKey, String name, Path path, boolean directory) {
    }

    private final class Page {

        private final String prefix;
        private final String startAfter;
        private final int maxKeys;
        private final List<ObjectInfo> entries = new ArrayList<>();
        private boolean truncated;

        Page(String prefix, String startAfter, int maxKeys) {
            this.prefix = prefix;
            this.startAfter = startAfter;
            this.maxKeys = maxKeys;
        }

        boolean done() {
            return truncated;
        }

        boolean mayHold(String subtree) {
            boolean overlapsPrefix = subtree.startsWith(prefix) || prefix.startsWith(subtree);
            boolean pastEverything = !startAfter.isEmpty() && startAfter.compareTo(subtree) > 0
                    && !startAfter.startsWith(subtree);
            return overlapsPrefix && !pastEverything;
        }

        void offer(String key, Path file) throws IOException {
            if (!key.startsWith(prefix) || (!startAfter.isEmpty() && key.compareTo(startAfter) <= 0)) {
                return;
            }
            if (entries.size() == maxKeys) {
                truncated = true;
                return;
            }
            try {
                entries.add(new ObjectInfo(key, Files.size(file), Files.getLastModifiedTime(file).toMillis(),
                        readMetadata(key)));
            } catch (NoSuchFileException vanished) {
                return;
            }
        }

        ListPage result() {
            return new ListPage(entries, truncated);
        }
    }

    private final Path root;
    private final Path temporary;
    private final Path sidecars;
    private final LongSupplier clockMillis;

    public DirectoryObjectStore(Path root) {
        this(root, null);
    }

    public DirectoryObjectStore(Path root, LongSupplier clockMillis) {
        this.root = root.toAbsolutePath().normalize();
        this.temporary = this.root.resolve(TEMPORARY_DIRECTORY);
        this.sidecars = this.root.resolve(SIDECAR_DIRECTORY);
        this.clockMillis = clockMillis;
        try {
            Files.createDirectories(temporary);
            Files.createDirectories(sidecars);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        sweepStaleTemporaries();
    }

    @Override
    public PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata) {
        validate(key, metadata);
        Path target = pathOf(key);
        Path staged = null;
        try {
            requireNotDirectory(key, target);
            if (Files.exists(target)) {
                return PutResult.ALREADY_EXISTS;
            }
            staged = stage(content);
            makeParent(target);
            try {
                Files.createLink(target, staged);
            } catch (FileAlreadyExistsException exists) {
                requireNotDirectory(key, target);
                return PutResult.ALREADY_EXISTS;
            }
            writeMetadata(key, metadata);
            FileSync.directory(target.getParent());
            return PutResult.CREATED;
        } catch (UnsupportedOperationException noLinks) {
            throw new FatalStoreException("the directory store needs a file system with hard links", 0, noLinks);
        } catch (IOException e) {
            throw new TransientStoreException("directory store write failed: " + e.getMessage(), 0, e);
        } finally {
            discard(staged);
        }
    }

    @Override
    public void put(String key, byte[] content) {
        validate(key, Map.of());
        Path staged = null;
        try {
            staged = stage(content);
            replace(key, staged, Map.of());
        } catch (IOException e) {
            throw new TransientStoreException("directory store write failed: " + e.getMessage(), 0, e);
        } finally {
            discard(staged);
        }
    }

    @Override
    public void putFile(String key, Path file, Map<String, String> metadata) {
        validate(key, metadata);
        Path staged = temporary.resolve(UUID.randomUUID().toString());
        try {
            Files.copy(file, staged, StandardCopyOption.REPLACE_EXISTING);
            seal(staged);
            replace(key, staged, metadata);
        } catch (IOException e) {
            throw new TransientStoreException("directory store write failed: " + e.getMessage(), 0, e);
        } finally {
            discard(staged);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        ObjectKeys.requireKey(key);
        Path path = pathOf(key);
        try {
            return Files.isRegularFile(path) ? Optional.of(Files.readAllBytes(path)) : Optional.empty();
        } catch (NoSuchFileException vanished) {
            return Optional.empty();
        } catch (IOException e) {
            throw new TransientStoreException("directory store read failed: " + e.getMessage(), 0, e);
        }
    }

    @Override
    public Optional<byte[]> getRange(String key, long offset, int length) {
        ObjectKeys.requireKey(key);
        if (offset < 0 || length <= 0) {
            throw new IllegalArgumentException("a range needs a non-negative offset and a positive length");
        }
        Path path = pathOf(key);
        if (!Files.isRegularFile(path)) {
            return Optional.empty();
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = channel.size();
            if (offset >= size) {
                throw new FatalStoreException("range starts at or past the end of the object",
                        RANGE_NOT_SATISFIABLE);
            }
            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(length, size - offset));
            readFully(channel, buffer, offset);
            return Optional.of(buffer.array());
        } catch (NoSuchFileException vanished) {
            return Optional.empty();
        } catch (IOException e) {
            throw new TransientStoreException("directory store read failed: " + e.getMessage(), 0, e);
        }
    }

    @Override
    public Optional<ObjectInfo> head(String key) {
        ObjectKeys.requireKey(key);
        Path path = pathOf(key);
        try {
            if (!Files.isRegularFile(path)) {
                return Optional.empty();
            }
            return Optional.of(new ObjectInfo(key, Files.size(path), Files.getLastModifiedTime(path).toMillis(),
                    readMetadata(key)));
        } catch (NoSuchFileException vanished) {
            return Optional.empty();
        } catch (IOException e) {
            throw new TransientStoreException("directory store read failed: " + e.getMessage(), 0, e);
        }
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) {
        ObjectKeys.requirePrefix(prefix);
        ObjectKeys.requirePrefix(startAfter);
        if (maxKeys < 1 || maxKeys > MemoryObjectStore.MAX_LIST_KEYS) {
            throw new IllegalArgumentException("maxKeys must be between 1 and " + MemoryObjectStore.MAX_LIST_KEYS);
        }
        Page page = new Page(prefix, startAfter, maxKeys);
        try {
            walk(root, "", page);
        } catch (IOException e) {
            throw new TransientStoreException("directory store listing failed: " + e.getMessage(), 0, e);
        }
        return page.result();
    }

    @Override
    public void delete(String key) {
        ObjectKeys.requireKey(key);
        try {
            Files.deleteIfExists(pathOf(key));
            Files.deleteIfExists(sidecars.resolve(key));
        } catch (IOException e) {
            throw new TransientStoreException("directory store delete failed: " + e.getMessage(), 0, e);
        }
    }

    private void walk(Path directory, String relative, Page page) throws IOException {
        for (Child child : children(directory)) {
            if (page.done()) {
                return;
            }
            if (child.directory()) {
                String subtree = relative + child.name() + "/";
                if (page.mayHold(subtree)) {
                    walk(child.path(), subtree, page);
                }
            } else {
                page.offer(relative + child.name(), child.path());
            }
        }
    }

    private List<Child> children(Path directory) throws IOException {
        List<Child> children = new ArrayList<>();
        try (Stream<Path> entries = Files.list(directory)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                String name = entry.getFileName().toString();
                if (name.startsWith(".")) {
                    continue;
                }
                boolean isDirectory = Files.isDirectory(entry);
                children.add(new Child(isDirectory ? name + "/" : name, name, entry, isDirectory));
            }
        } catch (NoSuchFileException vanished) {
            return List.of();
        }
        children.sort(Comparator.comparing(Child::sortKey));
        return children;
    }

    private void replace(String key, Path staged, Map<String, String> metadata) throws IOException {
        Path target = pathOf(key);
        requireNotDirectory(key, target);
        makeParent(target);
        moveOver(staged, target);
        if (metadata.isEmpty()) {
            Files.deleteIfExists(sidecars.resolve(key));
        } else {
            writeMetadata(key, metadata);
        }
        FileSync.directory(target.getParent());
    }

    private static void moveOver(Path staged, Path target) throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                return;
            } catch (AccessDeniedException held) {
                if (attempt == REPLACE_ATTEMPTS) {
                    throw held;
                }
                LockSupport.parkNanos(REPLACE_BACKOFF_NANOS);
            }
        }
    }

    private Path stage(byte[] content) throws IOException {
        Path staged = temporary.resolve(UUID.randomUUID().toString());
        try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        stamp(staged);
        return staged;
    }

    private void seal(Path staged) throws IOException {
        try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        stamp(staged);
    }

    private void stamp(Path staged) throws IOException {
        if (clockMillis != null) {
            Files.setLastModifiedTime(staged, FileTime.fromMillis(clockMillis.getAsLong()));
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, offset + buffer.position());
            if (read < 0) {
                return;
            }
        }
    }

    private static void requireNotDirectory(String key, Path target) {
        if (Files.isDirectory(target)) {
            throw new FatalStoreException("the key names an existing directory: " + key, 0);
        }
    }

    private void makeParent(Path target) throws IOException {
        try {
            Files.createDirectories(target.getParent());
        } catch (FileAlreadyExistsException blocked) {
            throw new FatalStoreException("a key conflicts with an existing object: " + root.relativize(target), 0,
                    blocked);
        }
    }

    private void writeMetadata(String key, Map<String, String> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        StringBuilder text = new StringBuilder();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            text.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }
        Path sidecar = sidecars.resolve(key);
        Files.createDirectories(sidecar.getParent());
        Path staged = temporary.resolve(UUID.randomUUID().toString());
        try {
            Files.writeString(staged, text, StandardCharsets.US_ASCII);
            moveOver(staged, sidecar);
        } finally {
            discard(staged);
        }
    }

    private Map<String, String> readMetadata(String key) throws IOException {
        Path sidecar = sidecars.resolve(key);
        if (!Files.isRegularFile(sidecar)) {
            return Map.of();
        }
        Map<String, String> entries = new LinkedHashMap<>();
        try {
            for (String line : Files.readAllLines(sidecar, StandardCharsets.US_ASCII)) {
                int separator = line.indexOf('=');
                if (separator > 0) {
                    entries.put(line.substring(0, separator), line.substring(separator + 1));
                }
            }
        } catch (NoSuchFileException vanished) {
            return Map.of();
        }
        return entries;
    }

    private Path pathOf(String key) {
        Path path = root.resolve(key).normalize();
        if (!path.startsWith(root)) {
            throw new IllegalArgumentException("the key leaves the store: " + key);
        }
        return path;
    }

    private void sweepStaleTemporaries() {
        long cutoff = System.currentTimeMillis() - STALE_TEMPORARY_MILLIS;
        try (Stream<Path> entries = Files.list(temporary)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                if (Files.getLastModifiedTime(entry).toMillis() < cutoff) {
                    Files.deleteIfExists(entry);
                }
            }
        } catch (IOException e) {
            return;
        }
    }

    private static void discard(Path staged) {
        if (staged == null) {
            return;
        }
        try {
            Files.deleteIfExists(staged);
        } catch (IOException e) {
            return;
        }
    }

    private static void validate(String key, Map<String, String> metadata) {
        ObjectKeys.requireKey(key);
        ObjectKeys.requireMetadata(metadata);
    }
}
