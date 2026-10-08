package io.nodusdb.objectstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectoryObjectStoreTest extends ObjectStoreContractTest {

    private static final String[] DIRECTORIES = {"d", "d-b", "d.b", "d0", "d1", "d10", "d_x"};
    private static final String[] FILES = {"f", "f-b", "f.b", "f0", "f1", "f10", "f_x"};
    private static final long ONE_HOUR_MILLIS = 60L * 60L * 1000L;

    private Path root() {
        return scratch.resolve("store");
    }

    @Override
    protected ObjectStore create() {
        return new DirectoryObjectStore(root());
    }

    private List<String> stagedFiles() throws IOException {
        try (Stream<Path> staged = Files.list(root().resolve(".tmp"))) {
            return staged.map(path -> path.getFileName().toString()).toList();
        }
    }

    @Test
    void writesWorkAgainAfterTheRootWasRemovedAndComesBack() throws IOException {
        store.put("kept/before", bytes("1"));
        try (Stream<Path> walk = Files.walk(root())) {
            for (Path entry : (Iterable<Path>) walk.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(entry);
            }
        }
        Path file = scratch.resolve("upload.bin");
        Files.write(file, bytes("3"));

        assertEquals(PutResult.CREATED, store.putIfAbsent("kept/after", bytes("2"), Map.of("note", "x")));
        store.putFile("kept/file", file, Map.of("note", "y"));
        store.put("kept/plain", bytes("4"));

        assertArrayEquals(bytes("2"), store.get("kept/after").orElseThrow());
        assertArrayEquals(bytes("3"), store.get("kept/file").orElseThrow());
        assertEquals("y", store.head("kept/file").orElseThrow().metadata().get("note"));
        assertEquals(Optional.empty(), store.get("kept/before"));
        assertEquals(List.of(), stagedFiles());
    }

    @Test
    void aRootThatIsAFileFailsWritesAsTransientAndRecoversWhenItIsReplacedByADirectory() throws IOException {
        Path blocker = root();
        try (Stream<Path> walk = Files.walk(blocker)) {
            for (Path entry : (Iterable<Path>) walk.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(entry);
            }
        }
        Files.writeString(blocker, "not a directory");

        assertThrows(TransientStoreException.class, () -> store.put("a", bytes("1")));
        Files.delete(blocker);

        store.put("a", bytes("1"));
        assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
    }

    @Test
    void noStagedFileRemainsAfterSuccessfulAndLostWrites() throws IOException {
        store.putIfAbsent("a/b", bytes("1"));
        store.putIfAbsent("a/b", bytes("2"));
        store.put("a/b", bytes("3"));
        Path file = scratch.resolve("upload.bin");
        Files.write(file, bytes("4"));
        store.putFile("a/c", file, Map.of("note", "x"));
        store.delete("a/c");

        assertEquals(List.of(), stagedFiles());
    }

    @Test
    void noStagedFileRemainsAfterARefusedWrite() throws IOException {
        store.put("a/b", bytes("1"));

        assertThrows(FatalStoreException.class, () -> store.putIfAbsent("a/b/c", bytes("2")));
        assertThrows(FatalStoreException.class, () -> store.put("a", bytes("3")));

        assertEquals(List.of(), stagedFiles());
    }

    @Test
    void staleStagedFilesAreRemovedWhenTheStoreOpens() throws IOException {
        Path old = root().resolve(".tmp").resolve("old");
        Path fresh = root().resolve(".tmp").resolve("fresh");
        Files.write(old, bytes("crashed"));
        Files.write(fresh, bytes("in flight"));
        Files.setLastModifiedTime(old, FileTime.fromMillis(System.currentTimeMillis() - 2 * ONE_HOUR_MILLIS));

        new DirectoryObjectStore(root());

        assertFalse(Files.exists(old));
        assertTrue(Files.exists(fresh));
    }

    @Test
    void aPathThatIsAnObjectCannotAlsoBeAFolder() {
        store.put("a/b", bytes("1"));

        assertThrows(FatalStoreException.class, () -> store.putIfAbsent("a/b/c", bytes("2")));
        assertThrows(FatalStoreException.class, () -> store.put("a/b/c", bytes("2")));
        assertThrows(FatalStoreException.class, () -> store.putIfAbsent("a", bytes("3")));
        assertThrows(FatalStoreException.class, () -> store.put("a", bytes("3")));
        assertEquals(Optional.empty(), store.get("a"));
        assertEquals(Optional.empty(), store.head("a"));
        assertArrayEquals(bytes("1"), store.get("a/b").orElseThrow());
    }

    @Test
    void theReservedFoldersAreNeverListed() throws IOException {
        store.put("a/b", bytes("1"));
        Files.write(root().resolve(".tmp").resolve("junk"), bytes("x"));
        Files.createDirectories(root().resolve(".other"));
        Files.write(root().resolve(".other").resolve("hidden"), bytes("x"));

        assertEquals(List.of("a/b"), keys(store.list("", "", 100)));
    }

    @Test
    void twoStoresOnOneDirectoryShareObjectsAndRaceSafely() throws Exception {
        DirectoryObjectStore other = new DirectoryObjectStore(root());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int key = 0; key < 50; key++) {
                String name = "shared/" + key;
                CountDownLatch go = new CountDownLatch(1);
                Future<PutResult> first = pool.submit(() -> {
                    go.await();
                    return store.putIfAbsent(name, bytes("first"));
                });
                Future<PutResult> second = pool.submit(() -> {
                    go.await();
                    return other.putIfAbsent(name, bytes("second"));
                });
                go.countDown();
                List<PutResult> results = List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));

                assertEquals(1, results.stream().filter(result -> result == PutResult.CREATED).count(), name);
                assertEquals(store.get(name).orElseThrow().length, other.get(name).orElseThrow().length);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void modificationTimesFollowTheInjectedClock() {
        long stamp = 1_700_000_000_000L;
        DirectoryObjectStore stamped = new DirectoryObjectStore(scratch.resolve("stamped"), () -> stamp);

        stamped.put("a/one", bytes("1"));
        stamped.putIfAbsent("a/two", bytes("2"));

        assertTrue(Math.abs(stamped.head("a/one").orElseThrow().lastModifiedMillis() - stamp) < 2_000);
        assertTrue(Math.abs(stamped.head("a/two").orElseThrow().lastModifiedMillis() - stamp) < 2_000);
    }

    @Test
    void aStoreReopenedOnTheSameDirectoryKeepsContentAndMetadata() {
        store.putIfAbsent("snap/1", bytes("kept"), Map.of("chain-seq", "3"));

        ObjectStore reopened = new DirectoryObjectStore(root());

        assertArrayEquals(bytes("kept"), reopened.get("snap/1").orElseThrow());
        assertEquals(Map.of("chain-seq", "3"), reopened.head("snap/1").orElseThrow().metadata());
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6})
    void listingMatchesTheMemoryStoreForRandomKeysAndQueries(long seed) {
        Random random = new Random(seed);
        MemoryObjectStore reference = new MemoryObjectStore();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            String key = randomKey(random);
            keys.add(key);
            reference.put(key, bytes("v"));
            store.put(key, bytes("v"));
        }

        for (int query = 0; query < 500; query++) {
            String prefix = random.nextInt(4) == 0 ? "" : cut(random, keys);
            String startAfter = random.nextInt(3) == 0 ? "" : cut(random, keys);
            int maxKeys = 1 + random.nextInt(12);

            ListPage expected = reference.list(prefix, startAfter, maxKeys);
            ListPage actual = store.list(prefix, startAfter, maxKeys);

            String where = "seed=" + seed + " query=" + query + " prefix='" + prefix + "' startAfter='" + startAfter
                    + "' maxKeys=" + maxKeys;
            assertEquals(keys(expected), keys(actual), where);
            assertEquals(expected.truncated(), actual.truncated(), where);
        }
    }

    private static String randomKey(Random random) {
        StringBuilder key = new StringBuilder();
        int depth = random.nextInt(3);
        for (int i = 0; i < depth; i++) {
            key.append(DIRECTORIES[random.nextInt(DIRECTORIES.length)]).append('/');
        }
        return key.append(FILES[random.nextInt(FILES.length)]).toString();
    }

    private static String cut(Random random, List<String> keys) {
        String key = keys.get(random.nextInt(keys.size()));
        return key.substring(0, random.nextInt(key.length() + 1));
    }
}
