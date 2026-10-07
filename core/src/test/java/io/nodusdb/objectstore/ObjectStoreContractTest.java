package io.nodusdb.objectstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class ObjectStoreContractTest {

    private static final long CLOCK_TOLERANCE_MILLIS = 10_000;
    private static final int RACERS = 16;
    private static final int RACED_KEYS = 100;
    private static final long TIMEOUT_SECONDS = 60;

    @TempDir
    Path scratch;

    ObjectStore store;

    abstract ObjectStore create() throws Exception;

    @BeforeEach
    void openStore() throws Exception {
        store = create();
    }

    static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static List<String> keys(ListPage page) {
        return page.entries().stream().map(ObjectInfo::key).toList();
    }

    @Test
    void aFirstWriteCreatesAndASecondDoesNotOverwrite() {
        assertEquals(PutResult.CREATED, store.putIfAbsent("chain/1.obj", bytes("first")));
        assertEquals(PutResult.ALREADY_EXISTS, store.putIfAbsent("chain/1.obj", bytes("second")));

        assertArrayEquals(bytes("first"), store.get("chain/1.obj").orElseThrow());
    }

    @Test
    void aLostCreateKeepsTheMetadataOfTheWinner() {
        store.putIfAbsent("snap/1.nsnap", bytes("a"), Map.of("chain-seq", "7"));
        store.putIfAbsent("snap/1.nsnap", bytes("b"), Map.of("chain-seq", "9"));

        assertEquals(Map.of("chain-seq", "7"), store.head("snap/1.nsnap").orElseThrow().metadata());
    }

    @Test
    void anUnconditionalPutReplacesContentAndClearsMetadata() {
        store.putIfAbsent("hint/version", bytes("1"), Map.of("note", "old"));

        store.put("hint/version", bytes("22"));

        ObjectInfo info = store.head("hint/version").orElseThrow();
        assertEquals(2, info.size());
        assertEquals(Map.of(), info.metadata());
        assertArrayEquals(bytes("22"), store.get("hint/version").orElseThrow());
    }

    @Test
    void anUnconditionalPutCreatesAMissingObject() {
        store.put("hint/version", bytes("1"));

        assertArrayEquals(bytes("1"), store.get("hint/version").orElseThrow());
    }

    @Test
    void aFileIsStoredWithItsMetadata() throws IOException {
        Path file = scratch.resolve("upload.bin");
        Files.write(file, bytes("snapshot bytes"));

        store.putFile("snap/2.nsnap", file, Map.of("chain-seq", "12"));

        assertArrayEquals(bytes("snapshot bytes"), store.get("snap/2.nsnap").orElseThrow());
        assertEquals(Map.of("chain-seq", "12"), store.head("snap/2.nsnap").orElseThrow().metadata());
    }

    @Test
    void aFileReplacesWhatWasThere() throws IOException {
        Path file = scratch.resolve("upload.bin");
        store.put("snap/2.nsnap", bytes("old old old"));
        Files.write(file, bytes("new"));

        store.putFile("snap/2.nsnap", file, Map.of());

        assertArrayEquals(bytes("new"), store.get("snap/2.nsnap").orElseThrow());
    }

    @Test
    void anAbsentObjectReadsAsEmptyEverywhere() {
        assertEquals(Optional.empty(), store.get("nothing/here"));
        assertEquals(Optional.empty(), store.getRange("nothing/here", 0, 10));
        assertEquals(Optional.empty(), store.head("nothing/here"));
        assertFalse(store.exists("nothing/here"));
    }

    @Test
    void aReadCannotChangeWhatIsStored() {
        store.put("a/b", bytes("stable"));

        byte[] read = store.get("a/b").orElseThrow();
        read[0] = 'X';

        assertArrayEquals(bytes("stable"), store.get("a/b").orElseThrow());
    }

    @Test
    void theStoredBytesAreNotTiedToTheCallersArray() {
        byte[] content = bytes("original");
        store.put("a/b", content);

        content[0] = 'X';

        assertArrayEquals(bytes("original"), store.get("a/b").orElseThrow());
    }

    @Test
    void rangesReturnTheRequestedBytes() {
        store.put("data/blob", bytes("0123456789"));

        assertArrayEquals(bytes("234"), store.getRange("data/blob", 2, 3).orElseThrow());
        assertArrayEquals(bytes("0"), store.getRange("data/blob", 0, 1).orElseThrow());
        assertArrayEquals(bytes("9"), store.getRange("data/blob", 9, 1).orElseThrow());
        assertArrayEquals(bytes("0123456789"), store.getRange("data/blob", 0, 10).orElseThrow());
    }

    @Test
    void aRangePastTheEndIsClampedAndOneStartingAtTheEndIsRefused() {
        store.put("data/blob", bytes("0123456789"));

        assertArrayEquals(bytes("789"), store.getRange("data/blob", 7, 100).orElseThrow());
        FatalStoreException refused = assertThrows(FatalStoreException.class,
                () -> store.getRange("data/blob", 10, 1));
        assertEquals(416, refused.status());
        assertThrows(FatalStoreException.class, () -> store.getRange("data/blob", 11, 1));
    }

    @Test
    void anEmptyObjectCanBeStoredAndHasNoReadableRange() {
        store.put("data/empty", new byte[0]);

        assertEquals(0, store.get("data/empty").orElseThrow().length);
        assertEquals(0, store.head("data/empty").orElseThrow().size());
        assertThrows(FatalStoreException.class, () -> store.getRange("data/empty", 0, 1));
    }

    @Test
    void invalidRangeArgumentsAreRejected() {
        store.put("data/blob", bytes("0123"));

        assertThrows(IllegalArgumentException.class, () -> store.getRange("data/blob", -1, 1));
        assertThrows(IllegalArgumentException.class, () -> store.getRange("data/blob", 0, 0));
        assertThrows(IllegalArgumentException.class, () -> store.getRange("data/blob", 0, -5));
    }

    @Test
    void aLargeObjectRoundTripsAndIsReadableByRange() {
        byte[] large = new byte[5 << 20];
        new Random(7).nextBytes(large);
        store.put("data/large", large);

        assertArrayEquals(large, store.get("data/large").orElseThrow());
        byte[] tail = store.getRange("data/large", large.length - 1000, 5000).orElseThrow();
        assertEquals(1000, tail.length);
        assertArrayEquals(Arrays.copyOfRange(large, large.length - 1000, large.length), tail);
    }

    @Test
    void headReportsSizeAndAPlausibleModificationTime() {
        long before = System.currentTimeMillis();
        store.put("data/blob", bytes("12345"));

        ObjectInfo info = store.head("data/blob").orElseThrow();

        assertEquals("data/blob", info.key());
        assertEquals(5, info.size());
        assertTrue(Math.abs(info.lastModifiedMillis() - before) < CLOCK_TOLERANCE_MILLIS,
                "modified at " + info.lastModifiedMillis() + " but written at about " + before);
    }

    @Test
    void deletingIsIdempotentAndRemovesMetadataWithTheObject() {
        store.delete("never/existed");
        store.putIfAbsent("a/b", bytes("1"), Map.of("note", "kept"));

        store.delete("a/b");
        store.delete("a/b");

        assertFalse(store.exists("a/b"));
        assertEquals(PutResult.CREATED, store.putIfAbsent("a/b", bytes("2")));
        assertEquals(Map.of(), store.head("a/b").orElseThrow().metadata());
    }

    @Test
    void deletingManyKeysRemovesEachOfThem() {
        List<String> doomed = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            store.put("batch/" + i, bytes("x"));
            if (i % 2 == 0) {
                doomed.add("batch/" + i);
            }
        }

        store.deleteAll(doomed);

        assertEquals(12, store.list("batch/", "", 100).entries().size());
        for (String key : doomed) {
            assertFalse(store.exists(key));
        }
    }

    @Test
    void listingReturnsKeysInBinaryOrder() {
        for (String key : new String[]{"a0", "a/c", "a.b", "a/b", "a-b", "0", "_x", "z"}) {
            store.put(key, bytes("v"));
        }

        assertEquals(List.of("0", "_x", "a-b", "a.b", "a/b", "a/c", "a0", "z"), keys(store.list("", "", 100)));
    }

    @Test
    void listingFiltersByPrefix() {
        for (String key : new String[]{"_nodus/chain/1.obj", "_nodus/chain/2.obj", "_nodus/epoch/1.json",
                "_nodus/snapshots/1.nsnap", "iceberg/data/a.parquet"}) {
            store.put(key, bytes("v"));
        }

        assertEquals(List.of("_nodus/chain/1.obj", "_nodus/chain/2.obj"),
                keys(store.list("_nodus/chain/", "", 100)));
        assertEquals(List.of("_nodus/chain/1.obj", "_nodus/chain/2.obj", "_nodus/epoch/1.json"),
                keys(store.list("_nodus/", "", 3)));
        assertEquals(List.of(), keys(store.list("missing/", "", 100)));
        assertEquals(5, store.list("", "", 100).entries().size());
    }

    @Test
    void aPartialSegmentPrefixMatchesTheKeysThatStartWithIt() {
        for (String key : new String[]{"_nodus/chain/1.obj", "_nodus/chain/2.obj", "_nodus/changes", "_nodus/epoch/1"}) {
            store.put(key, bytes("v"));
        }

        assertEquals(List.of("_nodus/chain/1.obj", "_nodus/chain/2.obj", "_nodus/changes"),
                keys(store.list("_nodus/ch", "", 100)));
    }

    @Test
    void startAfterIsExclusive() {
        for (int i = 1; i <= 5; i++) {
            store.put("chain/" + i, bytes("v"));
        }

        assertEquals(List.of("chain/4", "chain/5"), keys(store.list("chain/", "chain/3", 100)));
        assertEquals(List.of(), keys(store.list("chain/", "chain/5", 100)));
        assertEquals(List.of(), keys(store.list("chain/", "chain/9", 100)));
        assertEquals(5, store.list("chain/", "chain/0", 100).entries().size());
        assertEquals(5, store.list("chain/", "a", 100).entries().size());
    }

    @Test
    void startAfterMayBeAKeyThatDoesNotExistOrADirectoryPrefix() {
        for (String key : new String[]{"a/1", "a/2", "b/1", "c"}) {
            store.put(key, bytes("v"));
        }

        assertEquals(List.of("a/1", "a/2", "b/1", "c"), keys(store.list("", "a/", 100)));
        assertEquals(List.of("a/2", "b/1", "c"), keys(store.list("", "a/1", 100)));
        assertEquals(List.of("b/1", "c"), keys(store.list("", "a/5", 100)));
        assertEquals(List.of("c"), keys(store.list("", "b/1", 100)));
        assertEquals(List.of(), keys(store.list("", "d", 100)));
    }

    @Test
    void pagingWalksEveryKeyExactlyOnce() {
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 23; i++) {
            String key = String.format("chain/%020d.obj", i);
            store.put(key, bytes("v"));
            expected.add(key);
        }

        List<String> seen = new ArrayList<>();
        String after = "";
        int pages = 0;
        while (true) {
            ListPage page = store.list("chain/", after, 5);
            seen.addAll(keys(page));
            pages++;
            if (!page.truncated()) {
                break;
            }
            after = page.lastKey();
        }

        assertEquals(expected, seen);
        assertEquals(5, pages);
    }

    @Test
    void aPageHoldingExactlyTheLimitIsNotTruncated() {
        for (int i = 0; i < 3; i++) {
            store.put("k/" + i, bytes("v"));
        }

        assertFalse(store.list("k/", "", 3).truncated());
        assertTrue(store.list("k/", "", 2).truncated());
        assertFalse(store.list("k/", "", 4).truncated());
        assertFalse(store.list("none/", "", 1).truncated());
    }

    @Test
    void listingAnEmptyStoreIsEmpty() {
        ListPage page = store.list("", "", 10);

        assertTrue(page.entries().isEmpty());
        assertFalse(page.truncated());
        assertEquals("", page.lastKey());
    }

    @Test
    void listedEntriesCarrySizeMetadataAndTime() {
        store.putIfAbsent("snap/1", bytes("abc"), Map.of("chain-seq", "5"));

        ObjectInfo info = store.list("snap/", "", 10).entries().get(0);

        assertEquals("snap/1", info.key());
        assertEquals(3, info.size());
        assertEquals(Map.of("chain-seq", "5"), info.metadata());
        assertNotEquals(0, info.lastModifiedMillis());
    }

    @Test
    void theListLimitIsBounded() {
        assertThrows(IllegalArgumentException.class, () -> store.list("", "", 0));
        assertThrows(IllegalArgumentException.class, () -> store.list("", "", 1001));
        store.list("", "", 1000);
        store.list("", "", 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "/abs", "a//b", "a/../b", "../a", "./a", "a/./b", ".hidden", "a/.hidden", "a b", "a\\b", "a:b",
            "ключ", "a/", "a?b", "a*b", "a\u0000b", "a\tb", "A", "Chain/1", "a/B", "nul", "a/con", "aux.txt",
            "a/prn/b", "lpt1", "com9.log", "a.", "a/b.", "a./b"
    })
    void aMalformedKeyIsRefusedByEveryOperation(String key) {
        assertThrows(IllegalArgumentException.class, () -> store.putIfAbsent(key, bytes("x")));
        assertThrows(IllegalArgumentException.class, () -> store.put(key, bytes("x")));
        assertThrows(IllegalArgumentException.class, () -> store.get(key));
        assertThrows(IllegalArgumentException.class, () -> store.getRange(key, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> store.head(key));
        assertThrows(IllegalArgumentException.class, () -> store.delete(key));
    }

    @Test
    void aKeyLongerThanTheLimitIsRefusedAndOneAtTheLimitIsKept() {
        String segment = "k".repeat(200);
        String atLimit = String.join("/", segment, segment, segment, segment, "k".repeat(220));
        assertEquals(ObjectKeys.MAX_KEY_LENGTH, atLimit.length());

        assertThrows(IllegalArgumentException.class, () -> store.put(atLimit + "k", bytes("x")));
        store.put(atLimit, bytes("x"));
        assertTrue(store.exists(atLimit));
        assertEquals(List.of(atLimit), keys(store.list("", "", 10)));
    }

    @Test
    void aSegmentAtTheLimitIsKeptAndOnePastItIsRefused() {
        String atLimit = "s".repeat(ObjectKeys.MAX_SEGMENT_LENGTH);

        assertThrows(IllegalArgumentException.class, () -> store.put(atLimit + "s", bytes("x")));
        assertThrows(IllegalArgumentException.class, () -> store.put("a/" + atLimit + "s/b", bytes("x")));
        store.put("a/" + atLimit, bytes("x"));
        assertTrue(store.exists("a/" + atLimit));
    }

    @Test
    void aPartialPrefixMayLookLikeAReservedNameOrEndWithADot() {
        store.put("console/1", bytes("x"));
        store.put("a.b/1", bytes("x"));

        assertEquals(List.of("console/1"), keys(store.list("con", "", 10)));
        assertEquals(List.of("a.b/1"), keys(store.list("a.", "", 10)));
        assertEquals(List.of(), keys(store.list("nul", "", 10)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/a", "a//b", "../", "a/../b", ".x/", "a b/", "ключ/"})
    void aMalformedPrefixOrStartAfterIsRefused(String value) {
        assertThrows(IllegalArgumentException.class, () -> store.list(value, "", 10));
        assertThrows(IllegalArgumentException.class, () -> store.list("", value, 10));
    }

    @Test
    void metadataIsValidatedBeforeAnythingIsWritten() {
        Map<String, String> tooMany = new HashMap<>();
        for (int i = 0; i <= ObjectKeys.MAX_METADATA_ENTRIES; i++) {
            tooMany.put("k" + i, "v");
        }
        List<Map<String, String>> invalid = List.of(
                Map.of("Upper", "v"), Map.of("has space", "v"), Map.of("", "v"), Map.of("k", "café"),
                Map.of("k", "line\nbreak"), Map.of("k", "x".repeat(ObjectKeys.MAX_METADATA_VALUE_LENGTH + 1)),
                Map.of("k".repeat(ObjectKeys.MAX_METADATA_NAME_LENGTH + 1), "v"), tooMany);

        for (Map<String, String> metadata : invalid) {
            assertThrows(IllegalArgumentException.class, () -> store.putIfAbsent("m/x", bytes("x"), metadata));
        }
        assertFalse(store.exists("m/x"));
    }

    @Test
    void manyThreadsRacingOnOneKeyProduceExactlyOneWinner() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(RACERS);
        try {
            for (int key = 0; key < RACED_KEYS; key++) {
                String name = "race/" + key;
                CountDownLatch go = new CountDownLatch(1);
                AtomicInteger created = new AtomicInteger();
                List<Future<Integer>> racers = new ArrayList<>();
                for (int racer = 0; racer < RACERS; racer++) {
                    int id = racer;
                    racers.add(pool.submit(() -> {
                        go.await();
                        PutResult result = store.putIfAbsent(name, bytes("racer-" + id));
                        if (result == PutResult.CREATED) {
                            created.incrementAndGet();
                            return id;
                        }
                        assertEquals(PutResult.ALREADY_EXISTS, result);
                        return -1;
                    }));
                }
                go.countDown();
                int winner = -1;
                for (Future<Integer> racer : racers) {
                    int outcome = racer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (outcome >= 0) {
                        winner = outcome;
                    }
                }
                assertEquals(1, created.get(), "key " + name);
                assertArrayEquals(bytes("racer-" + winner), store.get(name).orElseThrow(), "key " + name);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void readersNeverSeeAPartialObjectWhileItIsBeingReplaced() throws Exception {
        byte[] first = new byte[1 << 20];
        byte[] second = new byte[1 << 20];
        Arrays.fill(first, (byte) 'a');
        Arrays.fill(second, (byte) 'b');
        store.put("swap/object", first);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicInteger torn = new AtomicInteger();
        try {
            CountDownLatch writing = new CountDownLatch(1);
            Future<?> writer = pool.submit(() -> {
                for (int i = 0; i < 40; i++) {
                    store.put("swap/object", i % 2 == 0 ? second : first);
                    writing.countDown();
                }
            });
            Future<?> reader = pool.submit(() -> {
                writing.await();
                for (int i = 0; i < 200; i++) {
                    byte[] seen = store.get("swap/object").orElseThrow();
                    if (seen.length != first.length || seen[0] != seen[seen.length - 1]) {
                        torn.incrementAndGet();
                    }
                }
                return null;
            });
            writer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertEquals(0, torn.get());
    }
}
