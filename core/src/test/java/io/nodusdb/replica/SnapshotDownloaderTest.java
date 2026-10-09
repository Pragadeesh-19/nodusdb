package io.nodusdb.replica;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.FatalStoreException;
import io.nodusdb.objectstore.ForwardingObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotDownloaderTest {

    private static final String KEY = "_nodus/snapshots/00000000000000000042.nsnap";
    private static final int RANGE = 1_000;

    @TempDir
    Path directory;

    private final MemoryObjectStore memory = new MemoryObjectStore();

    private byte[] snapshot(int size) {
        byte[] content = new byte[size];
        new Random(size).nextBytes(content);
        memory.put(KEY, content);
        return content;
    }

    private SnapshotDownloader downloader(ObjectStore store, int parallelism) {
        return new SnapshotDownloader(store, parallelism, RANGE, Duration.ZERO);
    }

    private long leftovers() throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.count();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 999, 1_000, 1_001, 2_000, 5_432, 100_000})
    void everySizeAroundTheRangeBoundaryComesBackByteForByte(int size) throws IOException {
        byte[] content = snapshot(size);

        for (int parallelism : new int[]{1, 2, 4, 16}) {
            Path file = downloader(memory, parallelism).download(KEY, ChainHash.sha256(content), directory);

            assertArrayEquals(content, Files.readAllBytes(file), "size " + size + " parallelism " + parallelism);
            Files.delete(file);
        }
    }

    @Test
    void anEmptySnapshotNeedsNoRanges() throws IOException {
        byte[] content = snapshot(0);
        List<String> ranges = new ArrayList<>();

        Path file = downloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                ranges.add(offset + "+" + length);
                return super.getRange(key, offset, length);
            }
        }, 4).download(KEY, ChainHash.sha256(content), directory);

        assertEquals(0, Files.size(file));
        assertEquals(List.of(), ranges);
    }

    @Test
    void theRangesRequestedCoverTheObjectExactlyOnce() throws IOException {
        byte[] content = snapshot(5_432);
        List<String> ranges = Collections.synchronizedList(new ArrayList<>());

        downloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                ranges.add(offset + "+" + length);
                return super.getRange(key, offset, length);
            }
        }, 4).download(KEY, ChainHash.sha256(content), directory);

        List<String> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparable::compareTo);
        assertEquals(List.of("0+1000", "1000+1000", "2000+1000", "3000+1000", "4000+1000", "5000+432"), sorted);
    }

    @Test
    void neverMoreThanTheConfiguredNumberOfRangesAreInFlightAndTheyDoOverlap() throws IOException {
        byte[] content = snapshot(40_000);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();

        downloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                int now = inFlight.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                try {
                    Thread.sleep(15);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                try {
                    return super.getRange(key, offset, length);
                } finally {
                    inFlight.decrementAndGet();
                }
            }
        }, 4).download(KEY, ChainHash.sha256(content), directory);

        assertEquals(4, peak.get());
    }

    @Test
    void aSingleWorkerNeverOverlapsRanges() throws IOException {
        byte[] content = snapshot(10_000);
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger inFlight = new AtomicInteger();

        downloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                try {
                    return super.getRange(key, offset, length);
                } finally {
                    inFlight.decrementAndGet();
                }
            }
        }, 1).download(KEY, ChainHash.sha256(content), directory);

        assertEquals(1, peak.get());
    }

    @Test
    void aRangeThatFailsTransientlyIsRetriedAndTheFileIsStillExact() throws IOException {
        byte[] content = snapshot(5_000);
        FaultyObjectStore faulty = new FaultyObjectStore(memory).failNext(Operation.GET_RANGE, 3, Fault.FAIL_BEFORE);

        Path file = downloader(faulty, 2).download(KEY, ChainHash.sha256(content), directory);

        assertArrayEquals(content, Files.readAllBytes(file));
        assertTrue(faulty.count(Operation.GET_RANGE) >= 8);
    }

    @Test
    void aRangeThatKeepsFailingGivesUpWithTheStoresOwnError() throws IOException {
        byte[] content = snapshot(5_000);
        FaultyObjectStore faulty = new FaultyObjectStore(memory).failWhen(call -> call.operation()
                == Operation.GET_RANGE, 1_000, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class,
                () -> downloader(faulty, 2).download(KEY, ChainHash.sha256(content), directory));
        assertEquals(0, leftovers());
    }

    @Test
    void aFatalErrorIsNotRetried() throws IOException {
        byte[] content = snapshot(5_000);
        FaultyObjectStore faulty = new FaultyObjectStore(memory).failNext(Operation.GET_RANGE, 1, Fault.FATAL);

        assertThrows(FatalStoreException.class,
                () -> downloader(faulty, 1).download(KEY, ChainHash.sha256(content), directory));

        assertEquals(1, faulty.count(Operation.GET_RANGE));
        assertEquals(0, leftovers());
    }

    @Test
    void aShortReadIsRetriedAsIfTheConnectionHadBeenCut() throws IOException {
        byte[] content = snapshot(3_000);
        AtomicInteger shortened = new AtomicInteger();

        Path file = downloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                Optional<byte[]> bytes = super.getRange(key, offset, length);
                if (offset == 1_000 && shortened.getAndIncrement() < 2) {
                    return bytes.map(whole -> Arrays.copyOf(whole, whole.length - 1));
                }
                return bytes;
            }
        }, 2).download(KEY, ChainHash.sha256(content), directory);

        assertArrayEquals(content, Files.readAllBytes(file));
        assertEquals(3, shortened.get());
    }

    @Test
    void aSnapshotThatIsNotThereIsReportedAsUnavailable() throws IOException {
        SnapshotUnavailableException missing = assertThrows(SnapshotUnavailableException.class,
                () -> downloader(memory, 2).download(KEY, ChainHash.ZERO, directory));

        assertTrue(missing.getMessage().contains(KEY), missing.getMessage());
        assertEquals(0, leftovers());
    }

    @Test
    void anObjectThatVanishesAfterTheHeadIsReportedAsUnavailable() throws IOException {
        byte[] content = snapshot(3_000);

        assertThrows(SnapshotUnavailableException.class, () -> downloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                memory.delete(key);
                return super.getRange(key, offset, length);
            }
        }, 1).download(KEY, ChainHash.sha256(content), directory));
        assertEquals(0, leftovers());
    }

    @Test
    void bytesThatDifferFromTheSignedHashAreRefusedAndNothingIsLeftOnDisk() throws IOException {
        byte[] content = snapshot(5_000);
        ChainHash signed = ChainHash.sha256(content);
        content[4_321] ^= 0x40;
        memory.put(KEY, content);

        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> downloader(memory, 4).download(KEY, signed, directory));

        assertTrue(refused.getMessage().contains(KEY), refused.getMessage());
        assertEquals(0, leftovers());
    }

    @Test
    void anObjectThatShrinksAfterTheHeadFailsInsteadOfProducingAShortFile() throws IOException {
        byte[] content = snapshot(5_000);

        assertThrows(RuntimeException.class, () -> downloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                memory.put(key, Arrays.copyOf(content, 2_500));
                return super.getRange(key, offset, length);
            }
        }, 1).download(KEY, ChainHash.sha256(content), directory));
        assertEquals(0, leftovers());
    }

    @Test
    void theDefaultRangeIsEightMebibytes() throws IOException {
        byte[] content = snapshot(20 * 1024 * 1024);
        List<Integer> lengths = Collections.synchronizedList(new ArrayList<>());

        new SnapshotDownloader(new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                lengths.add(length);
                return super.getRange(key, offset, length);
            }
        }, 4).download(KEY, ChainHash.sha256(content), directory);

        List<Integer> sorted = new ArrayList<>(lengths);
        Collections.sort(sorted);
        assertEquals(List.of(4 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024), sorted);
    }

    @Test
    void theParallelismMustBeAtLeastOne() {
        assertThrows(IllegalArgumentException.class, () -> new SnapshotDownloader(memory, 0));
    }
}
