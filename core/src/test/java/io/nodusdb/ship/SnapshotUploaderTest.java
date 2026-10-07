package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.objectstore.DirectoryObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.ForwardingObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotUploaderTest {

    private static final long LSN = 42;

    @TempDir
    Path scratch;

    private final MemoryObjectStore memory = new MemoryObjectStore();
    private final FaultyObjectStore store = new FaultyObjectStore(memory);

    private Path snapshot(int bytes) throws IOException {
        byte[] content = new byte[bytes];
        for (int i = 0; i < bytes; i++) {
            content[i] = (byte) (i * 31 + 7);
        }
        Path file = scratch.resolve("snapshot-" + bytes + ".bin");
        Files.write(file, content);
        return file;
    }

    private SnapshotUploader uploader(ObjectStore target) {
        return new SnapshotUploader(target, 7);
    }

    @Test
    void aSnapshotIsUploadedWithItsHashAndTheChainFloorAndReturnsTheReference() throws IOException {
        Path file = snapshot(100);

        ChainBody.SnapshotRef reference = uploader(store).upload(file, LSN, 9);

        assertEquals(ChainLayout.snapshotKey(LSN), reference.path());
        assertEquals(LSN, reference.lsn());
        assertEquals(ChainHash.sha256(Files.readAllBytes(file)), reference.sha256());
        assertArrayEquals(Files.readAllBytes(file), memory.get(reference.path()).orElseThrow());
        ObjectInfo info = memory.head(reference.path()).orElseThrow();
        assertEquals("9", info.metadata().get(ChainHead.FLOOR_METADATA));
        assertEquals(reference.sha256().hex(), info.metadata().get(SnapshotUploader.SHA256_METADATA));
    }

    @Test
    void aFloorBelowOneIsRaisedToOne() throws IOException {
        uploader(store).upload(snapshot(10), LSN, 0);

        assertEquals("1", memory.head(ChainLayout.snapshotKey(LSN)).orElseThrow().metadata()
                .get(ChainHead.FLOOR_METADATA));
    }

    @Test
    void aOneByteSnapshotIsUploadedAndVerified() throws IOException {
        ChainBody.SnapshotRef reference = uploader(store).upload(snapshot(1), LSN, 1);

        assertEquals(1, memory.head(reference.path()).orElseThrow().size());
    }

    @Test
    void theUploadIsVerifiedByReadingItBackInChunks() throws IOException {
        uploader(store).upload(snapshot(100), LSN, 1);

        assertEquals(15, store.count(Operation.GET_RANGE));
        assertEquals(1, store.count(Operation.PUT_FILE));
    }

    @Test
    void aChunkLargerThanTheFileReadsItOnce() throws IOException {
        new SnapshotUploader(store, 1 << 20).upload(snapshot(100), LSN, 1);

        assertEquals(1, store.count(Operation.GET_RANGE));
    }

    @Test
    void uploadingTheSameSnapshotAgainReusesTheObjectWithoutWritingIt() throws IOException {
        Path file = snapshot(100);
        ChainBody.SnapshotRef first = uploader(store).upload(file, LSN, 1);

        ChainBody.SnapshotRef second = uploader(store).upload(file, LSN, 50);

        assertEquals(first, second);
        assertEquals(1, store.count(Operation.PUT_FILE));
        assertEquals("1", memory.head(first.path()).orElseThrow().metadata().get(ChainHead.FLOOR_METADATA));
    }

    @Test
    void anObjectWithTheSameSizeButOtherContentIsNeverOverwritten() throws IOException {
        Path file = snapshot(100);
        byte[] foreign = Files.readAllBytes(file);
        foreign[50] ^= 0x01;
        memory.put(ChainLayout.snapshotKey(LSN), foreign);

        WriterFencedException refused = assertThrows(WriterFencedException.class,
                () -> uploader(store).upload(file, LSN, 1));

        assertTrue(refused.getMessage().contains("different content"), refused.getMessage());
        assertArrayEquals(foreign, memory.get(ChainLayout.snapshotKey(LSN)).orElseThrow());
        assertEquals(0, store.count(Operation.PUT_FILE));
        assertEquals(0, store.count(Operation.DELETE));
    }

    @Test
    void anObjectOfAnotherSizeIsNeverOverwritten() throws IOException {
        Path file = snapshot(100);
        memory.put(ChainLayout.snapshotKey(LSN), new byte[99]);

        assertThrows(WriterFencedException.class, () -> uploader(store).upload(file, LSN, 1));

        assertEquals(99, memory.head(ChainLayout.snapshotKey(LSN)).orElseThrow().size());
    }

    @Test
    void anUploadThatIsCorruptedInTransitIsRemovedAndReportedAsRetryable() throws IOException {
        Path file = snapshot(100);
        ObjectStore tampering = new ForwardingObjectStore(memory) {
            @Override
            public void putFile(String key, Path source, Map<String, String> metadata) {
                try {
                    byte[] bytes = Files.readAllBytes(source);
                    bytes[bytes.length - 1] ^= 0x7F;
                    put(key, bytes);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        TransientStoreException failure = assertThrows(TransientStoreException.class,
                () -> uploader(tampering).upload(file, LSN, 1));

        assertTrue(failure.getMessage().contains("removed"), failure.getMessage());
        assertFalse(memory.exists(ChainLayout.snapshotKey(LSN)));
        assertEquals(ChainHash.sha256(Files.readAllBytes(file)), uploader(store).upload(file, LSN, 1).sha256());
    }

    @Test
    void aTruncatedUploadIsRemoved() throws IOException {
        Path file = snapshot(100);
        ObjectStore truncating = new ForwardingObjectStore(memory) {
            @Override
            public void putFile(String key, Path source, Map<String, String> metadata) {
                try {
                    put(key, Arrays.copyOf(Files.readAllBytes(source), 60));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        assertThrows(TransientStoreException.class, () -> uploader(truncating).upload(file, LSN, 1));

        assertFalse(memory.exists(ChainLayout.snapshotKey(LSN)));
    }

    @Test
    void aShortReadBackIsRejected() throws IOException {
        Path file = snapshot(100);
        ObjectStore shortReads = new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                return super.getRange(key, offset, length).map(bytes -> Arrays.copyOf(bytes, bytes.length - 1));
            }
        };

        assertThrows(TransientStoreException.class, () -> uploader(shortReads).upload(file, LSN, 1));

        assertFalse(memory.exists(ChainLayout.snapshotKey(LSN)));
    }

    @Test
    void anObjectThatVanishesBeforeItIsReadBackIsRejected() throws IOException {
        Path file = snapshot(100);
        ObjectStore vanishing = new ForwardingObjectStore(memory) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                return Optional.empty();
            }
        };

        assertThrows(TransientStoreException.class, () -> uploader(vanishing).upload(file, LSN, 1));
    }

    @Test
    void aFailedWriteLeavesNothingAndTheRetrySucceeds() throws IOException {
        Path file = snapshot(100);
        store.failNext(Operation.PUT_FILE, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> uploader(store).upload(file, LSN, 1));
        assertFalse(memory.exists(ChainLayout.snapshotKey(LSN)));

        assertEquals(LSN, uploader(store).upload(file, LSN, 1).lsn());
    }

    @Test
    void aWriteThatLandedBeforeTheErrorIsAdoptedOnTheRetry() throws IOException {
        Path file = snapshot(100);
        store.failNext(Operation.PUT_FILE, Fault.FAIL_AFTER);

        assertThrows(TransientStoreException.class, () -> uploader(store).upload(file, LSN, 1));
        ChainBody.SnapshotRef adopted = uploader(store).upload(file, LSN, 1);

        assertEquals(ChainHash.sha256(Files.readAllBytes(file)), adopted.sha256());
        assertEquals(1, store.count(Operation.PUT_FILE));
    }

    @Test
    void aStoreFailureWhileLookingForAnExistingObjectIsPassedOn() throws IOException {
        Path file = snapshot(100);
        store.failNext(Operation.HEAD, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> uploader(store).upload(file, LSN, 1));

        assertEquals(0, store.count(Operation.PUT_FILE));
    }

    @Test
    void aMissingFileFailsBeforeTheStoreIsTouched() {
        assertThrows(UncheckedIOException.class, () -> uploader(store).upload(scratch.resolve("absent"), LSN, 1));

        assertEquals(0, store.callCount());
    }

    @Test
    void aDirectoryBackedStoreRoundTripsTheSnapshotAndItsMetadata() throws IOException {
        Path file = snapshot(5_000);
        DirectoryObjectStore directory = new DirectoryObjectStore(scratch.resolve("bucket"));

        ChainBody.SnapshotRef reference = new SnapshotUploader(directory, 1_024).upload(file, LSN, 3);

        assertArrayEquals(Files.readAllBytes(file), directory.get(reference.path()).orElseThrow());
        assertEquals("3", directory.head(reference.path()).orElseThrow().metadata().get(ChainHead.FLOOR_METADATA));
        assertEquals(reference, new SnapshotUploader(directory, 1_024).upload(file, LSN, 3));
    }

    @Test
    void thePublisherStagesUploadsAndAlwaysReleasesTheStagedFile() throws IOException {
        Path file = snapshot(100);
        AtomicInteger released = new AtomicInteger();
        SnapshotUploadPublisher publisher = new SnapshotUploadPublisher(
                () -> new StagedSnapshot(file, LSN, released::incrementAndGet), uploader(store));

        ChainBody.SnapshotRef reference = publisher.publish(4);

        assertEquals(LSN, reference.lsn());
        assertEquals(1, released.get());

        store.failNext(Operation.HEAD, Fault.FAIL_BEFORE);
        assertThrows(TransientStoreException.class, () -> publisher.publish(4));
        assertEquals(2, released.get());
    }

    @Test
    void thePublisherWrapsAFailureToStageTheSnapshot() {
        SnapshotUploadPublisher publisher = new SnapshotUploadPublisher(() -> {
            throw new IOException("disk full");
        }, uploader(store));

        UncheckedIOException failure = assertThrows(UncheckedIOException.class, () -> publisher.publish(1));

        assertEquals("disk full", failure.getCause().getMessage());
    }

    @Test
    void aStagedSnapshotValidatesItsFields() {
        assertThrows(NullPointerException.class, () -> new StagedSnapshot(null, 1, () -> { }));
        assertThrows(NullPointerException.class, () -> new StagedSnapshot(scratch, 1, null));
        assertThrows(IllegalArgumentException.class, () -> new StagedSnapshot(scratch, -1, () -> { }));
    }
}
