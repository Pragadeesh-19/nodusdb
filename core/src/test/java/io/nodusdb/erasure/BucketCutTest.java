package io.nodusdb.erasure;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.symbols.Pseudonym;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.replica.AnchorFinder;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainRetention;
import io.nodusdb.ship.ShipState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BucketCutTest {

    private static final byte[] SALT = new byte[Pseudonym.SALT_BYTES];
    private static final int VIEWER = 1;

    @TempDir
    Path directory;

    private final MemoryObjectStore store = new MemoryObjectStore();
    private final ChainBuilder chain = new ChainBuilder(store, 7).epoch(1);
    private final GraphKernel kernel = new GraphKernel();
    private final ShipState state = new ShipState(1, 0, 0, 1L << 30);
    private final AtomicLong projected = new AtomicLong(Long.MAX_VALUE);

    private static void symbol(RecordBatch batch, int id, String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        batch.symbol(id, bytes, 0, bytes.length);
    }

    private static void grant(RecordBatch batch) {
        symbol(batch, 0, "user:alice");
        symbol(batch, 1, "document:readme");
        batch.tuple(RecordType.TUPLE_ADD, 1, VIEWER, 0, 0);
    }

    private static void erase(RecordBatch batch) {
        batch.erase(0, Pseudonym.padded(Pseudonym.of(SALT, "user:alice".getBytes(StandardCharsets.UTF_8))));
    }

    private byte[] commitBoth(long firstLsn, Consumer<RecordBatch> records) {
        RecordBatch batch = new RecordBatch();
        records.accept(batch);
        batch.commit();
        kernel.commit(batch);
        RecordBatch shipped = new RecordBatch();
        records.accept(shipped);
        shipped.commit();
        shipped.seal(firstLsn, RecordFixtures.COMMIT_MICROS);
        return RecordFixtures.copyOf(shipped);
    }

    private BucketCut cut() {
        ChainFetch fetch = new ChainFetch(store, ChainBuilder.keyring(), ChainFetch.Trust.WHEN_KEY_KNOWN);
        BucketCut.Bucket bucket = new BucketCut.Bucket(new AnchorFinder(store, fetch), fetch,
                new ChainRetention(store, Duration.ofDays(7), System::currentTimeMillis), projected::get);
        return new BucketCut(kernel, state, bucket, ErasureCoverage.open(directory), System::nanoTime);
    }

    private void commitOfferedCut(long seq) {
        ShipState.CutRequest request = state.takeCut();
        state.cutCommitted(seq, new ChainBody.Redaction(request.eraseLsn(), request.erasedCount(),
                request.firstKeptSeq(), request.snapshotLsn(), chain.lastLsn()));
    }

    @Test
    void theCutKeepsWhatFollowersNeedAndDeletesEverythingBeforeIt() {
        chain.snapshotRef(0, 1);
        chain.records(commitBoth(1, BucketCutTest::grant), 1, 4);
        chain.records(commitBoth(5, BucketCutTest::erase), 5, 6);
        chain.snapshotRef(6, 3);
        BucketCut bucket = cut();
        assertFalse(bucket.covers(kernel.newestEraseLsn(), kernel.erasedSymbols()));

        bucket.advance(kernel.newestEraseLsn(), false);
        commitOfferedCut(5);
        bucket.advance(kernel.newestEraseLsn(), false);

        assertTrue(store.get(ChainLayout.chainKey(1)).isEmpty());
        assertTrue(store.get(ChainLayout.chainKey(2)).isEmpty());
        assertTrue(store.get(ChainLayout.chainKey(3)).isPresent());
        assertTrue(store.get(ChainLayout.snapshotKey(0)).isEmpty());
        assertTrue(store.get(ChainLayout.snapshotKey(6)).isPresent());
        assertTrue(bucket.covers(kernel.newestEraseLsn(), kernel.erasedSymbols()));
        assertEquals(new ErasureCoverage.Covered(1, 6, 3, 6), ErasureCoverage.open(directory).covered());
    }

    @Test
    void aKeptObjectThatStillHoldsTheErasedNameStopsTheCut() {
        chain.snapshotRef(0, 1);
        chain.records(commitBoth(1, batch -> {
            grant(batch);
            erase(batch);
        }), 1, 5);
        chain.snapshotRef(5, 3);
        BucketCut bucket = cut();

        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> bucket.advance(kernel.newestEraseLsn(), false));

        assertTrue(refused.getMessage().contains("still holds an erased name"));
        assertNull(state.takeCut());
        assertTrue(store.get(ChainLayout.chainKey(2)).isPresent());
    }

    @Test
    void theCutWaitsForTheProjectorToPassTheKeptObject() {
        chain.snapshotRef(0, 1);
        chain.records(commitBoth(1, BucketCutTest::grant), 1, 4);
        chain.records(commitBoth(5, BucketCutTest::erase), 5, 6);
        chain.snapshotRef(6, 3);
        projected.set(2);
        BucketCut bucket = cut();

        bucket.advance(kernel.newestEraseLsn(), false);
        assertNull(state.takeCut());

        projected.set(3);
        bucket.advance(kernel.newestEraseLsn(), false);
        assertEquals(3, state.takeCut().firstKeptSeq());
    }

    @Test
    void aSnapshotOlderThanTheErasureIsNotCutAt() {
        chain.snapshotRef(0, 1);
        chain.records(commitBoth(1, BucketCutTest::grant), 1, 4);
        chain.snapshotRef(4, 2);
        chain.records(commitBoth(5, BucketCutTest::erase), 5, 6);
        BucketCut bucket = cut();

        bucket.advance(kernel.newestEraseLsn(), false);

        assertNull(state.takeCut());
        assertTrue(store.get(ChainLayout.chainKey(1)).isPresent());
    }
}
