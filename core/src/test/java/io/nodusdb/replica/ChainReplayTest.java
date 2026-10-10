package io.nodusdb.replica;

import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.objectstore.ForwardingObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.replica.RecordingSink.Applied;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.ship.ChainRetention;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainReplayTest {

    private static final long DAY = 86_400_000L;

    @TempDir
    Path scratch;

    private final AtomicLong clock = new AtomicLong(1_000 * DAY);
    private final MemoryObjectStore store = new MemoryObjectStore(clock::get);
    private final ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);
    private final RecordingSink sink = new RecordingSink();

    private ChainReplay replay(io.nodusdb.objectstore.ObjectStore target) {
        ChainFetch fetch = new ChainFetch(target, ChainBuilder.keyring(), Trust.REQUIRED);
        return new ChainReplay(target, fetch, new SnapshotDownloader(target, 2), scratch);
    }

    private long leftovers() throws IOException {
        try (Stream<Path> files = Files.list(scratch)) {
            return files.count();
        }
    }

    @Test
    void anEmptyBucketHasNothingToBootstrapFrom() throws IOException {
        assertEquals(Optional.empty(), replay(store).bootstrap(sink));
        assertEquals(List.of(), sink.loaded);
    }

    @Test
    void bootstrapLoadsTheSnapshotAndAppliesOnlyWhatFollowsItsLsn() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        ChainReplay replay = replay(store);

        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        int more = replay.catchUp(position, sink);

        assertEquals(List.of(10L), sink.loaded);
        assertEquals(3, more);
        assertEquals(List.of(new Applied(11, 12, sink.applied.get(0).bytes()),
                new Applied(13, 14, sink.applied.get(1).bytes()),
                new Applied(15, 16, sink.applied.get(2).bytes())), sink.applied);
        assertEquals(16, position.appliedLsn());
        assertEquals(4, position.cursor().seq());
        assertEquals(0, leftovers());
    }

    @Test
    void aSlowSnapshotReplaysTheObjectsBeforeItsReferenceAndSkipsWhatItAlreadyHolds() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(3, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(3, 1);
        chain.snapshotRef(snapshotLsn, 5);
        chain.many(2, 1);
        ChainReplay replay = replay(store);

        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);

        assertEquals(List.of(snapshotLsn), sink.loaded);
        assertEquals(snapshotLsn + 1, sink.applied.get(0).first());
        assertEquals(chain.lastLsn(), sink.lastAppliedLsn());
        long expected = snapshotLsn + 1;
        for (Applied applied : sink.applied) {
            assertEquals(expected, applied.first());
            expected = applied.last() + 1;
        }
        assertEquals(chain.seq(), position.cursor().seq());
    }

    @Test
    void aReferenceCommittedBeforeTheRecordsItCoversSkipsThemAndAppliesWhatComesAfter() throws IOException {
        chain.snapshotRef(0, 1);
        chain.snapshotRef(2, 2);
        chain.many(1, 1);
        chain.many(2, 1);
        ChainReplay replay = replay(store);

        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);

        assertEquals(List.of(2L), sink.loaded);
        assertEquals(2, sink.applied.size());
        assertEquals(3, sink.applied.get(0).first());
        assertEquals(4, sink.applied.get(0).last());
        assertEquals(5, sink.applied.get(1).first());
        assertEquals(chain.lastLsn(), position.appliedLsn());
    }

    @Test
    void anObjectHoldingTheSnapshotLsnInTheMiddleIsAppliedFromTheNextTransaction() throws IOException {
        chain.snapshotRef(10, 1);
        byte[] raw = concat(ChainBuilder.transaction(11, 1), ChainBuilder.transaction(13, 1));
        chain.records(raw, 11, 14);
        chain.snapshotRef(12, 3);
        ChainReplay replay = replay(store);

        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);

        assertEquals(List.of(12L), sink.loaded);
        assertEquals(1, sink.applied.size());
        assertEquals(13, sink.applied.get(0).first());
        assertEquals(14, sink.applied.get(0).last());
        assertEquals(ChainBuilder.transaction(13, 1).length, sink.applied.get(0).bytes());
    }

    @Test
    void recordsEntirelyBelowTheSnapshotAreNeverHandedToTheSink() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        chain.snapshotRef(chain.lastLsn(), 4);
        ChainReplay replay = replay(store);

        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);

        assertEquals(List.of(), sink.applied);
        assertEquals(chain.lastLsn(), position.appliedLsn());
    }

    @Test
    void aSnapshotThatCannotBeDownloadedFallsBackToTheOlderReference() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        long newest = chain.lastLsn();
        chain.snapshotRef(newest, 4);
        ChainReplay replay = replay(new ForwardingObjectStore(store) {
            @Override
            public Optional<byte[]> getRange(String key, long offset, int length) {
                return key.equals(ChainLayout.snapshotKey(newest)) ? Optional.empty()
                        : super.getRange(key, offset, length);
            }
        });

        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);

        assertEquals(List.of(10L), sink.loaded);
        assertEquals(newest, sink.lastAppliedLsn());
        assertEquals(0, leftovers());
    }

    @Test
    void snapshotBytesThatDifferFromTheSignedHashStopTheBootstrapInsteadOfFallingBack() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        long newest = chain.lastLsn();
        chain.snapshotRef(newest, 4);
        store.put(ChainLayout.snapshotKey(newest), "snapshot 999".getBytes());

        assertThrows(ChainTrustException.class, () -> replay(store).bootstrap(sink));

        assertEquals(List.of(), sink.loaded);
        assertEquals(0, leftovers());
    }

    @Test
    void aSnapshotHoldingADifferentLsnThanItsReferenceIsRefused() throws IOException {
        chain.snapshotRef(10, 1);
        sink.overrideLoadedLsn = 11;

        ChainTrustException refused = assertThrows(ChainTrustException.class, () -> replay(store).bootstrap(sink));

        assertTrue(refused.getMessage().contains("holds LSN 11"), refused.getMessage());
        assertEquals(0, leftovers());
    }

    @Test
    void anObjectWhoseLinkDoesNotMatchIsRefused() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.advance(position, sink);
        forge(3, ChainHash.ZERO, 2, 13, ChainBuilder.signingKey());

        assertThrows(ChainTrustException.class, () -> replay.advance(position, sink));
    }

    @Test
    void anObjectSignedByAnotherKeyIsRefused() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.advance(position, sink);
        forge(3, chain.digest(), 2, 13, new SigningKey(ChainBuilder.KEY_ID, KeyFiles.generate().getPrivate()));

        assertThrows(ChainTrustException.class, () -> replay.advance(position, sink));
    }

    @Test
    void anObjectWithALowerEpochIsRefused() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.advance(position, sink);
        forge(3, chain.digest(), 1, 13, ChainBuilder.signingKey());

        assertThrows(ChainTrustException.class, () -> replay.advance(position, sink));
    }

    @Test
    void anObjectThatSkipsAnLsnIsRefused() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(1, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.advance(position, sink);
        forge(3, chain.digest(), 2, 14, ChainBuilder.signingKey());

        assertThrows(ChainTrustException.class, () -> replay.advance(position, sink));
    }

    @Test
    void anObjectDeletedFromTheMiddleIsAGapNotAnEndOfChain() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(4, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.advance(position, sink);
        store.delete(ChainLayout.chainKey(3));

        ChainGapException gap = assertThrows(ChainGapException.class, () -> replay.catchUp(position, sink));

        assertEquals(3, gap.missingSeq());
    }

    @Test
    void reachingTheRealEndOfTheChainIsNotAGap() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();

        assertEquals(2, replay.catchUp(position, sink));
        assertEquals(0, replay.catchUp(position, sink));
        chain.many(1, 1);
        assertEquals(1, replay.catchUp(position, sink));
    }

    @Test
    void theReferenceThatWasPinnedMustNotChangeWhileTheReplicaStarts() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(1, 1);
        chain.snapshotRef(snapshotLsn, 4);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        ChainObject swapped = ChainCodec.seal(new ChainHeader(ChainKind.SNAPSHOT_REF, 5, 2, 7,
                ChainCodec.decode(store.get(ChainLayout.chainKey(4)).orElseThrow()).digest(), ChainBuilder.KEY_ID),
                new ChainBody.SnapshotRef(ChainLayout.snapshotKey(snapshotLsn), ChainHash.sha256(new byte[]{1}),
                        snapshotLsn), ChainBuilder.signingKey());
        store.put(ChainLayout.chainKey(5), swapped.encoded());

        assertThrows(ChainTrustException.class, () -> replay.advance(position, sink));
    }

    @Test
    void aSweepAfterEightIdleDaysStillLeavesTheNewestReferenceBootstrappable() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(5, 1);
        long snapshotLsn = chain.lastLsn();
        chain.many(4, 1);
        chain.snapshotRef(snapshotLsn, 11);
        chain.many(2, 1);
        clock.addAndGet(8 * DAY);
        new ChainRetention(store, Duration.ofDays(7), clock::get)
                .sweep(new ChainRetention.Reference(11, snapshotLsn), ChainRetention.NO_PROJECTOR);
        ChainReplay replay = replay(store);

        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);

        assertEquals(List.of(snapshotLsn), sink.loaded);
        assertEquals(snapshotLsn + 1, sink.applied.get(0).first());
        assertEquals(chain.lastLsn(), sink.lastAppliedLsn());
    }

    @Test
    void aSinkFailureWhileApplyingPropagatesToTheCaller() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        sink.failOnApply = new IllegalStateException("memory");

        assertThrows(IllegalStateException.class, () -> replay.advance(position, sink));
    }

    @Test
    void aGuardThatRefusesAnObjectStopsItBeforeAnythingIsAppliedAndTheCursorStaysPut() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        int appliedBefore = sink.applied.size();

        assertThrows(ChainTrustException.class, () -> replay.advance(position, sink, object -> {
            throw new ChainTrustException("refused " + object.seq());
        }));

        assertEquals(appliedBefore, sink.applied.size());
        assertEquals(1, position.cursor().seq());
        assertTrue(replay.advance(position, sink).isPresent());
        assertEquals(2, position.cursor().seq());
    }

    @Test
    void theGuardSeesEachFetchedObjectAndNothingWhenThereIsNoNextObject() throws IOException {
        chain.snapshotRef(10, 1);
        chain.many(2, 1);
        ChainReplay replay = replay(store);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        List<Long> seen = new ArrayList<>();

        replay.advance(position, sink, object -> seen.add(object.seq()));
        replay.advance(position, sink, object -> seen.add(object.seq()));
        Optional<ChainObject> none = replay.advance(position, sink, object -> seen.add(object.seq()));

        assertEquals(List.of(2L, 3L), seen);
        assertEquals(Optional.empty(), none);
    }

    private ChainObject forge(long seq, ChainHash prev, long epoch, long firstLsn, SigningKey key) {
        ChainBody.Records body = new ChainBody.Records(firstLsn, firstLsn + 1, ChainBuilder.transaction(firstLsn, 1));
        ChainObject object = ChainCodec.seal(new ChainHeader(ChainKind.RECORDS, seq, epoch, 1, prev,
                ChainBuilder.KEY_ID), body, key);
        store.put(ChainLayout.chainKey(seq), object.encoded());
        return object;
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
