package io.nodusdb.replica;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;

public final class ChainReplay {

    private final ChainFetch chainFetch;
    private final AnchorFinder anchors;
    private final SnapshotDownloader downloader;
    private final Path scratch;

    public ChainReplay(ObjectStore store, ChainFetch chainFetch, SnapshotDownloader downloader, Path scratch) {
        this.chainFetch = chainFetch;
        this.anchors = new AnchorFinder(store, chainFetch);
        this.downloader = downloader;
        this.scratch = scratch;
    }

    public Optional<ReplayPosition> bootstrap(ReplicaSink sink) throws IOException {
        return bootstrap(sink, AnchorBounds.NONE, ReplayLimit.ALL);
    }

    public Optional<ReplayPosition> bootstrap(ReplicaSink sink, AnchorBounds bounds, ReplayLimit limit)
            throws IOException {
        Iterator<Anchor> candidates = anchors.candidates(bounds);
        while (candidates.hasNext()) {
            Anchor anchor = candidates.next();
            Optional<Path> snapshot = download(anchor);
            if (snapshot.isPresent()) {
                return Optional.of(start(anchor, snapshot.get(), sink, limit));
            }
        }
        return Optional.empty();
    }

    public Optional<ChainObject> advance(ReplayPosition position, ReplicaSink sink) {
        return advance(position, sink, object -> { });
    }

    public Optional<ChainObject> advance(ReplayPosition position, ReplicaSink sink, Consumer<ChainObject> guard) {
        return advance(position, sink, guard, ReplayLimit.ALL);
    }

    public Optional<ChainObject> advance(ReplayPosition position, ReplicaSink sink, Consumer<ChainObject> guard,
                                         ReplayLimit limit) {
        Optional<ChainObject> next = chainFetch.fetch(position.cursor().seq() + 1);
        if (next.isEmpty()) {
            return next;
        }
        guard.accept(next.get());
        position.cursor().accept(next.get());
        position.requirePinnedUnchanged(next.get());
        applyRecords(next.get(), position, sink, limit);
        return next;
    }

    public static void applyRecords(ChainObject object, ReplayPosition position, ReplicaSink sink,
                                    ReplayLimit limit) {
        if (!(object.body() instanceof ChainBody.Records records) || records.lsnLast() <= position.appliedLsn()) {
            return;
        }
        long last = Math.min(records.lsnLast(), limit.lastLsn(records, position.appliedLsn()));
        if (last <= position.appliedLsn()) {
            return;
        }
        long first = Math.max(records.lsnFirst(), position.appliedLsn() + 1);
        byte[] bytes = first == records.lsnFirst() && last == records.lsnLast() ? records.records()
                : RecordSlice.between(records.records(), first, last);
        sink.apply(bytes, first, last);
        position.applied(last);
    }

    public int catchUp(ReplayPosition position, ReplicaSink sink) {
        int objects = 0;
        while (advance(position, sink).isPresent()) {
            objects++;
        }
        requireNoGap(position);
        return objects;
    }

    public void requireNoGap(ReplayPosition position) {
        long next = position.cursor().seq() + 1;
        if (chainFetch.hasObjectAfter(next) && chainFetch.fetch(next).isEmpty()) {
            throw new ChainGapException(next);
        }
    }

    private Optional<Path> download(Anchor anchor) throws IOException {
        try {
            return Optional.of(downloader.download(anchor.snapshotKey(), anchor.snapshot().sha256(), scratch));
        } catch (SnapshotUnavailableException unavailable) {
            return Optional.empty();
        }
    }

    private ReplayPosition start(Anchor anchor, Path snapshot, ReplicaSink sink, ReplayLimit limit)
            throws IOException {
        long loaded;
        try {
            loaded = sink.load(snapshot);
        } finally {
            Files.deleteIfExists(snapshot);
        }
        if (loaded != anchor.snapshotLsn()) {
            throw new ChainTrustException("the snapshot " + anchor.snapshotKey() + " holds LSN " + loaded
                    + " but its reference commits to LSN " + anchor.snapshotLsn());
        }
        ChainObject first = chainFetch.require(anchor.firstSeq());
        ReplayPosition position = new ReplayPosition(ChainCursor.startingAfter(first), loaded, anchor.reference());
        position.requirePinnedUnchanged(first);
        applyRecords(first, position, sink, limit);
        return position;
    }
}
