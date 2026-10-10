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
        Iterator<Anchor> candidates = anchors.candidates();
        while (candidates.hasNext()) {
            Anchor anchor = candidates.next();
            Optional<Path> snapshot = download(anchor);
            if (snapshot.isPresent()) {
                return Optional.of(start(anchor, snapshot.get(), sink));
            }
        }
        return Optional.empty();
    }

    public Optional<ChainObject> advance(ReplayPosition position, ReplicaSink sink) {
        return advance(position, sink, object -> { });
    }

    public Optional<ChainObject> advance(ReplayPosition position, ReplicaSink sink, Consumer<ChainObject> guard) {
        Optional<ChainObject> next = chainFetch.fetch(position.cursor().seq() + 1);
        if (next.isEmpty()) {
            return next;
        }
        guard.accept(next.get());
        position.cursor().accept(next.get());
        position.requirePinnedUnchanged(next.get());
        applyRecords(next.get(), position, sink);
        return next;
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
        if (chainFetch.hasObjectAfter(next - 1)) {
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

    private ReplayPosition start(Anchor anchor, Path snapshot, ReplicaSink sink) throws IOException {
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
        applyRecords(first, position, sink);
        return position;
    }

    private static void applyRecords(ChainObject object, ReplayPosition position, ReplicaSink sink) {
        if (!(object.body() instanceof ChainBody.Records records) || records.lsnLast() <= position.appliedLsn()) {
            return;
        }
        long first = Math.max(records.lsnFirst(), position.appliedLsn() + 1);
        byte[] bytes = first == records.lsnFirst() ? records.records() : RecordSlice.from(records.records(), first);
        sink.apply(bytes, first, records.lsnLast());
        position.applied(records.lsnLast());
    }
}
