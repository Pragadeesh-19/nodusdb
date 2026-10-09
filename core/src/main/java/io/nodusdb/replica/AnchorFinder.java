package io.nodusdb.replica;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainHead;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

public final class AnchorFinder {

    static final int MAX_EXAMINED = 8;
    static final int MAX_REFERENCE_SCAN = 1_000;

    private static final int PAGE_KEYS = 1_000;
    private static final long NO_HINT = 1;
    private static final long FIRST_SEQ = 1;

    private record Candidate(String key, long lsn) {
    }

    private final ObjectStore store;
    private final ChainFetch chainFetch;

    public AnchorFinder(ObjectStore store, ChainFetch chainFetch) {
        this.store = store;
        this.chainFetch = chainFetch;
    }

    public Iterator<Anchor> candidates() {
        return new Candidates(newestFirst());
    }

    private List<Candidate> newestFirst() {
        List<Candidate> found = new ArrayList<>();
        String after = "";
        while (true) {
            ListPage page = store.list(ChainLayout.SNAPSHOT_PREFIX, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                OptionalLong lsn = ChainLayout.snapshotLsn(entry.key());
                if (lsn.isPresent()) {
                    found.add(new Candidate(entry.key(), lsn.getAsLong()));
                }
            }
            if (!page.truncated()) {
                break;
            }
            after = page.lastKey();
        }
        found.sort(Comparator.comparingLong(Candidate::lsn).reversed());
        return found;
    }

    private Optional<Anchor> anchorFor(Candidate candidate) {
        Optional<ObjectInfo> info = store.head(candidate.key());
        if (info.isEmpty()) {
            return Optional.empty();
        }
        Optional<ChainObject> reference = locateReference(candidate, floorHint(info.get()));
        if (reference.isEmpty()) {
            return Optional.empty();
        }
        OptionalLong firstSeq = firstSeqToReplay(reference.get(), candidate.lsn());
        return firstSeq.isEmpty() ? Optional.empty()
                : Optional.of(new Anchor(candidate.key(), reference.get(), firstSeq.getAsLong()));
    }

    private static long floorHint(ObjectInfo info) {
        String text = info.metadata().get(ChainHead.FLOOR_METADATA);
        if (text == null) {
            return NO_HINT;
        }
        try {
            return Math.max(NO_HINT, Long.parseLong(text));
        } catch (NumberFormatException garbage) {
            return NO_HINT;
        }
    }

    private Optional<ChainObject> locateReference(Candidate candidate, long floor) {
        long start = Math.max(floor, chainFetch.oldestSeq().orElse(floor));
        for (long seq = start; seq < start + MAX_REFERENCE_SCAN; seq++) {
            Optional<ChainObject> object = chainFetch.fetch(seq);
            if (object.isEmpty()) {
                return Optional.empty();
            }
            if (object.get().body() instanceof ChainBody.SnapshotRef reference
                    && reference.path().equals(candidate.key())) {
                if (reference.lsn() != candidate.lsn()) {
                    throw new ChainTrustException("the reference at object " + seq + " names " + candidate.key()
                            + " but commits to LSN " + reference.lsn());
                }
                return object;
            }
        }
        return Optional.empty();
    }

    private OptionalLong firstSeqToReplay(ChainObject reference, long snapshotLsn) {
        for (long seq = reference.seq() - 1; seq >= 1; seq--) {
            Optional<ChainObject> object = chainFetch.fetch(seq);
            if (object.isEmpty()) {
                return OptionalLong.empty();
            }
            if (object.get().body() instanceof ChainBody.Records records && records.lsnFirst() <= snapshotLsn + 1) {
                return OptionalLong.of(seq);
            }
        }
        return OptionalLong.of(FIRST_SEQ);
    }

    private final class Candidates implements Iterator<Anchor> {

        private final Iterator<Candidate> remaining;
        private int examined;
        private Anchor ready;

        Candidates(List<Candidate> candidates) {
            this.remaining = candidates.iterator();
        }

        @Override
        public boolean hasNext() {
            while (ready == null && examined < MAX_EXAMINED && remaining.hasNext()) {
                examined++;
                ready = anchorFor(remaining.next()).orElse(null);
            }
            return ready != null;
        }

        @Override
        public Anchor next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Anchor anchor = ready;
            ready = null;
            return anchor;
        }
    }
}
