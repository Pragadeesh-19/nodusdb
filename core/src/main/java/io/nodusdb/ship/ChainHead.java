package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;

import java.util.Optional;

public final class ChainHead {

    public static final String FLOOR_METADATA = "chain-seq-floor";
    public static final long NO_SNAPSHOT = -1;

    private static final int PAGE_KEYS = 1000;

    public record Found(ChainCursor cursor, ChainObject object) {
    }

    private record NewestSnapshot(long lsn, String key) {
    }

    private final ObjectStore store;
    private final ChainFetch chainFetch;

    public ChainHead(ObjectStore store, Keyring keyring) {
        this(store, keyring, ChainFetch.Trust.WHEN_KEY_KNOWN);
    }

    public ChainHead(ObjectStore store, Keyring keyring, ChainFetch.Trust trust) {
        this.store = store;
        this.chainFetch = new ChainFetch(store, keyring, trust);
    }

    public Optional<Found> find() {
        long floor = snapshotFloor();
        long head = lastSeqAfter(floor > 1 ? floor - 1 : 0);
        if (head == 0 && floor > 1) {
            head = lastSeqAfter(0);
        }
        if (head == 0) {
            return Optional.empty();
        }
        ChainObject object = chainFetch.require(head);
        long lastLsn = lastLsn(object);
        return Optional.of(new Found(ChainCursor.after(object.seq(), object.digest(), object.epoch(), lastLsn),
                object));
    }

    public long newestSnapshotLsn() {
        return newestSnapshot().map(NewestSnapshot::lsn).orElse(NO_SNAPSHOT);
    }

    private long snapshotFloor() {
        return newestSnapshot()
                .flatMap(newest -> store.head(newest.key()))
                .map(info -> parseFloor(info.metadata().get(FLOOR_METADATA)))
                .orElse(0L);
    }

    private Optional<NewestSnapshot> newestSnapshot() {
        NewestSnapshot newest = null;
        String after = "";
        while (true) {
            ListPage page = store.list(ChainLayout.SNAPSHOT_PREFIX, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                long lsn = ChainLayout.snapshotLsn(entry.key()).orElse(NO_SNAPSHOT);
                if (lsn > (newest == null ? NO_SNAPSHOT : newest.lsn())) {
                    newest = new NewestSnapshot(lsn, entry.key());
                }
            }
            if (!page.truncated()) {
                return Optional.ofNullable(newest);
            }
            after = page.lastKey();
        }
    }

    private static long parseFloor(String text) {
        if (text == null) {
            return 0;
        }
        try {
            return Math.max(0, Long.parseLong(text));
        } catch (NumberFormatException garbage) {
            return 0;
        }
    }

    private long lastSeqAfter(long seqFloor) {
        long last = 0;
        String after = seqFloor == 0 ? "" : ChainLayout.chainKey(seqFloor);
        while (true) {
            ListPage page = store.list(ChainLayout.CHAIN_PREFIX, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                last = Math.max(last, ChainLayout.chainSeq(entry.key()).orElse(0));
            }
            if (!page.truncated()) {
                return last;
            }
            after = page.lastKey();
        }
    }

    private long lastLsn(ChainObject head) {
        ChainObject current = head;
        while (true) {
            switch (current.body()) {
                case ChainBody.Records records -> {
                    return records.lsnLast();
                }
                case ChainBody.SnapshotRef ref -> {
                    if (current.seq() == 1) {
                        return ref.lsn();
                    }
                    current = chainFetch.require(current.seq() - 1);
                }
            }
        }
    }
}
