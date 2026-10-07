package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainRecords;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.ChainVerifier;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;

import java.util.Optional;

public final class ChainHead {

    public static final String FLOOR_METADATA = "chain-seq-floor";

    private static final int PAGE_KEYS = 1000;

    public record Found(ChainCursor cursor, ChainObject object) {
    }

    private final ObjectStore store;
    private final Keyring keyring;

    public ChainHead(ObjectStore store, Keyring keyring) {
        this.store = store;
        this.keyring = keyring;
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
        ChainObject object = fetch(head);
        long lastLsn = lastLsn(object);
        return Optional.of(new Found(ChainCursor.after(object.seq(), object.digest(), object.epoch(), lastLsn),
                object));
    }

    private long snapshotFloor() {
        long newestLsn = -1;
        String newestKey = null;
        String after = "";
        while (true) {
            ListPage page = store.list(ChainLayout.SNAPSHOT_PREFIX, after, PAGE_KEYS);
            for (ObjectInfo entry : page.entries()) {
                long lsn = ChainLayout.snapshotLsn(entry.key()).orElse(-1);
                if (lsn > newestLsn) {
                    newestLsn = lsn;
                    newestKey = entry.key();
                }
            }
            if (!page.truncated()) {
                break;
            }
            after = page.lastKey();
        }
        if (newestKey == null) {
            return 0;
        }
        return store.head(newestKey).map(info -> parseFloor(info.metadata().get(FLOOR_METADATA))).orElse(0L);
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

    private ChainObject fetch(long seq) {
        byte[] bytes = store.get(ChainLayout.chainKey(seq)).orElseThrow(() -> new ChainTrustException(
                "object " + seq + " was listed but cannot be read"));
        ChainObject object = ChainCodec.decode(bytes);
        if (object.seq() != seq) {
            throw new ChainTrustException("object " + seq + " carries sequence number " + object.seq());
        }
        if (keyring.find(object.header().keyId()).isPresent()) {
            new ChainVerifier(keyring).verifySignature(object);
        }
        if (object.body() instanceof ChainBody.Records records) {
            ChainRecords.verify(records);
        }
        return object;
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
                    current = fetch(current.seq() - 1);
                }
            }
        }
    }
}
