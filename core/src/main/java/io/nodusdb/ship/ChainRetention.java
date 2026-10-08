package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

public final class ChainRetention {

    public record Reference(long seq, long lsn) {
    }

    public record Result(int chainObjects, int snapshots, int abandonedUploads) {
    }

    public static final long NO_PROJECTOR = Long.MAX_VALUE;

    private static final int PAGE_KEYS = 1000;
    private static final Duration STALE_UPLOAD_AGE = Duration.ofDays(1);
    private static final String UPLOAD_PREFIX = "_nodus/";

    private final ObjectStore store;
    private final Duration retention;
    private final LongSupplier clockMillis;

    public ChainRetention(ObjectStore store, Duration retention, LongSupplier clockMillis) {
        this.store = store;
        this.retention = retention;
        this.clockMillis = clockMillis;
    }

    public Result sweep(Reference newest, long projectedSeq) {
        long now = clockMillis.getAsLong();
        long cutoff = now - retention.toMillis();
        long keepFrom = Math.min(lastRecordsBefore(newest.seq()), projectedSeq);
        int chainObjects = deleteChainPrefix(keepFrom, cutoff);
        int snapshots = deleteSnapshots(newest.lsn(), cutoff);
        int uploads = store.abortStaleUploads(UPLOAD_PREFIX, STALE_UPLOAD_AGE, Instant.ofEpochMilli(now));
        return new Result(chainObjects, snapshots, uploads);
    }

    private long lastRecordsBefore(long referenceSeq) {
        long seq = referenceSeq;
        while (seq > 1) {
            Optional<byte[]> bytes = store.get(ChainLayout.chainKey(seq - 1));
            if (bytes.isEmpty()) {
                return seq;
            }
            if (ChainCodec.decode(bytes.get()).body() instanceof ChainBody.Records) {
                return seq - 1;
            }
            seq--;
        }
        return 1;
    }

    private int deleteChainPrefix(long keepFrom, long cutoff) {
        int deleted = 0;
        String after = "";
        while (true) {
            ListPage page = store.list(ChainLayout.CHAIN_PREFIX, after, PAGE_KEYS);
            List<String> doomed = new ArrayList<>();
            boolean reachedKept = false;
            for (ObjectInfo entry : page.entries()) {
                OptionalLong seq = ChainLayout.chainSeq(entry.key());
                if (seq.isEmpty()) {
                    continue;
                }
                if (seq.getAsLong() >= keepFrom || entry.lastModifiedMillis() >= cutoff) {
                    reachedKept = true;
                    break;
                }
                doomed.add(entry.key());
            }
            if (!doomed.isEmpty()) {
                store.deleteAll(doomed);
                deleted += doomed.size();
            }
            if (reachedKept || !page.truncated()) {
                return deleted;
            }
            after = page.lastKey();
        }
    }

    private int deleteSnapshots(long newestLsn, long cutoff) {
        int deleted = 0;
        String after = "";
        while (true) {
            ListPage page = store.list(ChainLayout.SNAPSHOT_PREFIX, after, PAGE_KEYS);
            List<String> doomed = new ArrayList<>();
            for (ObjectInfo entry : page.entries()) {
                OptionalLong lsn = ChainLayout.snapshotLsn(entry.key());
                if (lsn.isPresent() && lsn.getAsLong() < newestLsn && entry.lastModifiedMillis() < cutoff) {
                    doomed.add(entry.key());
                }
            }
            if (!doomed.isEmpty()) {
                store.deleteAll(doomed);
                deleted += doomed.size();
            }
            if (!page.truncated()) {
                return deleted;
            }
            after = page.lastKey();
        }
    }
}
