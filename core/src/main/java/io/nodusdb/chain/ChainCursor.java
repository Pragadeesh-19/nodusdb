package io.nodusdb.chain;

import java.util.Objects;

public final class ChainCursor {

    private static final long UNKNOWN_LSN = -1;

    private long seq;
    private ChainHash digest;
    private long epoch;
    private long lastLsn;

    private ChainCursor(long seq, ChainHash digest, long epoch, long lastLsn) {
        this.seq = seq;
        this.digest = digest;
        this.epoch = epoch;
        this.lastLsn = lastLsn;
    }

    public static ChainCursor beforeFirst() {
        return new ChainCursor(0, ChainHash.ZERO, 0, UNKNOWN_LSN);
    }

    public static ChainCursor after(long seq, ChainHash digest, long epoch, long lastLsn) {
        if (seq < 1 || epoch < 0 || lastLsn < 0) {
            throw new IllegalArgumentException("a cursor after an object needs seq >= 1, epoch >= 0, lsn >= 0");
        }
        return new ChainCursor(seq, Objects.requireNonNull(digest, "digest"), epoch, lastLsn);
    }

    public long seq() {
        return seq;
    }

    public ChainHash digest() {
        return digest;
    }

    public long epoch() {
        return epoch;
    }

    public long lastLsn() {
        return lastLsn;
    }

    public boolean atStart() {
        return seq == 0;
    }

    public void accept(ChainObject object) {
        ChainHeader header = object.header();
        if (header.seq() != seq + 1) {
            throw new ChainTrustException("expected object " + (seq + 1) + " but found " + header.seq());
        }
        if (!header.prev().equals(digest)) {
            throw new ChainTrustException("object " + header.seq() + " does not extend object " + seq);
        }
        if (header.epoch() < epoch) {
            throw new ChainTrustException("object " + header.seq() + " carries epoch " + header.epoch()
                    + " after epoch " + epoch);
        }
        long nextLsn = advance(object);
        seq = header.seq();
        digest = object.digest();
        epoch = header.epoch();
        lastLsn = nextLsn;
    }

    private long advance(ChainObject object) {
        return switch (object.body()) {
            case ChainBody.SnapshotRef ref -> lastLsn == UNKNOWN_LSN ? ref.lsn() : lastLsn;
            case ChainBody.Records records -> {
                if (seq == 0) {
                    throw new ChainTrustException("a chain starts with a snapshot reference");
                }
                if (records.lsnFirst() != lastLsn + 1) {
                    throw new ChainTrustException("object " + object.seq() + " starts at LSN " + records.lsnFirst()
                            + " but LSN " + (lastLsn + 1) + " was expected");
                }
                yield records.lsnLast();
            }
        };
    }
}
