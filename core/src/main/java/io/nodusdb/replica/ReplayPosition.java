package io.nodusdb.replica;

import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;

public final class ReplayPosition {

    private final ChainCursor cursor;
    private final long pinnedSeq;
    private final ChainHash pinnedDigest;
    private long appliedLsn;

    ReplayPosition(ChainCursor cursor, long appliedLsn, ChainObject pinned) {
        this.cursor = cursor;
        this.appliedLsn = appliedLsn;
        this.pinnedSeq = pinned.seq();
        this.pinnedDigest = pinned.digest();
    }

    public ChainCursor cursor() {
        return cursor;
    }

    public long appliedLsn() {
        return appliedLsn;
    }

    void applied(long lsn) {
        appliedLsn = lsn;
    }

    void requirePinnedUnchanged(ChainObject object) {
        if (object.seq() == pinnedSeq && !object.digest().equals(pinnedDigest)) {
            throw new ChainTrustException("object " + pinnedSeq + " changed while the replica was starting");
        }
    }
}
