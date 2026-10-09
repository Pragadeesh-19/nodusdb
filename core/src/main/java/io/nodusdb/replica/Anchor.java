package io.nodusdb.replica;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainObject;

public record Anchor(String snapshotKey, ChainObject reference, long firstSeq) {

    public long snapshotLsn() {
        return ((ChainBody.SnapshotRef) reference.body()).lsn();
    }

    public ChainBody.SnapshotRef snapshot() {
        return (ChainBody.SnapshotRef) reference.body();
    }
}
