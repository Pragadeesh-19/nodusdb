package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;

public interface SnapshotPublisher {

    ChainBody.SnapshotRef publish(long chainSeqFloor);
}
