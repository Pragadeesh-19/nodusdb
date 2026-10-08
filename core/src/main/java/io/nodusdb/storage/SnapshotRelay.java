package io.nodusdb.storage;

import io.nodusdb.ship.ShipState;
import io.nodusdb.ship.SnapshotShipper;
import io.nodusdb.ship.StagedSnapshot;

import java.io.IOException;
import java.nio.file.Path;

final class SnapshotRelay implements CheckpointListener {

    private final SnapshotStaging staging;
    private final SnapshotShipper shipper;
    private final ShipState state;
    private long newestLsn;

    SnapshotRelay(SnapshotStaging staging, SnapshotShipper shipper, ShipState state, long newestLsn) {
        this.staging = staging;
        this.shipper = shipper;
        this.state = state;
        this.newestLsn = newestLsn;
    }

    @Override
    public synchronized void checkpointed(Path snapshot, long lsn) {
        if (lsn <= newestLsn) {
            return;
        }
        try {
            StagedSnapshot staged = staging.link(snapshot, lsn);
            newestLsn = lsn;
            shipper.submit(staged);
        } catch (IOException | IllegalStateException failure) {
            state.referenceFailed("the checkpoint at LSN " + lsn + " could not be handed to the shipper: "
                    + failure.getMessage());
        }
    }
}
