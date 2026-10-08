package io.nodusdb.storage;

import io.nodusdb.iceberg.DataFileWriter;
import io.nodusdb.iceberg.IcebergTable;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.projection.EdgeLogProjector;
import io.nodusdb.projection.KernelNameResolver;
import io.nodusdb.projection.ProjectionSettings;
import io.nodusdb.projection.ProjectorRunner;
import io.nodusdb.projection.StoreChainSource;
import io.nodusdb.ship.ChainRetention;
import io.nodusdb.ship.EpochClaims;
import io.nodusdb.ship.LogFeed;
import io.nodusdb.ship.OpenReconciler;
import io.nodusdb.ship.RetentionRunner;
import io.nodusdb.ship.ShipSettings;
import io.nodusdb.ship.ShipState;
import io.nodusdb.ship.ShipperCore;
import io.nodusdb.ship.ShipperRunner;
import io.nodusdb.ship.ShippingBootstrap.Reconciled;
import io.nodusdb.ship.ShippingConfig;
import io.nodusdb.ship.SnapshotShipper;
import io.nodusdb.ship.SnapshotUploader;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class ShippingRuntime implements AutoCloseable {

    static final Duration MIN_RETENTION_INTERVAL = Duration.ofMinutes(1);
    static final Duration MAX_RETENTION_INTERVAL = Duration.ofHours(1);

    private static final int RETENTION_SWEEPS_PER_WINDOW = 24;

    private final Reconciled reconciled;
    private final ShipState state;
    private final SnapshotRelay relay;
    private final List<Runnable> closers = new ArrayList<>();
    private boolean closed;

    private ShippingRuntime(Reconciled reconciled, SnapshotStaging staging) {
        ShipSettings settings = reconciled.config().ship();
        OpenReconciler.Start start = reconciled.start();
        this.reconciled = reconciled;
        this.state = new ShipState(start.epoch(), start.shippedLsn(), start.cursor().seq(),
                settings.backlogCapBytes());
        SnapshotShipper snapshotShipper = SnapshotShipper.start(new SnapshotUploader(reconciled.store()), state,
                settings, "nodus-snapshot-shipper");
        closers.add(snapshotShipper::close);
        this.relay = new SnapshotRelay(staging, snapshotShipper, state, reconciled.newestSnapshotLsn());
    }

    static ShippingRuntime begin(Reconciled reconciled, SnapshotStaging staging) {
        return new ShippingRuntime(reconciled, staging);
    }

    static Duration retentionInterval(ShipSettings settings) {
        Duration sweep = settings.retention().dividedBy(RETENTION_SWEEPS_PER_WINDOW);
        if (sweep.compareTo(MIN_RETENTION_INTERVAL) < 0) {
            return MIN_RETENTION_INTERVAL;
        }
        return sweep.compareTo(MAX_RETENTION_INTERVAL) > 0 ? MAX_RETENTION_INTERVAL : sweep;
    }

    ShipState state() {
        return state;
    }

    CheckpointListener checkpointListener() {
        return relay;
    }

    synchronized void startShipping(GraphKernel kernel, LogStore log, LogFileSystem files, Path scratch) {
        ShippingConfig config = reconciled.config();
        ShipSettings settings = config.ship();
        OpenReconciler.Start start = reconciled.start();
        LogFeed feed = new LogFeed(log, new LogTailReader(files, start.shippedLsn()), state::durabilityWanted);
        EpochClaims claims = new EpochClaims(reconciled.store(), reconciled.identity().nonce(),
                reconciled.identity().key().keyId(), DurableGraph::wallClockMicros);
        ShipperCore core = new ShipperCore(reconciled.store(), claims, reconciled.identity(), start, feed, state,
                settings, System::nanoTime);
        closers.add(ShipperRunner.start(core, state, feed, settings, "nodus-shipper")::close);
        if (config.iceberg().enabled()) {
            closers.add(startProjection(kernel, scratch)::close);
        }
        RetentionRunner retention = RetentionRunner.start(
                new ChainRetention(reconciled.store(), settings.retention(), System::currentTimeMillis), state,
                config.iceberg().enabled() ? state::projectedSeq : () -> Long.MAX_VALUE,
                retentionInterval(settings), "nodus-retention");
        closers.add(retention::close);
    }

    private ProjectorRunner startProjection(GraphKernel kernel, Path scratch) {
        ShippingConfig.Iceberg iceberg = reconciled.config().iceberg();
        ProjectionSettings projection = ProjectionSettings.defaults()
                .withCommitInterval(iceberg.commitInterval())
                .withSnapshotRetention(iceberg.tableRetention());
        IcebergTable table = new IcebergTable(reconciled.store(), reconciled.icebergLocation(), "iceberg/",
                System::currentTimeMillis);
        DataFileWriter writer = new DataFileWriter(reconciled.store(), table, scratch, projection.codec());
        EdgeLogProjector projector = new EdgeLogProjector(
                new StoreChainSource(state.ring(), reconciled.store(), reconciled.keys().keyring()), state, table,
                writer, new KernelNameResolver(kernel), reconciled.keys().signing(), projection,
                new EdgeLogProjector.Clocks(System::nanoTime, System::currentTimeMillis));
        return ProjectorRunner.start(projector, state, projection, "nodus-projector");
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        closers.add(state::closed);
        closers.add(reconciled.store()::close);
        RuntimeException failure = null;
        for (Runnable closer : closers) {
            failure = run(closer, failure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException run(Runnable closer, RuntimeException failure) {
        try {
            closer.run();
            return failure;
        } catch (RuntimeException e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
            return failure;
        }
    }
}
