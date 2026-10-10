package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.Keyring;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.SegmentedLog;
import io.nodusdb.log.SyncMode;
import io.nodusdb.log.simulation.LogHarness;
import io.nodusdb.log.simulation.SimulatedDisk;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.ship.OpenReconciler.Local;
import io.nodusdb.ship.OpenReconciler.Start;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

final class ShipperRig implements AutoCloseable {

    record Session(Start start, ShipState state, ShipperCore core, LogFeed feed) {
    }

    private static final long CLAIM_MICROS = 1_700_000_000_000_000L;

    final SimulatedDisk disk = new SimulatedDisk();
    final MemoryObjectStore memory;
    final FaultyObjectStore store;
    final ShipSettings settings;
    final SegmentedLog log;
    LongFunction<String> snapshotContent = lsn -> "snapshot " + lsn;
    private final AtomicLong clock = new AtomicLong();
    private long localEpoch;
    private final AtomicInteger nextObject = new AtomicInteger(1);

    ShipperRig() {
        this(new MemoryObjectStore(), ShipSettings.defaults(), SyncMode.SYNC);
    }

    ShipperRig(MemoryObjectStore memory, ShipSettings settings, SyncMode mode) {
        this.memory = memory;
        this.store = new FaultyObjectStore(memory);
        this.settings = settings;
        try {
            this.log = LogHarness.open(disk, LogConfig.withSyncMode(mode).withSegmentBytes(1 << 14), 0).log();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    long append(int tuples) {
        long lsn = log.append(LogHarness.tuples(nextObject.getAndAdd(tuples), tuples));
        log.awaitDurable(lsn);
        return lsn;
    }

    Session open(long nonce) {
        WriterIdentity identity = new WriterIdentity(ChainBuilder.signingKey(), nonce);
        Start start = reconcile(identity);
        return assemble(identity, start, start.shippedLsn());
    }

    Session openSkipping(long nonce, long skippedLsns) {
        WriterIdentity identity = new WriterIdentity(ChainBuilder.signingKey(), nonce);
        Start start = reconcile(identity);
        return assemble(identity, start, start.shippedLsn() + skippedLsns);
    }

    private Start reconcile(WriterIdentity identity) {
        log.force();
        OpenReconciler reconciler = new OpenReconciler(store, identity, Keyring.empty(), settings, publisher(),
                () -> CLAIM_MICROS);
        Start start = reconciler.reconcile(new Local(log.lastLsn(), localEpoch));
        localEpoch = start.epoch();
        return start;
    }

    private Session assemble(WriterIdentity identity, Start start, long tailAfter) {
        ShipState state = new ShipState(start.epoch(), start.shippedLsn(), start.cursor().seq(),
                settings.backlogCapBytes());
        LogFeed feed = new LogFeed(log, new LogTailReader(disk, tailAfter));
        EpochClaims claims = new EpochClaims(store, identity.nonce(), identity.key().keyId(), () -> CLAIM_MICROS);
        ShipperCore core = new ShipperCore(store, claims, identity, start, feed, state, settings, clock::get);
        return new Session(start, state, core, feed);
    }

    ChainBody.SnapshotRef referenceAt(long lsn) {
        byte[] content = snapshotContent.apply(lsn).getBytes(StandardCharsets.UTF_8);
        String path = ChainLayout.snapshotKey(lsn);
        memory.put(path, content);
        return new ChainBody.SnapshotRef(path, ChainHash.sha256(content), lsn);
    }

    ShipperCore.Next run(Session session) {
        ShipperCore.Next next;
        do {
            next = session.core().step();
            clock.addAndGet(1_000);
        } while (next.cadence() == ShipperCore.Cadence.CONTINUE);
        return next;
    }

    ShipperCore.Next runPastBackoffs(Session session, int maxSteps) {
        ShipperCore.Next next = run(session);
        for (int i = 0; i < maxSteps && next.cadence() == ShipperCore.Cadence.BACKOFF; i++) {
            next = run(session);
        }
        return next;
    }

    private SnapshotPublisher publisher() {
        return floor -> {
            long lsn = log.lastLsn();
            byte[] content = snapshotContent.apply(lsn).getBytes(StandardCharsets.UTF_8);
            String path = ChainLayout.snapshotKey(lsn);
            store.put(path, content);
            return new ChainBody.SnapshotRef(path, ChainHash.sha256(content), lsn);
        };
    }

    @Override
    public void close() {
        log.close();
    }
}
