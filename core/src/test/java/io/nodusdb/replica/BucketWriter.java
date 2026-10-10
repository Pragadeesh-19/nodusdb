package io.nodusdb.replica;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainAudit;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.DurableGraph;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

final class BucketWriter implements AutoCloseable {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final long WAIT_SECONDS = 20;

    private final ShippingFixture shipping;
    private final GraphKernel kernel;
    private final TupleStore tuples;

    private BucketWriter(ShippingFixture shipping, GraphKernel kernel) {
        this.shipping = shipping;
        this.kernel = kernel;
        this.tuples = TupleStore.open(kernel);
    }

    static BucketWriter open(ShippingFixture shipping, Path graph) throws IOException {
        GraphKernel kernel = DurableGraph.open(graph, SYNC, GraphKernel.NO_MEMORY_LIMIT, shipping.config()).kernel();
        return new BucketWriter(shipping, kernel);
    }

    GraphKernel kernel() {
        return kernel;
    }

    TupleStore tuples() {
        return tuples;
    }

    void grants(String prefix, int count) {
        for (int i = 0; i < count; i++) {
            tuples.add("document:" + prefix + i, "viewer", "user:u" + i % 7);
        }
    }

    void awaitReferenced(long lsn) {
        ObjectStore store = shipping.store();
        await(() -> referenced(store, lsn), "the checkpoint reference for LSN " + lsn);
    }

    static boolean referenced(ObjectStore store, long lsn) {
        for (String key : ChainAudit.chainKeys(store)) {
            if (ChainCodec.decode(store.get(key).orElseThrow()).body() instanceof ChainBody.SnapshotRef ref
                    && ref.lsn() == lsn) {
                return true;
            }
        }
        return false;
    }

    static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for " + what);
            }
        }
    }

    @Override
    public void close() {
        kernel.close();
    }
}
