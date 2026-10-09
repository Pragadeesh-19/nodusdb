package io.nodusdb.replica;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainAudit;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.DurableGraph;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainReplayEndToEndTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);

    @TempDir
    Path root;

    private GraphKernel openWriter(ShippingFixture shipping, Path graph) throws IOException {
        return DurableGraph.open(graph, SYNC, GraphKernel.NO_MEMORY_LIMIT, shipping.config()).kernel();
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
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

    private static boolean referenced(ObjectStore store, long lsn) {
        for (String key : ChainAudit.chainKeys(store)) {
            if (ChainCodec.decode(store.get(key).orElseThrow()).body() instanceof ChainBody.SnapshotRef ref
                    && ref.lsn() == lsn) {
                return true;
            }
        }
        return false;
    }

    private static void grants(TupleStore store, String prefix, int count) {
        for (int i = 0; i < count; i++) {
            store.add("document:" + prefix + i, "viewer", "user:u" + (i % 7));
        }
    }

    private Replayed replicate(ShippingFixture shipping) throws IOException {
        ObjectStore store = shipping.store();
        ChainFetch fetch = new ChainFetch(store, ChainBuilder.keyring(), Trust.REQUIRED);
        ChainReplay replay = new ChainReplay(store, fetch, new SnapshotDownloader(store, 3), root);
        KernelSink sink = new KernelSink();
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);
        return new Replayed(sink, position);
    }

    private record Replayed(KernelSink sink, ReplayPosition position) {
    }

    @Test
    void aBootstrapFromTheBucketEqualsTheWriterAfterACheckpointAndMoreWrites() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = openWriter(shipping, root.resolve("graph"));
        try {
            TupleStore tuples = TupleStore.open(writer);
            tuples.applySchema(SchemaFixtures.DOCUMENTS);
            grants(tuples, "a", 50);
            writer.checkpoint();
            long checkpointLsn = writer.appliedLsn();
            grants(tuples, "b", 30);
            tuples.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2"));
            Token last = tuples.write(new TupleTransaction().add("document:last", "viewer", "user:alice"),
                    Durability.LAKE);
            await(() -> referenced(shipping.store(), checkpointLsn), "the checkpoint reference");

            Replayed replicated = replicate(shipping);

            assertEquals(GraphDigest.of(writer), GraphDigest.of(replicated.sink().kernel));
            assertEquals(last.lsn(), replicated.position().appliedLsn());
            TupleStore copy = TupleStore.open(replicated.sink().kernel);
            assertEquals(2, copy.schemaVersion());
            assertTrue(copy.check("document:a0", "view", "user:u0"));
            assertTrue(copy.check("document:b3", "view", "user:u3"));
            assertTrue(copy.check("document:last", "view", "user:alice"));
            assertFalse(copy.check("document:a0", "view", "user:nobody"));
            assertEquals(writer.epochHistory().latestEpoch(), replicated.sink().kernel.epochHistory().latestEpoch());
        } finally {
            writer.close();
        }
    }

    @Test
    void aBootstrapFromTheOpeningSnapshotAloneReplaysEverythingAfterIt() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = openWriter(shipping, root.resolve("graph"));
        try {
            TupleStore tuples = TupleStore.open(writer);
            tuples.applySchema(SchemaFixtures.DOCUMENTS);
            grants(tuples, "a", 40);
            tuples.write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            Replayed replicated = replicate(shipping);

            assertEquals(GraphDigest.of(writer), GraphDigest.of(replicated.sink().kernel));
            assertTrue(TupleStore.open(replicated.sink().kernel).check("document:z", "view", "user:alice"));
        } finally {
            writer.close();
        }
    }

    @Test
    void aBootstrapSpansAWriterRestartAndItsNewEpoch() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel first = openWriter(shipping, graph);
        TupleStore firstTuples = TupleStore.open(first);
        firstTuples.applySchema(SchemaFixtures.DOCUMENTS);
        grants(firstTuples, "a", 20);
        first.close();

        GraphKernel second = openWriter(shipping, graph);
        try {
            TupleStore secondTuples = TupleStore.open(second);
            grants(secondTuples, "b", 20);
            secondTuples.write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            Replayed replicated = replicate(shipping);

            assertEquals(GraphDigest.of(second), GraphDigest.of(replicated.sink().kernel));
            assertEquals(2, replicated.sink().kernel.epochHistory().size());
            assertEquals(second.epoch(), replicated.sink().kernel.epochHistory().latestEpoch());
        } finally {
            second.close();
        }
    }
}
