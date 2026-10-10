package io.nodusdb.replica;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainReplayEndToEndTest {

    @TempDir
    Path root;

    private Replayed replicate(ShippingFixture shipping) throws IOException {
        ObjectStore store = shipping.store();
        ChainFetch fetch = new ChainFetch(store, ChainBuilder.keyring(), Trust.REQUIRED);
        ChainReplay replay = new ChainReplay(store, fetch, new SnapshotDownloader(store, 3), root);
        KernelReplicaSink sink = new KernelReplicaSink(GraphKernel.NO_MEMORY_LIMIT);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow();
        replay.catchUp(position, sink);
        return new Replayed(sink, position);
    }

    private record Replayed(KernelReplicaSink sink, ReplayPosition position) {
    }

    @Test
    void aBootstrapFromTheBucketEqualsTheWriterAfterACheckpointAndMoreWrites() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            GraphKernel writer = bucket.kernel();
            TupleStore tuples = bucket.tuples();
            tuples.applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 50);
            writer.checkpoint();
            long checkpointLsn = writer.appliedLsn();
            bucket.grants("b", 30);
            tuples.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2"));
            Token last = tuples.write(new TupleTransaction().add("document:last", "viewer", "user:alice"),
                    Durability.LAKE);
            bucket.awaitReferenced(checkpointLsn);

            Replayed replicated = replicate(shipping);

            assertEquals(GraphDigest.of(writer), GraphDigest.of(replicated.sink().kernel()));
            assertEquals(last.lsn(), replicated.position().appliedLsn());
            TupleStore copy = TupleStore.open(replicated.sink().kernel());
            assertEquals(2, copy.schemaVersion());
            assertTrue(copy.check("document:a0", "view", "user:u0"));
            assertTrue(copy.check("document:b3", "view", "user:u3"));
            assertTrue(copy.check("document:last", "view", "user:alice"));
            assertFalse(copy.check("document:a0", "view", "user:nobody"));
            assertEquals(writer.epochHistory().latestEpoch(), replicated.sink().kernel().epochHistory().latestEpoch());
        }
    }

    @Test
    void aBootstrapFromTheOpeningSnapshotAloneReplaysEverythingAfterIt() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            bucket.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 40);
            bucket.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            Replayed replicated = replicate(shipping);

            assertEquals(GraphDigest.of(bucket.kernel()), GraphDigest.of(replicated.sink().kernel()));
            assertTrue(TupleStore.open(replicated.sink().kernel()).check("document:z", "view", "user:alice"));
        }
    }

    @Test
    void aBootstrapSpansAWriterRestartAndItsNewEpoch() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        try (BucketWriter first = BucketWriter.open(shipping, graph)) {
            first.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            first.grants("a", 20);
        }

        try (BucketWriter second = BucketWriter.open(shipping, graph)) {
            second.grants("b", 20);
            second.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            Replayed replicated = replicate(shipping);

            assertEquals(GraphDigest.of(second.kernel()), GraphDigest.of(replicated.sink().kernel()));
            assertEquals(2, replicated.sink().kernel().epochHistory().size());
            assertEquals(second.kernel().epoch(), replicated.sink().kernel().epoch());
        }
    }
}
