package io.nodusdb.replica;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.ForwardingObjectStore;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.DurableGraph;
import io.nodusdb.storage.GraphDigest;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RestoreTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final int PARALLELISM = 3;

    @TempDir
    Path root;

    private Restore restoreFrom(ShippingFixture shipping) {
        return new Restore(shipping.store(), ChainBuilder.keyring(), PARALLELISM);
    }

    private GraphDigest digestOfOpened(Path directory) throws IOException {
        GraphKernel opened = DurableGraph.open(directory, SYNC).kernel();
        try {
            return GraphDigest.of(opened);
        } finally {
            opened.close();
        }
    }

    private static boolean siblingsLeftBehind(Path target) throws IOException {
        try (var entries = Files.list(target.getParent())) {
            return entries.anyMatch(path -> path.getFileName().toString().startsWith(
                    target.getFileName() + ".restore-"));
        }
    }

    @Test
    void aRestoredDirectoryOpensToTheWritersGraphAfterACheckpointAndMoreWrites() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path target = root.resolve("restored");
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            TupleStore tuples = bucket.tuples();
            tuples.applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 50);
            bucket.kernel().checkpoint();
            long checkpointLsn = bucket.kernel().appliedLsn();
            bucket.grants("b", 30);
            tuples.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2"));
            Token last = tuples.write(new TupleTransaction().add("document:last", "viewer", "user:alice"),
                    Durability.LAKE);
            bucket.awaitReferenced(checkpointLsn);

            Restore.Restored restored = restoreFrom(shipping).restoreTo(target);

            assertEquals(last.lsn(), restored.appliedLsn());
            assertEquals(checkpointLsn, restored.snapshotLsn());
            assertEquals(bucket.kernel().epoch(), restored.epoch());
            assertEquals(GraphDigest.of(bucket.kernel()), digestOfOpened(target));
            assertFalse(siblingsLeftBehind(target));
        }
    }

    @Test
    void aRestoredGraphAnswersChecksAndAcceptsNewWrites() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path target = root.resolve("restored");
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            bucket.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 40);
            bucket.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            restoreFrom(shipping).restoreTo(target);
        }

        GraphKernel opened = DurableGraph.open(target, SYNC).kernel();
        try {
            TupleStore tuples = TupleStore.open(opened);
            assertTrue(tuples.check("document:z", "view", "user:alice"));
            assertFalse(tuples.check("document:z", "view", "user:bob"));
            tuples.add("document:z", "viewer", "user:bob");
            assertTrue(tuples.check("document:z", "view", "user:bob"));
        } finally {
            opened.close();
        }
    }

    @Test
    void theRestoredGraphKeepsTheWritersSaltAndEpochHistory() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        Path target = root.resolve("restored");
        try (BucketWriter first = BucketWriter.open(shipping, graph)) {
            first.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            first.grants("a", 10);
        }
        SnapshotMeta writerMeta = SnapshotReader.load(graph.resolve("snapshot.bin"), new GraphKernel());
        try (BucketWriter second = BucketWriter.open(shipping, graph)) {
            second.grants("b", 10);
            second.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            restoreFrom(shipping).restoreTo(target);
        }

        SnapshotMeta restoredMeta = SnapshotReader.load(target.resolve("snapshot.bin"), new GraphKernel());
        assertArrayEquals(writerMeta.salt(), restoredMeta.salt());
        GraphKernel opened = DurableGraph.open(target, SYNC).kernel();
        try {
            assertEquals(1, opened.epochHistory().tenureAt(0).epoch());
            assertEquals(2, opened.epochHistory().tenureAt(1).epoch());
            assertEquals(3, opened.epoch());
        } finally {
            opened.close();
        }
    }

    @Test
    void aConfigDocumentNamesTheStoreAndTheTrustedKey() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path target = root.resolve("restored");
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            bucket.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 25);
            bucket.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            Restore.restore(FollowerConfig.parse(shipping.followerJson()), target);

            assertEquals(GraphDigest.of(bucket.kernel()), digestOfOpened(target));
        }
    }

    @Test
    void aNonEmptyTargetIsRefusedBeforeTheBucketIsTouched() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path target = Files.createDirectory(root.resolve("restored"));
        Files.writeString(target.resolve("precious.txt"), "keep");
        AtomicInteger calls = new AtomicInteger();
        ForwardingObjectStore counting = new ForwardingObjectStore(shipping.store()) {
            @Override
            public Optional<byte[]> get(String key) {
                calls.incrementAndGet();
                return super.get(key);
            }

            @Override
            public ListPage list(String prefix, String startAfter, int maxKeys) {
                calls.incrementAndGet();
                return super.list(prefix, startAfter, maxKeys);
            }

            @Override
            public Optional<ObjectInfo> head(String key) {
                calls.incrementAndGet();
                return super.head(key);
            }
        };

        assertThrows(UnsupportedFeatureException.class,
                () -> new Restore(counting, ChainBuilder.keyring(), PARALLELISM).restoreTo(target));

        assertEquals(0, calls.get());
        assertEquals("keep", Files.readString(target.resolve("precious.txt")));
        assertFalse(siblingsLeftBehind(target));
    }

    @Test
    void anEmptyBucketIsRefusedAndLeavesNothingBehind() throws IOException {
        Path target = root.resolve("restored");
        Restore restore = new Restore(new MemoryObjectStore(() -> 1_000L), ChainBuilder.keyring(), PARALLELISM);

        assertThrows(IllegalStateException.class, () -> restore.restoreTo(target));

        assertFalse(Files.exists(target));
        assertFalse(siblingsLeftBehind(target));
    }

    @Test
    void aChainSignedByAnotherKeyIsRefusedAndLeavesNothingBehind() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path target = root.resolve("restored");
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            bucket.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 10);
            bucket.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);
        }
        Keyring stranger = Keyring.single(ChainBuilder.KEY_ID, KeyFiles.generate().getPublic());
        Restore restore = new Restore(shipping.store(), stranger, PARALLELISM);

        assertThrows(ChainTrustException.class, () -> restore.restoreTo(target));

        assertFalse(Files.exists(target));
        assertFalse(siblingsLeftBehind(target));
    }

    @Test
    void aKeyringThatLacksTheSigningKeyIsRefusedInsteadOfTrustingUnverifiedObjects() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path target = root.resolve("restored");
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            bucket.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 10);
            bucket.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);
        }
        Restore restore = new Restore(shipping.store(), Keyring.empty(), PARALLELISM);

        assertThrows(ChainTrustException.class, () -> restore.restoreTo(target));

        assertFalse(Files.exists(target));
    }

    @Test
    void aLeftoverScratchDirectoryFromACrashDoesNotBlockTheRestore() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path target = root.resolve("restored");
        Path scratch = Files.createDirectory(root.resolve("restored.restore-scratch"));
        Files.writeString(scratch.resolve("snapshot-1.part"), "half a download");
        try (BucketWriter bucket = BucketWriter.open(shipping, root.resolve("graph"))) {
            bucket.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            bucket.grants("a", 10);
            bucket.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"), Durability.LAKE);

            restoreFrom(shipping).restoreTo(target);

            assertEquals(GraphDigest.of(bucket.kernel()), digestOfOpened(target));
        }
        assertFalse(Files.exists(scratch));
    }

    @Test
    void aMissingTrustKeyFileIsReportedByName() {
        ShippingFixture shipping = ShippingFixture.in(root);
        String json = shipping.followerJson().replace("signing.pub", "missing.pub");

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> Restore.restore(FollowerConfig.parse(json), root.resolve("restored")));

        assertTrue(refused.getMessage().contains("missing.pub"), refused.getMessage());
    }
}
