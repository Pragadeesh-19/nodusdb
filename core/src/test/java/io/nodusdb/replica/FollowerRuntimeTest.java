package io.nodusdb.replica;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.objectstore.DirectoryObjectStore;
import io.nodusdb.replica.FollowerState.Phase;
import io.nodusdb.ship.ChainAudit;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FollowerRuntimeTest {

    private static final Duration POLL = Duration.ofMillis(10);

    @TempDir
    Path root;

    private static FollowerConfig.Follow follow(Path stateDirectory) {
        return new FollowerConfig.Follow(POLL, Duration.ofSeconds(1), null, 2, stateDirectory);
    }

    private static FollowerRuntime start(ShippingFixture shipping, Path stateDirectory, long limit)
            throws IOException {
        return FollowerRuntime.start(shipping.store(), ChainBuilder.keyring(), follow(stateDirectory), limit);
    }

    private static void awaitApplied(FollowerRuntime runtime, long lsn) {
        try {
            BucketWriter.await(() -> runtime.state().phase() != Phase.BOOTSTRAPPING
                    && runtime.kernel().appliedLsn() >= lsn, "the follower to apply LSN " + lsn);
        } catch (AssertionError timedOut) {
            throw new AssertionError(timedOut.getMessage() + "; follower state "
                    + runtime.state().snapshot(System.nanoTime()) + "; events " + runtime.state().recentEvents(),
                    timedOut);
        }
    }

    private static void awaitPhase(FollowerRuntime runtime, Phase phase) {
        BooleanSupplier reached = () -> runtime.state().phase() == phase;
        BucketWriter.await(reached, "the follower to reach " + phase);
    }

    @Test
    void aFollowerStartedBeforeTheWriterShipsAnythingWaitsAndThenCatchesUp() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        try (FollowerRuntime follower = start(shipping, null, GraphKernel.NO_MEMORY_LIMIT)) {
            BucketWriter.await(() -> !follower.state().recentEvents().isEmpty(), "the waiting note");
            assertEquals(Phase.BOOTSTRAPPING, follower.state().phase());

            try (BucketWriter writer = BucketWriter.open(shipping, root.resolve("graph"))) {
                writer.tuples().applySchema(SchemaFixtures.DOCUMENTS);
                writer.grants("a", 30);
                Token last = writer.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"),
                        Durability.LAKE);

                awaitApplied(follower, last.lsn());

                assertEquals(GraphDigest.of(writer.kernel()), GraphDigest.of(follower.kernel()));
                assertTrue(follower.tuples().check("document:z", "view", "user:alice"));
                assertTrue(follower.tuples().check("document:a3", "view", "user:u3"));
                assertFalse(follower.tuples().check("document:z", "view", "user:bob"));
            }
        }
    }

    @Test
    void aFollowerTracksALiveWriterThroughASchemaMigrationWithoutBeingReopened() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        try (BucketWriter writer = BucketWriter.open(shipping, root.resolve("graph"));
             FollowerRuntime follower = start(shipping, null, GraphKernel.NO_MEMORY_LIMIT)) {
            TupleStore tuples = writer.tuples();
            tuples.applySchema(SchemaFixtures.DOCUMENTS);
            writer.grants("a", 20);
            Token first = tuples.write(new TupleTransaction().add("document:plan", "viewer", "user:alice"),
                    Durability.LAKE);
            awaitApplied(follower, first.lsn());
            assertEquals(1, follower.tuples().schemaVersion());

            tuples.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2")
                    .replace("  permission view = viewer + editor + parent->view",
                            "  relation owner: user\n  permission view = viewer + editor + owner + parent->view"));
            Token second = tuples.write(new TupleTransaction().add("document:plan", "owner", "user:bob"),
                    Durability.LAKE);
            awaitApplied(follower, second.lsn());

            assertEquals(2, follower.tuples().schemaVersion());
            assertTrue(follower.tuples().check("document:plan", "view", "user:bob", second));
            assertTrue(follower.tuples().check("document:plan", "view", "user:alice"));
            awaitPhase(follower, Phase.CURRENT);
        }
    }

    @Test
    void aFollowerFollowsAWriterAcrossARestartAndItsNewEpoch() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        try (FollowerRuntime follower = start(shipping, null, GraphKernel.NO_MEMORY_LIMIT)) {
            try (BucketWriter first = BucketWriter.open(shipping, graph)) {
                first.tuples().applySchema(SchemaFixtures.DOCUMENTS);
                first.grants("a", 15);
            }
            try (BucketWriter second = BucketWriter.open(shipping, graph)) {
                second.grants("b", 15);
                Token last = second.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"),
                        Durability.LAKE);

                awaitApplied(follower, last.lsn());

                assertEquals(GraphDigest.of(second.kernel()), GraphDigest.of(follower.kernel()));
                assertEquals(2, follower.kernel().epochHistory().size());
                assertEquals(second.kernel().epoch(), follower.kernel().epoch());
                assertEquals(last, follower.kernel().token());
            }
        }
    }

    @Test
    void aFollowerRefusesEveryWrite() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        try (BucketWriter writer = BucketWriter.open(shipping, root.resolve("graph"));
             FollowerRuntime follower = start(shipping, null, GraphKernel.NO_MEMORY_LIMIT)) {
            writer.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            Token last = writer.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"),
                    Durability.LAKE);
            awaitApplied(follower, last.lsn());

            assertThrows(UnsupportedFeatureException.class,
                    () -> follower.tuples().add("document:y", "viewer", "user:alice"));
            assertThrows(UnsupportedFeatureException.class, () -> follower.kernel().addEdge(1, 2));
            assertEquals(last.lsn(), follower.kernel().appliedLsn());
        }
    }

    @Test
    void aFollowerWithATinyMemoryLimitStallsWithAReasonAndKeepsServingItsPrefix() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        long footprint = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT).memoryUsedBytes();
        try (BucketWriter writer = BucketWriter.open(shipping, root.resolve("graph"));
             FollowerRuntime follower = start(shipping, null, footprint + 60_000)) {
            writer.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            writer.grants("a", 2_500);
            Token last = writer.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"),
                    Durability.LAKE);

            awaitPhase(follower, Phase.STALLED);

            FollowerState.Snapshot snapshot = follower.state().snapshot(System.nanoTime());
            assertTrue(snapshot.stallReason().contains("memory limit"), snapshot.stallReason());
            assertTrue(follower.kernel().appliedLsn() < last.lsn());
            assertTrue(follower.kernel().appliedLsn() > 0);
            assertTrue(follower.tuples().schemaVersion() >= 1);
        }
    }

    @Test
    void aRestartedFollowerRefusesABucketThatWasRolledBack() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path stateDirectory = root.resolve("state");
        long rememberedSeq;
        try (BucketWriter writer = BucketWriter.open(shipping, root.resolve("graph"))) {
            writer.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            try (FollowerRuntime follower = start(shipping, stateDirectory, GraphKernel.NO_MEMORY_LIMIT)) {
                for (int i = 0; i < 4; i++) {
                    writer.grants("round" + i + "-", 5);
                    writer.tuples().write(new TupleTransaction().add("document:r" + i, "viewer", "user:alice"),
                            Durability.LAKE);
                }
                Token last = writer.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"),
                        Durability.LAKE);
                awaitApplied(follower, last.lsn());
                awaitPhase(follower, Phase.CURRENT);
            }
            rememberedSeq = AntiRollbackMarker.in(stateDirectory).remembered().orElseThrow().seq();
        }
        DirectoryObjectStore bucket = shipping.store();
        List<String> keys = ChainAudit.chainKeys(bucket);
        for (String key : keys) {
            if (ChainLayout.chainSeq(key).orElse(0) >= rememberedSeq) {
                bucket.delete(key);
            }
        }

        try (FollowerRuntime restarted = start(shipping, stateDirectory, GraphKernel.NO_MEMORY_LIMIT)) {
            awaitPhase(restarted, Phase.STALLED);

            assertTrue(restarted.state().snapshot(System.nanoTime()).stallReason().contains("rolled back"));
        }
    }

    @Test
    void aDamagedMarkerStopsTheStartAndNamesTheFile() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path stateDirectory = Files.createDirectories(root.resolve("state"));
        Path marker = stateDirectory.resolve(AntiRollbackMarker.FILE_NAME);
        Files.write(marker, new byte[] {9, 9, 9});

        io.nodusdb.error.CorruptLogException refused = assertThrows(io.nodusdb.error.CorruptLogException.class,
                () -> start(shipping, stateDirectory, GraphKernel.NO_MEMORY_LIMIT));

        assertTrue(refused.getMessage().contains(marker.toString()), refused.getMessage());
    }

    @Test
    void closingTwiceIsHarmlessAndTheStateEndsClosed() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        FollowerRuntime follower = start(shipping, null, GraphKernel.NO_MEMORY_LIMIT);

        follower.close();
        follower.close();

        assertEquals(Phase.CLOSED, follower.state().phase());
    }

    @Test
    void theScratchDirectoryUnderTheStateDirectoryIsRemovedOnClose() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path stateDirectory = root.resolve("state");
        FollowerRuntime follower = start(shipping, stateDirectory, GraphKernel.NO_MEMORY_LIMIT);

        follower.close();

        assertFalse(Files.exists(stateDirectory.resolve("scratch")));
    }
}
