package io.nodusdb.replica;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.replica.FollowerState.Phase;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.EpochClaims;
import io.nodusdb.ship.ShippingConfig;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriterTakeoverTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);

    @TempDir
    Path root;

    private long highestClaim(ShippingFixture shipping) {
        return new EpochClaims(shipping.store(), 0, 0, () -> 0).highestClaimed();
    }

    @Test
    void aTakeoverFencesTheOldWriterAndContinuesTheGraphInANewEpoch() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path newDirectory = root.resolve("taken");
        try (BucketWriter old = BucketWriter.open(shipping, root.resolve("graph"))) {
            old.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            old.grants("a", 25);
            Token last = old.tuples().write(new TupleTransaction().add("document:z", "viewer", "user:alice"),
                    Durability.LAKE);
            long oldEpoch = old.kernel().epoch();

            WriterTakeover.Taken taken = WriterTakeover.takeOver(shipping.config(), newDirectory, SYNC,
                    GraphKernel.NO_MEMORY_LIMIT);
            try {
                assertEquals(oldEpoch + 1, taken.claimedEpoch());
                assertEquals(oldEpoch + 2, taken.epoch());
                assertEquals(last.lsn(), taken.handoffLsn());
                assertEquals(1, taken.attempts());
                assertEquals(taken.epoch(), taken.kernel().epoch());
                assertTrue(taken.kernel().appliedLsn() > last.lsn());
                assertEquals(GraphDigest.of(old.kernel()), GraphDigest.of(taken.kernel()));

                TupleStore tuples = TupleStore.open(taken.kernel());
                assertTrue(tuples.check("document:z", "view", "user:alice"));
                Token fresh = tuples.write(new TupleTransaction().add("document:new", "viewer", "user:bob"),
                        Durability.LAKE);
                assertEquals(taken.epoch(), fresh.epoch());

                assertThrows(WriterFencedException.class, () -> old.tuples().write(
                        new TupleTransaction().add("document:stale", "viewer", "user:carol"), Durability.LAKE));
            } finally {
                taken.kernel().close();
            }
        }
    }

    @Test
    void aFollowerThatWasOpenBeforeTheTakeoverFollowsTheNewWriterAcrossTheEpoch() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        FollowerConfig.Follow follow = new FollowerConfig.Follow(Duration.ofMillis(10), Duration.ofSeconds(1), null,
                2, null);
        try (BucketWriter old = BucketWriter.open(shipping, root.resolve("graph"));
             FollowerRuntime follower = FollowerRuntime.start(shipping.store(), ChainBuilder.keyring(), follow,
                     GraphKernel.NO_MEMORY_LIMIT)) {
            old.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            Token before = old.tuples().write(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                    Durability.LAKE);
            BucketWriter.await(() -> follower.state().phase() == Phase.CURRENT
                    && follower.kernel().appliedLsn() >= before.lsn(), "the follower to be current");

            WriterTakeover.Taken taken = WriterTakeover.takeOver(shipping.config(), root.resolve("taken"), SYNC,
                    GraphKernel.NO_MEMORY_LIMIT);
            try {
                TupleStore tuples = TupleStore.open(taken.kernel());
                Token after = tuples.write(new TupleTransaction().add("document:b", "viewer", "user:alice"),
                        Durability.LAKE);
                BucketWriter.await(() -> follower.kernel().appliedLsn() >= after.lsn(),
                        "the follower to reach the new writer's write");

                assertEquals(taken.epoch(), follower.kernel().epoch());
                assertTrue(follower.tuples().check("document:b", "view", "user:alice", after));
                assertTrue(follower.tuples().check("document:a", "view", "user:alice"));
                assertFalse(follower.state().terminal());
            } finally {
                taken.kernel().close();
            }
        }
    }

    @Test
    void aNonEmptyDirectoryIsRefusedBeforeAnyEpochIsClaimed() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path occupied = Files.createDirectory(root.resolve("taken"));
        Files.writeString(occupied.resolve("keep"), "keep");
        long claimsBefore;
        try (BucketWriter old = BucketWriter.open(shipping, root.resolve("graph"))) {
            old.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            old.tuples().write(new TupleTransaction().add("document:a", "viewer", "user:alice"), Durability.LAKE);
            claimsBefore = highestClaim(shipping);

            assertThrows(UnsupportedFeatureException.class,
                    () -> WriterTakeover.takeOver(shipping.config(), occupied, SYNC, GraphKernel.NO_MEMORY_LIMIT));

            assertEquals(claimsBefore, highestClaim(shipping));
        }
    }

    @Test
    void anEmptyBucketIsRefusedBeforeAnyEpochIsClaimed() {
        ShippingFixture shipping = ShippingFixture.in(root);

        assertThrows(IllegalStateException.class, () -> WriterTakeover.takeOver(shipping.config(),
                root.resolve("taken"), SYNC, GraphKernel.NO_MEMORY_LIMIT));

        assertEquals(0, highestClaim(shipping));
        assertFalse(Files.exists(root.resolve("taken")));
    }

    @Test
    void aSigningConfigurationWithoutAPublicKeyCannotVerifyTheChainAndIsRefused() {
        ShippingFixture shipping = ShippingFixture.in(root);
        String withoutPublicKey = shipping.json().replaceAll(",\"public_key_file\":\"[^\"]*\"", "");

        assertThrows(IllegalArgumentException.class, () -> WriterTakeover.takeOver(
                ShippingConfig.parse(withoutPublicKey), root.resolve("taken"), SYNC,
                GraphKernel.NO_MEMORY_LIMIT));
    }

    @Test
    void aChainSignedByAnotherKeyIsRefusedBeforeAnyEpochIsClaimed() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        Path otherPrivate = root.resolve("other.pem");
        Path otherPublic = root.resolve("other.pub");
        KeyFiles.generateTo(otherPrivate, otherPublic);
        long claimsBefore;
        try (BucketWriter old = BucketWriter.open(shipping, root.resolve("graph"))) {
            old.tuples().applySchema(SchemaFixtures.DOCUMENTS);
            old.tuples().write(new TupleTransaction().add("document:a", "viewer", "user:alice"), Durability.LAKE);
            claimsBefore = highestClaim(shipping);
            ShippingConfig stranger = shipping.withSigning(otherPrivate, otherPublic).config();

            assertThrows(ChainTrustException.class, () -> WriterTakeover.takeOver(stranger, root.resolve("taken"),
                    SYNC, GraphKernel.NO_MEMORY_LIMIT));

            assertEquals(claimsBefore, highestClaim(shipping));
            assertFalse(Files.exists(root.resolve("taken")));
        }
    }
}
