package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.ship.ShippingFixture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionFollowerTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);

    @TempDir
    Path root;

    private final GraphSessions sessions = new GraphSessions();

    private GraphSession writer(ShippingFixture fixture) throws IOException {
        long handle = sessions.openDurable(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT, fixture.config());
        return sessions.get(handle);
    }

    private GraphSession follower(ShippingFixture fixture) throws IOException {
        String json = fixture.followerJson().replace("}}", "},\"follow\":{\"poll_interval_ms\":10,"
                + "\"read_wait_ms\":5000}}");
        long handle = sessions.openFollower(FollowerConfig.parse(json), GraphKernel.NO_MEMORY_LIMIT);
        return sessions.get(handle);
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
                throw new AssertionError("interrupted");
            }
        }
    }

    private static long appliedLsn(GraphSession session) {
        return JsonParser.parseObject(session.statsJson()).requireLong("applied_lsn");
    }

    private static String phase(GraphSession session) {
        return JsonParser.parseObject(session.statsJson()).requireObject("follower").requireString("phase");
    }

    @Test
    void readsAreRefusedUntilTheFollowerHasLoadedASnapshotAndThenAnswer() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession follower = follower(fixture);
        try {
            assertThrows(StaleReadException.class, () -> follower.check("document:a", "view", "user:alice", null));
            assertThrows(StaleReadException.class, () -> follower.hasEdge(1, 2));
            assertEquals("BOOTSTRAPPING", phase(follower));

            GraphSession writer = writer(fixture);
            try {
                writer.applySchema(SchemaFixtures.DOCUMENTS);
                Token token = writer.writeTuples(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                        Durability.LAKE);

                assertTrue(follower.check("document:a", "view", "user:alice", token));
                assertFalse(follower.check("document:a", "view", "user:bob", token));
                assertEquals(1, follower.schemaVersion());
                assertEquals(token, follower.token());
            } finally {
                writer.close();
            }
        } finally {
            follower.close();
        }
    }

    @Test
    void aCheckWithATokenWaitsForTheFollowerToCatchUpInsteadOfFailing() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = writer(fixture);
        GraphSession follower = follower(fixture);
        try {
            writer.applySchema(SchemaFixtures.DOCUMENTS);
            Token first = writer.writeTuples(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                    Durability.LAKE);
            await(() -> "CURRENT".equals(phase(follower)), "the follower to become current");

            Token second = writer.writeTuples(new TupleTransaction().add("document:b", "viewer", "user:alice"),
                    Durability.LAKE);

            assertTrue(follower.check("document:b", "view", "user:alice", second));
            assertTrue(follower.check("document:a", "view", "user:alice", first));
        } finally {
            follower.close();
            writer.close();
        }
    }

    @Test
    void aTokenTheFollowerNeverReachesFailsClosedAfterTheReadWait() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = writer(fixture);
        String json = fixture.followerJson().replace("}}", "},\"follow\":{\"poll_interval_ms\":10,"
                + "\"read_wait_ms\":100}}");
        long handle = sessions.openFollower(FollowerConfig.parse(json), GraphKernel.NO_MEMORY_LIMIT);
        GraphSession follower = sessions.get(handle);
        try {
            writer.applySchema(SchemaFixtures.DOCUMENTS);
            Token written = writer.writeTuples(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                    Durability.LAKE);
            await(() -> "CURRENT".equals(phase(follower)), "the follower to become current");
            Token unreachable = new Token(written.epoch(), written.lsn() + 1_000);

            assertThrows(StaleReadException.class,
                    () -> follower.check("document:a", "view", "user:alice", unreachable));
        } finally {
            follower.close();
            writer.close();
        }
    }

    @Test
    void everyWriteThroughAFollowerSessionIsRefused() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = writer(fixture);
        GraphSession follower = follower(fixture);
        try {
            writer.applySchema(SchemaFixtures.DOCUMENTS);
            Token token = writer.writeTuples(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                    Durability.LAKE);
            assertTrue(follower.check("document:a", "view", "user:alice", token));

            assertThrows(UnsupportedFeatureException.class, () -> follower.addEdge(1, 2));
            assertThrows(UnsupportedFeatureException.class,
                    () -> follower.writeTuples(new TupleTransaction().add("document:b", "viewer", "user:alice"),
                            Durability.LOCAL));
            assertThrows(UnsupportedFeatureException.class,
                    () -> follower.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2")));
            assertThrows(UnsupportedFeatureException.class, follower::checkpoint);
            assertThrows(UnsupportedFeatureException.class, () -> follower.intern(new byte[] {120}, 0, 1));
            assertEquals(KeyKind.STRING, follower.keyKind());
        } finally {
            follower.close();
            writer.close();
        }
    }

    @Test
    void theStatsDocumentCarriesFollowerStateAndNoShipping() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = writer(fixture);
        GraphSession follower = follower(fixture);
        try {
            writer.applySchema(SchemaFixtures.DOCUMENTS);
            Token token = writer.writeTuples(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                    Durability.LAKE);
            await(() -> "CURRENT".equals(phase(follower))
                    && appliedLsn(follower) >= token.lsn(), "the follower to apply the write and become current");

            JsonObject stats = JsonParser.parseObject(follower.statsJson());

            assertEquals(token.lsn(), stats.requireLong("applied_lsn"));
            assertFalse(stats.requireObject("shipping").boolOr("configured", true));
            JsonObject state = stats.requireObject("follower");
            assertTrue(state.requireLong("objects_applied") >= 1);
            assertTrue(state.requireLong("since_confirmed_ms") >= 0);
        } finally {
            follower.close();
            writer.close();
        }
    }

    @Test
    void closingAFollowerSessionStopsItsRuntimeAndRefusesFurtherCalls() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession follower = follower(fixture);

        follower.close();
        follower.close();

        assertThrows(IllegalStateException.class, () -> follower.hasEdge(1, 2));
        assertThrows(IllegalStateException.class, follower::statsJson);
    }
}
