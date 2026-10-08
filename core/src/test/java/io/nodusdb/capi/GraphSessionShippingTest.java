package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.ErrorCode;
import io.nodusdb.error.ShipTimeoutException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.ship.ShippingFixture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionShippingTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);

    @TempDir
    Path root;

    private final GraphSessions sessions = new GraphSessions();

    private GraphSession shipped(ShippingFixture fixture) throws IOException {
        long handle = sessions.openDurable(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT, fixture.config());
        return sessions.get(handle);
    }

    private static TupleTransaction grant(String object) {
        return new TupleTransaction().add(object, "viewer", "user:alice");
    }

    @Test
    void aLakeWriteThroughASessionIsShippedWhenItReturns() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession session = shipped(fixture);
        try {
            session.applySchema(SchemaFixtures.DOCUMENTS);

            Token token = session.writeTuples(grant("document:a"), Durability.LAKE);

            JsonObject shipping = JsonParser.parseObject(session.statsJson()).requireObject("shipping");
            assertTrue(shipping.requireLong("shipped_lsn") >= token.lsn());
            assertEquals(0, shipping.requireLong("lag_lsn"));
            assertEquals("ACTIVE", shipping.requireString("phase"));
        } finally {
            session.close();
        }
    }

    @Test
    void aLocalWriteDoesNotWaitAndTheTokenCanBeAwaitedLater() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession session = shipped(fixture);
        try {
            session.applySchema(SchemaFixtures.DOCUMENTS);
            Token token = session.writeTuples(grant("document:a"), Durability.LOCAL);

            session.awaitShipped(token, Duration.ofSeconds(20));

            assertTrue(JsonParser.parseObject(session.statsJson()).requireObject("shipping")
                    .requireLong("shipped_lsn") >= token.lsn());
        } finally {
            session.close();
        }
    }

    @Test
    void anUnconfiguredSessionRefusesLakeWritesBeforeWritingAndReportsNoShipping() {
        GraphSession session = new GraphSession(new GraphKernel());
        session.applySchema(SchemaFixtures.DOCUMENTS);
        Token before = session.token();

        UnsupportedFeatureException refused = assertThrows(UnsupportedFeatureException.class,
                () -> session.writeTuples(grant("document:a"), Durability.LAKE));

        assertEquals(before, session.token());
        assertEquals(ErrorCode.UNSUPPORTED.value(), Failures.codeOf(refused));
        assertFalse(JsonParser.parseObject(session.statsJson()).requireObject("shipping").boolOr("configured", true));
        assertThrows(UnsupportedFeatureException.class,
                () -> session.awaitShipped(before, Duration.ofSeconds(1)));
    }

    @Test
    void aTimedOutWaitCarriesTheTokenOfTheWriteThatWasApplied() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession session = shipped(fixture);
        try {
            session.applySchema(SchemaFixtures.DOCUMENTS);
            session.writeTuples(grant("document:a"), Durability.LAKE);
            deleteTree(fixture.bucket());
            Files.writeString(fixture.bucket(), "not a directory");
            Token token = session.writeTuples(grant("document:b"), Durability.LOCAL);

            ShipTimeoutException timeout = assertThrows(ShipTimeoutException.class,
                    () -> session.awaitShipped(token, Duration.ofMillis(300)));

            assertEquals(token.epoch(), timeout.epoch());
            assertEquals(token.lsn(), timeout.lsn());
            assertEquals(ErrorCode.SHIP_TIMEOUT.value(), Failures.codeOf(timeout));
            JsonObject shipping = JsonParser.parseObject(session.statsJson()).requireObject("shipping");
            assertTrue(shipping.requireLong("lag_lsn") > 0);
            assertFalse(shipping.requireString("last_error").isEmpty());
        } finally {
            session.close();
        }
    }

    @Test
    void aWaitingLakeWriteDoesNotBlockReadsOrCloseOnTheSameSession() throws Exception {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession session = shipped(fixture);
        session.applySchema(SchemaFixtures.DOCUMENTS);
        session.writeTuples(grant("document:a"), Durability.LAKE);
        deleteTree(fixture.bucket());
        Files.writeString(fixture.bucket(), "not a directory");
        Token before = session.token();

        CompletableFuture<Token> waiting = CompletableFuture.supplyAsync(
                () -> session.writeTuples(grant("document:b"), Durability.LAKE));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (session.token().lsn() == before.lsn()) {
            assertTrue(System.nanoTime() < deadline, "the lake write was never applied locally");
            Thread.sleep(5);
        }

        assertTrue(session.check("document:b", "view", "user:alice", null));
        assertFalse(waiting.isDone());
        long started = System.nanoTime();
        session.close();

        ExecutionException failed = assertThrows(ExecutionException.class, () -> waiting.get(30, TimeUnit.SECONDS));
        assertTrue(failed.getCause() instanceof ShipTimeoutException, String.valueOf(failed.getCause()));
        assertTrue(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started) < 28);
    }

    @Test
    void sessionsOnADurableGraphWithoutShippingStayUnconfigured() throws IOException {
        long handle = sessions.openDurable(root.resolve("plain"), SYNC);
        GraphSession session = sessions.get(handle);
        try {
            assertFalse(JsonParser.parseObject(session.statsJson()).requireObject("shipping")
                    .boolOr("configured", true));
        } finally {
            sessions.close(handle);
        }
    }

    private static void deleteTree(Path path) throws IOException {
        try (Stream<Path> walk = Files.walk(path)) {
            for (Path entry : (Iterable<Path>) walk.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(entry);
            }
        }
    }
}
