package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.HistoryExpiredException;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.changes.ChangeRing;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.watch.Change;
import io.nodusdb.watch.ChangeBatch;
import io.nodusdb.watch.ChangeFeed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionWatchTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final Duration NO_WAIT = Duration.ZERO;
    private static final Duration FOLLOW_WAIT = Duration.ofSeconds(30);

    @TempDir
    Path root;

    private final GraphSessions sessions = new GraphSessions();

    private GraphSession durable(ShippingFixture fixture) throws IOException {
        long handle = fixture == null
                ? sessions.openDurable(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT)
                : sessions.openDurable(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT, fixture.config());
        return sessions.get(handle);
    }

    private GraphSession follower(ShippingFixture fixture) throws IOException {
        String json = fixture.followerJson().replace("}}", "},\"follow\":{\"poll_interval_ms\":10,"
                + "\"read_wait_ms\":5000}}");
        return sessions.get(sessions.openFollower(FollowerConfig.parse(json), GraphKernel.NO_MEMORY_LIMIT));
    }

    private static Token grant(GraphSession session, String object, String subject, Durability durability) {
        return session.writeTuples(new TupleTransaction().add(object, "viewer", subject), durability);
    }

    private static List<Change> readAll(GraphSession session, Token after) {
        List<Change> changes = new ArrayList<>();
        Token cursor = after;
        while (true) {
            ChangeBatch batch = session.changes(cursor, ChangeFeed.NO_BOUND, 2, NO_WAIT);
            if (batch.changes().isEmpty() && batch.next().equals(cursor)) {
                return changes;
            }
            changes.addAll(batch.changes());
            cursor = batch.next();
        }
    }

    private static List<String> objects(List<Change> changes) {
        return changes.stream().map(Change::object).toList();
    }

    @Test
    void anInMemorySessionWatchesFromTheMomentItsBufferStarts() {
        GraphSession session = sessions.get(sessions.open(GraphKernel.NO_MEMORY_LIMIT));
        try {
            assertEquals(ChangeRing.MIN_CAPACITY, session.watchBuffer(ChangeRing.MIN_CAPACITY));
            assertThrows(IllegalArgumentException.class, () -> session.watchBuffer(1_000));
            session.applySchema(SchemaFixtures.DOCUMENTS);
            Token start = session.changes(session.token(), ChangeFeed.NO_BOUND, 10, NO_WAIT).next();

            Token written = grant(session, "document:a", "user:alice", Durability.LOCAL);

            List<Change> changes = readAll(session, start);
            assertEquals(List.of("document:a"), objects(changes));
            assertEquals(written, changes.get(0).token());
            assertEquals(ChangeRing.MIN_CAPACITY, session.watchBuffer(ChangeRing.DEFAULT_CAPACITY));
            JsonObject watch = JsonParser.parseObject(session.statsJson()).requireObject("watch");
            assertTrue(watch.boolOr("enabled", false));
            assertEquals(ChangeRing.MIN_CAPACITY, watch.requireLong("buffer_changes"));
            assertEquals(written.lsn(), watch.requireLong("published_lsn"));
        } finally {
            session.close();
        }
    }

    @Test
    void aCursorOlderThanTheBufferIsReadFromTheLogUntilACheckpointTrimsIt() throws IOException {
        GraphSession session = durable(null);
        try {
            session.applySchema(SchemaFixtures.DOCUMENTS);
            Token start = session.token();
            grant(session, "document:a", "user:alice", Durability.LOCAL);
            grant(session, "document:b", "user:bob", Durability.LOCAL);

            ChangeBatch fromLog = session.changes(start, ChangeFeed.NO_BOUND, 1, NO_WAIT);
            assertEquals(List.of("document:a"), objects(fromLog.changes()));
            assertEquals(1, JsonParser.parseObject(session.statsJson()).requireObject("watch")
                    .requireLong("fallback_readers"));

            session.checkpoint();
            Token third = grant(session, "document:c", "user:carol", Durability.LOCAL);

            ChangeBatch continued = session.changes(fromLog.next(), ChangeFeed.NO_BOUND, 10, NO_WAIT);
            assertEquals(List.of("document:b", "document:c"), objects(continued.changes()));
            assertEquals(third, continued.next());
            HistoryExpiredException expired = assertThrows(HistoryExpiredException.class,
                    () -> session.changes(start, ChangeFeed.NO_BOUND, 10, NO_WAIT));
            assertTrue(expired.getMessage().contains("the log does not hold them"), expired.getMessage());
            assertEquals(1, JsonParser.parseObject(session.statsJson()).requireObject("watch")
                    .requireLong("expired"));
        } finally {
            session.close();
        }
    }

    @Test
    void aGraphWithoutShippingKeepsNoHistoryAcrossACleanClose() throws IOException {
        GraphSession first = durable(null);
        Token before;
        try {
            first.applySchema(SchemaFixtures.DOCUMENTS);
            before = first.token();
            grant(first, "document:a", "user:alice", Durability.LOCAL);
        } finally {
            first.close();
        }
        GraphSession reopened = durable(null);
        try {
            assertThrows(HistoryExpiredException.class,
                    () -> reopened.changes(before, ChangeFeed.NO_BOUND, 10, NO_WAIT));
            Token now = reopened.token();
            Token written = grant(reopened, "document:b", "user:bob", Durability.LOCAL);
            assertEquals(written, reopened.changes(now, ChangeFeed.NO_BOUND, 10, NO_WAIT).next());
        } finally {
            reopened.close();
        }
    }

    @Test
    void aShippedGraphServesCursorsTheLogNoLongerHoldsFromTheChain() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession first = durable(fixture);
        Token start;
        try {
            first.applySchema(SchemaFixtures.DOCUMENTS);
            start = first.token();
            grant(first, "document:a", "user:alice", Durability.LAKE);
            grant(first, "document:b", "user:bob", Durability.LAKE);
            first.checkpoint();
        } finally {
            first.close();
        }
        GraphSession reopened = durable(fixture);
        try {
            Token third = grant(reopened, "document:c", "user:carol", Durability.LAKE);
            reopened.checkpoint();

            List<Change> changes = readAll(reopened, start);

            assertEquals(List.of("add", "add", "epoch", "add"),
                    changes.stream().map(change -> change.kind().event()).toList());
            assertEquals(List.of("document:a", "document:b", "", "document:c"), objects(changes));
            assertEquals(third, changes.get(3).token());
            assertEquals(reopened.token().epoch(), changes.get(2).token().epoch());
            assertEquals("user:bob", changes.get(1).subject());
        } finally {
            reopened.close();
        }
    }

    @Test
    void aFollowerWaitingOnItsFeedSeesTheWritersRevocation() throws Exception {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = durable(fixture);
        GraphSession follower = follower(fixture);
        try {
            writer.applySchema(SchemaFixtures.DOCUMENTS);
            Token granted = grant(writer, "document:a", "user:alice", Durability.LAKE);
            follower.check("document:a", "view", "user:alice", granted);
            Token cursor = follower.token();

            Token revoked = writer.writeTuples(new TupleTransaction().remove("document:a", "viewer", "user:alice"),
                    Durability.LAKE);
            ChangeBatch batch = awaitChanges(follower, cursor);

            assertEquals(revoked, batch.next());
            Change change = batch.changes().get(0);
            assertEquals("remove", change.kind().event());
            assertEquals("user:alice", change.subject());
            assertFalse(follower.check("document:a", "view", "user:alice", batch.next()));
        } finally {
            follower.close();
            writer.close();
        }
    }

    @Test
    void aFollowerServesACursorOlderThanItsBufferFromTheChain() throws Exception {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = durable(fixture);
        GraphSession follower = follower(fixture);
        try {
            writer.applySchema(SchemaFixtures.DOCUMENTS);
            Token start = writer.token();
            grant(writer, "document:a", "user:alice", Durability.LAKE);
            Token second = grant(writer, "document:b", "user:bob", Durability.LAKE);
            follower.check("document:b", "view", "user:bob", second);

            List<Change> changes = readAll(follower, start);

            assertEquals(List.of("document:a", "document:b"), objects(changes));
        } finally {
            follower.close();
            writer.close();
        }
    }

    @Test
    void aBootstrappingFollowerRefusesToWatch() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession follower = follower(fixture);
        try {
            assertThrows(StaleReadException.class,
                    () -> follower.changes(new Token(0, 0), ChangeFeed.NO_BOUND, 10, NO_WAIT));
        } finally {
            follower.close();
        }
    }

    private static ChangeBatch awaitChanges(GraphSession session, Token cursor) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            ChangeBatch batch = session.changes(cursor, ChangeFeed.NO_BOUND, 10, FOLLOW_WAIT);
            if (!batch.changes().isEmpty()) {
                return batch;
            }
        }
        throw new AssertionError("no change arrived after " + cursor);
    }
}
