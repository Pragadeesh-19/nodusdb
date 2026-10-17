package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.HistoryExpiredException;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;
import io.nodusdb.kernel.past.PastReport;
import io.nodusdb.kernel.past.PastTarget;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.ship.ShippingFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionPastTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final Duration WAIT = Duration.ofSeconds(30);

    @TempDir
    Path root;

    private final GraphSessions sessions = new GraphSessions();
    private final List<GraphSession> opened = new ArrayList<>();

    @AfterEach
    void closeAll() {
        for (int i = opened.size() - 1; i >= 0; i--) {
            opened.get(i).close();
        }
    }

    private GraphSession writer(ShippingFixture fixture) throws IOException {
        long handle = fixture == null
                ? sessions.openDurable(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT)
                : sessions.openDurable(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT, fixture.config());
        GraphSession session = sessions.get(handle);
        opened.add(session);
        return session;
    }

    private GraphSession follower(ShippingFixture fixture) throws IOException {
        String json = fixture.followerJson().replace("}}", "},\"follow\":{\"poll_interval_ms\":10,"
                + "\"read_wait_ms\":5000}}");
        GraphSession session = sessions.get(sessions.openFollower(FollowerConfig.parse(json),
                GraphKernel.NO_MEMORY_LIMIT));
        opened.add(session);
        return session;
    }

    private GraphSession at(GraphSession live, Token token) {
        return live.openPast(new PastTarget.AtToken(token), GraphKernel.NO_MEMORY_LIMIT, WAIT);
    }

    private static Token grant(GraphSession session, String object, String subject) {
        return session.writeTuples(new TupleTransaction().add(object, "viewer", subject), Durability.LAKE);
    }

    private static Token revoke(GraphSession session, String object, String subject) {
        return session.writeTuples(new TupleTransaction().remove(object, "viewer", subject), Durability.LAKE);
    }

    private static boolean views(GraphSession session, String object, String subject) {
        return session.check(object, "view", subject, null);
    }

    private static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }

    @Test
    void aViewAtATokenAnswersAsTheGraphDidThen() throws IOException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token first = grant(live, "document:a", "user:alice");
        Token second = grant(live, "document:b", "user:bob");
        live.checkpoint();
        Token third = revoke(live, "document:a", "user:alice");

        GraphSession atFirst = at(live, first);
        GraphSession atSecond = at(live, second);
        GraphSession atThird = at(live, third);

        assertTrue(views(atFirst, "document:a", "user:alice"));
        assertFalse(views(atFirst, "document:b", "user:bob"));
        assertTrue(views(atSecond, "document:a", "user:alice"));
        assertTrue(views(atSecond, "document:b", "user:bob"));
        assertFalse(views(atThird, "document:a", "user:alice"));
        assertTrue(views(atThird, "document:b", "user:bob"));
        assertEquals(first, atFirst.token());
        assertEquals(third, atThird.pastReport().position());
        assertFalse(views(live, "document:a", "user:alice"));
        assertEquals(3, live.openViews());
    }

    @Test
    void aViewIsReadOnly() throws IOException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token token = grant(live, "document:a", "user:alice");
        GraphSession view = at(live, token);

        assertThrows(UnsupportedFeatureException.class,
                () -> view.writeTuples(new TupleTransaction().add("document:b", "viewer", "user:bob"),
                        Durability.LOCAL));
        assertThrows(UnsupportedFeatureException.class, () -> view.erase("user:alice"));
        assertThrows(UnsupportedFeatureException.class, view::checkpoint);
        assertThrows(UnsupportedFeatureException.class, view::sync);
        assertThrows(UnsupportedFeatureException.class, () -> view.applySchema(SchemaFixtures.DOCUMENTS));
        assertThrows(UnsupportedFeatureException.class, () -> view.awaitShipped(token, WAIT));
        assertThrows(UnsupportedFeatureException.class, () -> view.awaitErased(token, WAIT));
        assertThrows(UnsupportedFeatureException.class, () -> view.changes(token, Long.MAX_VALUE, 10, WAIT));
        assertThrows(UnsupportedFeatureException.class,
                () -> view.openPast(new PastTarget.AtToken(token), GraphKernel.NO_MEMORY_LIMIT, WAIT));
        assertThrows(UnsupportedFeatureException.class, () -> live.advancePast(new PastTarget.AtToken(token), WAIT));
    }

    @Test
    void aViewAtATimeStandsOnTheLastCommitAtOrBeforeIt() throws IOException, InterruptedException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token before = grant(live, "document:a", "user:alice");
        Thread.sleep(5);
        long moment = nowMicros();
        Thread.sleep(5);
        grant(live, "document:b", "user:bob");

        GraphSession view = live.openPast(new PastTarget.AtTime(moment), GraphKernel.NO_MEMORY_LIMIT, WAIT);

        assertEquals(before, view.token());
        assertTrue(view.pastReport().commitMicros() <= moment);
        assertTrue(views(view, "document:a", "user:alice"));
        assertFalse(views(view, "document:b", "user:bob"));
        assertThrows(IllegalArgumentException.class, () -> live.openPast(
                new PastTarget.AtTime(nowMicros() + 60_000_000L), GraphKernel.NO_MEMORY_LIMIT, WAIT));
    }

    @Test
    void aViewMovesForwardAndNeverBack() throws IOException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token first = grant(live, "document:a", "user:alice");
        Token second = grant(live, "document:b", "user:bob");
        GraphSession view = at(live, first);

        PastReport moved = view.advancePast(new PastTarget.AtToken(second), WAIT);

        assertEquals(second, moved.position());
        assertTrue(views(view, "document:b", "user:bob"));
        assertThrows(IllegalArgumentException.class, () -> view.advancePast(new PastTarget.AtToken(first), WAIT));
    }

    @Test
    void theHistoryBeforeTheNewestErasureIsGoneAndAnOpenViewForgetsANameErasedLater() throws IOException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token early = grant(live, "document:a", "user:carol");
        live.erase("user:carol");
        Token afterErase = grant(live, "document:b", "user:alice");
        live.checkpoint();
        Token later = grant(live, "document:c", "user:alice");

        assertThrows(HistoryExpiredException.class, () -> at(live, early));
        GraphSession view = at(live, later);
        assertTrue(views(view, "document:b", "user:alice"));

        live.erase("user:alice");

        assertFalse(views(view, "document:b", "user:alice"));
        byte[] name = "user:alice".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, view.lookup(name, 0, name.length));
        assertThrows(HistoryExpiredException.class, () -> view.advancePast(new PastTarget.AtToken(live.token()), WAIT));
        assertTrue(afterErase.lsn() < later.lsn());
    }

    @Test
    void aGraphWithoutShippingCannotTravelInTime() throws IOException {
        GraphSession live = writer(null);
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token token = live.writeTuples(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                Durability.LOCAL);

        assertThrows(UnsupportedFeatureException.class, () -> at(live, token));
    }

    @Test
    void aFollowerOpensAViewFromTheBucketItFollows() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession live = writer(fixture);
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token first = grant(live, "document:a", "user:alice");
        Token second = revoke(live, "document:a", "user:alice");
        GraphSession follower = follower(fixture);
        assertFalse(follower.check("document:a", "view", "user:alice", second));

        GraphSession view = at(follower, first);

        assertTrue(views(view, "document:a", "user:alice"));
        assertThrows(StaleReadException.class, () -> follower.openPast(
                new PastTarget.AtToken(new Token(second.epoch(), second.lsn() + 50)), GraphKernel.NO_MEMORY_LIMIT,
                Duration.ofMillis(100)));
    }

    @Test
    void aViewThatDoesNotFitItsLimitIsRefusedAndReturnsWhatItTook() throws IOException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token token = grant(live, "document:a", "user:alice");

        assertThrows(MemoryLimitExceededException.class,
                () -> live.openPast(new PastTarget.AtToken(token), 1, WAIT));

        JsonObject history = JsonParser.parseObject(live.statsJson()).requireObject("history");
        assertTrue(history.boolOr("configured", false));
        assertEquals(0, history.requireLong("views"));
        assertEquals(0, history.requireLong("used_bytes"));
        GraphSession view = at(live, token);
        assertTrue(JsonParser.parseObject(live.statsJson()).requireObject("history").requireLong("used_bytes") > 0);
        JsonObject asOf = JsonParser.parseObject(view.statsJson()).requireObject("as_of");
        assertEquals(token.lsn(), asOf.requireLong("lsn"));
        assertTrue(asOf.requireLong("memory_bytes") > 0);
    }

    @Test
    void closingTheGraphClosesItsViews() throws IOException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        Token token = grant(live, "document:a", "user:alice");
        GraphSession view = at(live, token);

        live.close();

        assertThrows(IllegalStateException.class, () -> views(view, "document:a", "user:alice"));
        assertEquals(0, live.openViews());
    }

    @Test
    void nestedGroupsAnswerAtOnceInAFreshView() throws IOException {
        GraphSession live = writer(ShippingFixture.in(root));
        live.applySchema(SchemaFixtures.DOCUMENTS);
        TupleTransaction nesting = new TupleTransaction();
        for (int depth = 0; depth < 40; depth++) {
            nesting.add("group:g" + depth, "member", "group:g" + (depth + 1), "member");
        }
        nesting.add("group:g40", "member", "user:alice");
        nesting.add("document:deep", "viewer", "group:g0", "member");
        Token token = live.writeTuples(nesting, Durability.LAKE);

        GraphSession view = at(live, token);

        JsonObject reachability = JsonParser.parseObject(view.statsJson()).requireObject("reachability");
        assertEquals("current", reachability.requireString("state"));
        assertEquals(0, reachability.requireLong("dirty_nodes"));
        assertTrue(views(view, "document:deep", "user:alice"));
    }
}
