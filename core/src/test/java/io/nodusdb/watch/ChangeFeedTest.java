package io.nodusdb.watch;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.HistoryExpiredException;
import io.nodusdb.error.TokenLostException;
import io.nodusdb.kernel.CapturingLog;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.changes.ChangeRing;
import io.nodusdb.log.RecordSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChangeFeedTest {

    private static final int CAPACITY = ChangeRing.MIN_CAPACITY;
    private static final Duration NO_WAIT = Duration.ZERO;
    private static final Duration LONG_WAIT = Duration.ofSeconds(60);

    private final CapturingLog log = new CapturingLog();
    private final GraphKernel kernel = new GraphKernel();
    private final TupleStore store;
    private final AtomicLong clock = new AtomicLong();

    ChangeFeedTest() {
        kernel.attachLog(log, CapturingLog.noStorage());
        store = TupleStore.open(kernel);
        store.applySchema(SchemaFixtures.DOCUMENTS);
    }

    @AfterEach
    void close() {
        kernel.close();
    }

    private ChangeFeed feed(RecordSource... sources) {
        return new ChangeFeed(kernel, List.of(sources), CAPACITY, clock::get, Duration.ofSeconds(60));
    }

    private static ChangeBatch read(ChangeFeed feed, Token after) {
        return feed.read(after, ChangeFeed.NO_BOUND, 1_000, NO_WAIT);
    }

    @Test
    void aTupleComesBackWithItsNamesAndTheTokenItsWriteReturned() {
        ChangeFeed feed = feed();
        Token start = read(feed, store.token()).next();

        Token written = store.add("document:readme", "viewer", "user:alice");
        Token userset = store.add("folder:eng", "viewer", "group:eng", "member");

        ChangeBatch batch = read(feed, start);
        assertEquals(userset, batch.next());
        assertEquals(2, batch.changes().size());
        Change first = batch.changes().get(0);
        assertEquals(ChangeKind.ADD, first.kind());
        assertEquals(written, first.token());
        assertEquals(written.lsn() - 1, first.lsn());
        assertEquals("document:readme", first.object());
        assertEquals("viewer", first.relation());
        assertEquals("user:alice", first.subject());
        assertEquals("", first.subjectRelation());
        assertEquals(log.lastCommitMicros() - 1_000, first.commitMicros());
        Change second = batch.changes().get(1);
        assertEquals("group:eng", second.subject());
        assertEquals("member", second.subjectRelation());
    }

    @Test
    void aTransactionArrivesWholeUnderOneToken() {
        ChangeFeed feed = feed();
        Token start = read(feed, store.token()).next();

        Token written = store.write(new TupleTransaction()
                .add("document:readme", "viewer", "user:alice")
                .add("document:readme", "editor", "user:bob")
                .remove("document:readme", "viewer", "user:alice"));

        ChangeBatch batch = read(feed, start);
        assertEquals(3, batch.changes().size());
        for (Change change : batch.changes()) {
            assertEquals(written, change.token());
        }
        assertEquals(ChangeKind.REMOVE, batch.changes().get(2).kind());
        assertTrue(batch.changes().get(0).lsn() < batch.changes().get(2).lsn());
    }

    @Test
    void aSchemaChangeReportsItsVersion() {
        ChangeFeed feed = feed();
        Token start = read(feed, store.token()).next();

        Token applied = store.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2")
                .replace("type user", "type user\ntype robot"));

        ChangeBatch batch = read(feed, start);
        assertEquals(applied, batch.next());
        assertEquals(1, batch.changes().size());
        assertEquals(ChangeKind.SCHEMA, batch.changes().get(0).kind());
        assertEquals(2, batch.changes().get(0).schemaVersion());
    }

    @Test
    void anErasedNameIsReadAsItsPseudonymEvenInChangesFromBeforeTheErase() {
        ChangeFeed feed = feed();
        Token start = read(feed, store.token()).next();
        store.add("document:readme", "viewer", "user:alice");

        Token erased = store.erase("user:alice");

        ChangeBatch batch = read(feed, start);
        Change grant = batch.changes().get(0);
        Change erase = batch.changes().get(batch.changes().size() - 1);
        assertEquals(ChangeKind.ERASE, erase.kind());
        assertEquals(erased, erase.token());
        assertTrue(erase.detail().startsWith("erased:"));
        assertEquals(erase.detail(), grant.subject());
        assertFalse(batch.changes().stream().anyMatch(change -> change.subject().contains("alice")
                || change.object().contains("alice") || change.detail().contains("alice")));
    }

    @Test
    void aCaughtUpCursorGetsAnEmptyBatchWithTheSameCursor() {
        ChangeFeed feed = feed();
        Token now = store.token();

        ChangeBatch batch = feed.read(now, ChangeFeed.NO_BOUND, 10, Duration.ofMillis(30));

        assertTrue(batch.changes().isEmpty());
        assertEquals(now, batch.next());
    }

    @Test
    void aWaitingReadReturnsAsSoonAsAWriteCommits() throws InterruptedException {
        ChangeFeed feed = feed();
        Token now = read(feed, store.token()).next();
        AtomicReference<ChangeBatch> result = new AtomicReference<>();
        Thread reader = new Thread(() -> result.set(feed.read(now, ChangeFeed.NO_BOUND, 10, LONG_WAIT)));
        long started = System.nanoTime();
        reader.start();
        awaitTimedWaiting(reader);

        Token written = store.add("document:readme", "viewer", "user:alice");
        reader.join(LONG_WAIT.toMillis());

        assertFalse(reader.isAlive());
        assertEquals(written, result.get().next());
        assertTrue(System.nanoTime() - started < LONG_WAIT.toNanos());
    }

    @Test
    void closingTheGraphEndsAWaitingRead() throws InterruptedException {
        ChangeFeed feed = feed();
        Token now = read(feed, store.token()).next();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                feed.read(now, ChangeFeed.NO_BOUND, 10, LONG_WAIT);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        reader.start();
        awaitTimedWaiting(reader);

        kernel.close();
        reader.join(LONG_WAIT.toMillis());

        assertFalse(reader.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
    }

    @Test
    void aCursorFromALaterEpochWaitsAndComesBackUnchanged() {
        ChangeFeed feed = feed();
        Token ahead = new Token(kernel.epoch() + 1, 2);

        ChangeBatch batch = feed.read(ahead, ChangeFeed.NO_BOUND, 10, Duration.ofMillis(20));

        assertTrue(batch.changes().isEmpty());
        assertEquals(ahead, batch.next());
    }

    @Test
    void aCursorNamingALostWriteIsRefused() {
        GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
        replica.recordEpoch(1, 1, 0);
        replica.recordEpoch(2, 6, 5);
        ChangeFeed feed = new ChangeFeed(replica, List.of(), CAPACITY);

        assertThrows(TokenLostException.class, () -> read(feed, new Token(1, 7)));
        replica.close();
    }

    @Test
    void theLimitIsSoftAndTheBoundIsHard() {
        ChangeFeed feed = feed();
        Token start = read(feed, store.token()).next();
        Token big = store.write(new TupleTransaction()
                .add("document:a", "viewer", "user:alice")
                .add("document:b", "viewer", "user:alice")
                .add("document:c", "viewer", "user:alice"));
        Token single = store.add("document:d", "viewer", "user:alice");

        ChangeBatch limited = feed.read(start, ChangeFeed.NO_BOUND, 1, NO_WAIT);
        assertEquals(3, limited.changes().size());
        assertEquals(big, limited.next());

        ChangeBatch bounded = feed.read(start, single.lsn() - 1, 1_000, NO_WAIT);
        assertEquals(big, bounded.next());

        assertEquals(single, read(feed, limited.next()).next());
    }

    @Test
    void aCursorOlderThanTheRingWithNoOtherHistoryHasExpired() {
        Token before = store.token();
        store.add("document:readme", "viewer", "user:alice");
        ChangeFeed feed = feed();

        HistoryExpiredException expired = assertThrows(HistoryExpiredException.class, () -> read(feed, before));

        assertTrue(expired.getMessage().contains("no other history"), expired.getMessage());
        assertEquals(1, feed.expiredReads());
    }

    @Test
    void aCursorOlderThanTheRingIsReadFromTheFirstSourceThatHoldsIt() {
        Token before = store.token();
        Token first = store.add("document:readme", "viewer", "user:alice");
        Token second = store.add("document:plan", "viewer", "user:bob");
        FakeRecordSource source = new FakeRecordSource("log").holding(log.drain().records());
        ChangeFeed feed = feed(new FakeRecordSource("empty").heldFrom(Long.MAX_VALUE), source);
        read(feed, store.token());
        Token third = store.add("document:spec", "viewer", "user:carol");

        ChangeBatch fromSource = read(feed, before);
        assertEquals(second, fromSource.next());
        assertEquals(first, fromSource.changes().get(0).token());
        assertEquals("user:bob", fromSource.changes().get(1).subject());

        ChangeBatch fromRing = read(feed, fromSource.next());
        assertEquals(third, fromRing.next());
        assertEquals("user:carol", fromRing.changes().get(0).subject());
    }

    @Test
    void aCatchingUpReaderContinuesItsCursorInsteadOfReopening() {
        Token before = store.token();
        for (int i = 0; i < 6; i++) {
            store.add("document:d" + i, "viewer", "user:alice");
        }
        FakeRecordSource source = new FakeRecordSource("log").holding(log.drain().records()).batchesOf(4);
        ChangeFeed feed = feed(source);
        read(feed, store.token());

        Token cursor = before;
        for (int i = 0; i < 6; i++) {
            ChangeBatch batch = feed.read(cursor, ChangeFeed.NO_BOUND, 1, NO_WAIT);
            assertEquals(1, batch.changes().size());
            assertEquals("document:d" + i, batch.changes().get(0).object());
            cursor = batch.next();
        }

        assertEquals(1, source.opens());
        assertEquals(1, feed.openReaders());
    }

    @Test
    void anIdleReaderIsClosedAndTheNextReadOpensAgain() {
        Token before = store.token();
        store.add("document:a", "viewer", "user:alice");
        store.add("document:b", "viewer", "user:alice");
        FakeRecordSource source = new FakeRecordSource("log").holding(log.drain().records());
        ChangeFeed feed = feed(source);
        read(feed, store.token());

        Token cursor = feed.read(before, ChangeFeed.NO_BOUND, 1, NO_WAIT).next();
        clock.addAndGet(TimeUnit.SECONDS.toNanos(61));
        feed.read(cursor, ChangeFeed.NO_BOUND, 1, NO_WAIT);

        assertEquals(2, source.opens());
        assertEquals(1, source.closes());
    }

    @Test
    void aSourceThatLosesItsRecordsHandsOverToTheNextOne() {
        Token before = store.token();
        store.add("document:a", "viewer", "user:alice");
        store.add("document:b", "viewer", "user:alice");
        byte[] records = log.drain().records();
        FakeRecordSource trimmed = new FakeRecordSource("log").holding(records).goneAfter(0);
        FakeRecordSource chain = new FakeRecordSource("chain").holding(records);
        ChangeFeed feed = feed(trimmed, chain);
        read(feed, store.token());

        ChangeBatch batch = read(feed, before);

        assertEquals(2, batch.changes().size());
        assertEquals(1, trimmed.closes());
        assertEquals(1, chain.opens());
    }

    @Test
    void aSourceWithNothingReadableYetGivesAnEmptyBatch() {
        Token before = store.token();
        store.add("document:a", "viewer", "user:alice");
        ChangeFeed feed = feed(new FakeRecordSource("log").holding(log.drain().records()).withholding());
        read(feed, store.token());

        ChangeBatch batch = feed.read(before, ChangeFeed.NO_BOUND, 10, Duration.ofMillis(30));

        assertTrue(batch.changes().isEmpty());
        assertEquals(before, batch.next());
    }

    @Test
    void anIntegerKeyedGraphNamesNodesByNumber() {
        GraphKernel edges = new GraphKernel();
        ChangeFeed feed = new ChangeFeed(edges, List.of(), CAPACITY);
        Token start = read(feed, edges.token()).next();

        edges.addEdge(3, 4);

        Change change = read(feed, start).changes().get(0);
        assertEquals("3", change.object());
        assertEquals("", change.relation());
        assertEquals("4", change.subject());
        assertEquals(edges.token(), change.token());
        edges.close();
    }

    @Test
    void aClosedFeedRefusesReads() {
        ChangeFeed feed = feed();
        feed.close();

        assertThrows(IllegalStateException.class, () -> read(feed, store.token()));
    }

    @Test
    void readArgumentsAreChecked() {
        ChangeFeed feed = feed();
        Token now = store.token();

        assertThrows(IllegalArgumentException.class, () -> feed.read(now, ChangeFeed.NO_BOUND, 0, NO_WAIT));
        assertThrows(IllegalArgumentException.class,
                () -> feed.read(now, ChangeFeed.NO_BOUND, ChangeFeed.MAX_LIMIT + 1, NO_WAIT));
        assertThrows(IllegalArgumentException.class, () -> feed.read(now, -1, 10, NO_WAIT));
        assertThrows(IllegalArgumentException.class,
                () -> feed.read(now, ChangeFeed.NO_BOUND, 10, Duration.ofMillis(-1)));
        assertThrows(IllegalArgumentException.class, () -> new ChangeFeed(kernel, List.of(), 3));
    }

    private static void awaitTimedWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (thread.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(Thread.State.TIMED_WAITING, thread.getState());
    }
}
