package io.nodusdb.authz;

import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.CapturingLog;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TupleStoreFollowerTest {

    private static final String SCHEMA_TWO = SchemaFixtures.DOCUMENTS
            .replace("schema 1", "schema 2")
            .replace("  permission view = viewer + editor + parent->view",
                    "  relation owner: user\n  permission view = viewer + editor + owner + parent->view");

    private final CapturingLog log = new CapturingLog();
    private final GraphKernel primaryKernel = new GraphKernel();
    private final GraphKernel followerKernel = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
    private TupleStore primary;
    private TupleStore follower;

    @BeforeEach
    void open() {
        primaryKernel.attachLog(log, CapturingLog.noStorage());
        primary = TupleStore.open(primaryKernel);
        followerKernel.recordEpoch(1, 1, 0);
        follower = TupleStore.open(followerKernel);
    }

    private void replicate() {
        CapturingLog.Drained drained = log.drain();
        if (!drained.isEmpty()) {
            followerKernel.applyReplicated(drained.records(), drained.first(), drained.last());
        }
    }

    @Test
    void aFollowerKeepsItsOwnReachabilityIndexCurrentAsNestingArrives() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        TupleTransaction chain = new TupleTransaction();
        for (int i = 0; i < 40; i++) {
            chain.add("group:g" + i, "member", "group:g" + (i + 1), "member");
        }
        chain.add("group:g40", "member", "user:alice");
        primary.write(chain);

        replicate();

        assertTrue(follower.check("group:g0", "member", "user:alice"));
        assertTrue(followerKernel.closureStats().incrementalUpdates() > 0);
        primary.remove("group:g20", "member", "group:g21", "member");
        replicate();
        assertFalse(follower.check("group:g0", "member", "user:alice"));
        assertTrue(follower.check("group:g21", "member", "user:alice"));
        assertEquals(primaryKernel.closureStats().entries(), followerKernel.closureStats().entries());
    }

    @Test
    void aFollowerAppliesAnErasureAndHoldsTheSamePseudonymAsThePrimary() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        primary.add("document:readme", "viewer", "user:alice");
        primary.add("group:eng", "member", "user:alice");
        replicate();
        byte[] name = "user:alice".getBytes(StandardCharsets.UTF_8);
        int alice = followerKernel.symbols().lookup(name, 0, name.length);

        primary.erase("user:alice");
        replicate();

        assertEquals(-1, followerKernel.symbols().lookup(name, 0, name.length));
        assertArrayEquals(primaryKernel.symbols().resolve(alice), followerKernel.symbols().resolve(alice));
        assertEquals(1, followerKernel.erasedSymbols());
        assertFalse(follower.check("document:readme", "view", "user:alice"));
        assertEquals(GraphDigest.of(primaryKernel), GraphDigest.of(followerKernel));
        assertThrows(UnsupportedFeatureException.class, () -> follower.erase("user:bob"));
    }

    @Test
    void aFollowerOpenedBeforeAnySchemaAnswersChecksOnceTheSchemaArrives() {
        assertEquals(0, follower.schemaVersion());
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        primary.add("document:readme", "viewer", "user:alice");

        replicate();

        assertEquals(1, follower.schemaVersion());
        assertTrue(follower.check("document:readme", "view", "user:alice"));
        assertFalse(follower.check("document:readme", "view", "user:bob"));
    }

    @Test
    void aSchemaMigrationIsVisibleToTheFollowersChecksWithoutReopeningTheStore() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        primary.add("document:plan", "viewer", "user:alice");
        replicate();
        assertTrue(follower.check("document:plan", "view", "user:alice"));
        assertEquals(1, follower.schemaVersion());

        primary.applySchema(SCHEMA_TWO);
        primary.add("document:plan", "owner", "user:bob");
        replicate();

        assertEquals(2, follower.schemaVersion());
        assertTrue(follower.check("document:plan", "view", "user:bob"));
        assertTrue(follower.check("document:plan", "view", "user:alice"));
        assertEquals(primary.check("document:plan", "view", "user:bob"),
                follower.check("document:plan", "view", "user:bob"));
    }

    @Test
    void theFollowerSchemaVersionTracksTheCatalogAfterEveryAppliedChunk() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        replicate();
        assertEquals(1, follower.schemaVersion());

        primary.applySchema(SCHEMA_TWO);
        replicate();

        assertEquals(2, follower.schemaVersion());
        assertEquals(primary.schemaVersion(), follower.schemaVersion());
    }

    @Test
    void aStoreOpenedOnAFollowerThatAlreadyHoldsASchemaCompilesItAtOpen() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        primary.add("document:readme", "viewer", "user:alice");
        replicate();

        TupleStore late = TupleStore.open(followerKernel);

        assertEquals(1, late.schemaVersion());
        assertTrue(late.check("document:readme", "view", "user:alice"));
    }

    @Test
    void aFollowerRefusesEveryTupleWriteAndChangesNothing() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        replicate();
        GraphDigest before = GraphDigest.of(followerKernel);
        long applied = followerKernel.appliedLsn();

        assertThrows(UnsupportedFeatureException.class, () -> follower.add("document:x", "viewer", "user:a"));
        assertThrows(UnsupportedFeatureException.class, () -> follower.remove("document:x", "viewer", "user:a"));
        assertThrows(UnsupportedFeatureException.class,
                () -> follower.write(new TupleTransaction().add("document:x", "viewer", "user:a")));
        assertThrows(UnsupportedFeatureException.class,
                () -> follower.write(new TupleTransaction().add("document:x", "viewer", "user:a"), Durability.LAKE));
        assertThrows(UnsupportedFeatureException.class, () -> follower.applySchema(SCHEMA_TWO));
        assertThrows(UnsupportedFeatureException.class,
                () -> follower.write(new TupleTransaction().add("nonsense", "viewer", "user:a")));

        assertEquals(before, GraphDigest.of(followerKernel));
        assertEquals(applied, followerKernel.appliedLsn());
        assertEquals(1, follower.schemaVersion());
    }

    @Test
    void aTokenFromThePrimaryIsStaleOnTheFollowerUntilTheWriteIsApplied() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        replicate();
        Token written = primary.add("document:readme", "viewer", "user:alice");

        assertThrows(StaleReadException.class,
                () -> follower.check("document:readme", "view", "user:alice", written));

        replicate();

        assertTrue(follower.check("document:readme", "view", "user:alice", written));
    }

    @Test
    void theFollowerTokenNamesTheEpochAndTheAppliedLsn() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        Token written = primary.add("document:readme", "viewer", "user:alice");

        replicate();

        assertEquals(written, follower.token());
    }

    @Test
    void aFollowerAndItsPrimaryAgreeOnEveryCheckOverAMixedHistory() {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        primary.add("group:eng", "member", "user:alice");
        primary.add("folder:root", "viewer", "group:eng", "member");
        primary.add("document:spec", "parent", "folder:root");
        primary.add("document:spec", "editor", "user:carol");
        primary.remove("document:spec", "editor", "user:carol");
        primary.applySchema(SCHEMA_TWO);
        primary.add("document:spec", "owner", "user:dave");
        replicate();

        String[] subjects = {"user:alice", "user:bob", "user:carol", "user:dave"};
        for (String subject : subjects) {
            assertEquals(primary.check("document:spec", "view", subject),
                    follower.check("document:spec", "view", subject), subject);
        }
        assertEquals(GraphDigest.of(primaryKernel), GraphDigest.of(followerKernel));
    }

    @Test
    void manyThreadsCheckingRightAfterAMigrationAllSeeTheNewSchema() throws Exception {
        primary.applySchema(SchemaFixtures.DOCUMENTS);
        replicate();
        primary.applySchema(SCHEMA_TWO);
        primary.add("document:plan", "owner", "user:bob");
        replicate();
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Boolean>> answers = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                answers.add(pool.submit(() -> {
                    start.await();
                    return follower.check("document:plan", "view", "user:bob");
                }));
            }
            start.countDown();
            for (Future<Boolean> answer : answers) {
                assertTrue(answer.get(30, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(2, follower.schemaVersion());
    }
}
