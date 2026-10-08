package io.nodusdb.ship;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.error.ShipTimeoutException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.storage.DurableGraph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableGraphShippingEdgeTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final long WAIT_MILLIS = 20_000;

    @TempDir
    Path root;

    private GraphKernel open(Path graph, ShippingFixture fixture) throws IOException {
        return DurableGraph.open(graph, SYNC, GraphKernel.NO_MEMORY_LIMIT, fixture.config()).kernel();
    }

    private static TupleTransaction grant(String object) {
        return new TupleTransaction().add(object, "viewer", "user:alice");
    }

    private static TupleStore schemaStore(GraphKernel kernel) {
        TupleStore store = TupleStore.open(kernel);
        if (store.schemaVersion() == 0) {
            store.applySchema(SchemaFixtures.DOCUMENTS);
        }
        return store;
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for " + what);
            }
        }
    }

    private static List<ChainObject> chain(ObjectStore store) {
        List<ChainObject> objects = new ArrayList<>();
        for (String key : ChainAudit.chainKeys(store)) {
            objects.add(ChainCodec.decode(store.get(key).orElseThrow()));
        }
        return objects;
    }

    private static List<Long> referencedLsns(ObjectStore store) {
        List<Long> lsns = new ArrayList<>();
        for (ChainObject object : chain(store)) {
            if (object.body() instanceof ChainBody.SnapshotRef reference) {
                lsns.add(reference.lsn());
            }
        }
        return lsns;
    }

    private static Set<Thread> shippingThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("nodus-snapshot-shipper")
                        || thread.getName().equals("nodus-shipper")
                        || thread.getName().equals("nodus-retention")
                        || thread.getName().equals("nodus-projector"))
                .filter(Thread::isAlive)
                .collect(Collectors.toSet());
    }

    private static boolean noNewThreads(Set<Thread> before) {
        Set<Thread> now = shippingThreads();
        now.removeAll(before);
        return now.isEmpty();
    }

    @Test
    void aCheckpointBecomesASnapshotObjectAndAReferenceOnTheChain() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel kernel = open(graph, fixture);
        try {
            TupleStore store = schemaStore(kernel);
            Token token = store.write(grant("document:a"), Durability.LAKE);

            kernel.checkpoint();

            ObjectStore bucket = fixture.store();
            await(() -> referencedLsns(bucket).contains(token.lsn()), "the checkpoint reference");
            assertArrayEquals(Files.readAllBytes(graph.resolve("snapshot.bin")),
                    bucket.get(ChainLayout.snapshotKey(token.lsn())).orElseThrow());
        } finally {
            kernel.close();
        }
    }

    @Test
    void checkpointingAgainAtTheSameLsnShipsNothingAndFencesNobody() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphKernel kernel = open(root.resolve("graph"), fixture);
        try {
            TupleStore store = schemaStore(kernel);
            Token token = store.write(grant("document:a"), Durability.LAKE);
            kernel.checkpoint();
            ObjectStore bucket = fixture.store();
            await(() -> referencedLsns(bucket).contains(token.lsn()), "the first reference");
            int objects = chain(bucket).size();

            kernel.checkpoint();
            kernel.checkpoint();
            Token next = store.write(grant("document:b"), Durability.LAKE);

            assertEquals(1, referencedLsns(bucket).stream().filter(lsn -> lsn == token.lsn()).count());
            assertTrue(chain(bucket).size() > objects);
            assertTrue(kernel.shipWatermark().shippedLsn() >= next.lsn());
        } finally {
            kernel.close();
        }
    }

    @Test
    void aCheckpointAfterARestartAtTheSameLsnIsNotAFalseFence() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel first = open(graph, fixture);
        Token token = schemaStore(first).write(grant("document:a"), Durability.LAKE);
        first.checkpoint();
        ObjectStore bucket = fixture.store();
        await(() -> referencedLsns(bucket).contains(token.lsn()), "the reference");
        first.close();

        GraphKernel second = open(graph, fixture);
        try {
            second.checkpoint();
            Token after = TupleStore.open(second).write(grant("document:b"), Durability.LAKE);

            assertTrue(after.lsn() > token.lsn());
            assertEquals(1, referencedLsns(bucket).stream().filter(lsn -> lsn == token.lsn()).count());
        } finally {
            second.close();
        }
    }

    @Test
    void aStoreThatCannotBeWrittenEndsALakeWaitWithTheShippingStateAndKeepsTheWrite() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphKernel kernel = open(root.resolve("graph"), fixture);
        try {
            TupleStore store = schemaStore(kernel);
            store.write(grant("document:a"), Durability.LAKE);
            deleteTree(fixture.bucket());
            Files.writeString(fixture.bucket(), "not a directory");

            ShipTimeoutException failed = assertThrows(ShipTimeoutException.class,
                    () -> store.write(grant("document:b"), Durability.LAKE, Duration.ofMillis(500)));

            assertTrue(failed.getMessage().contains("had not reached the object store"), failed.getMessage());
            assertEquals(kernel.appliedLsn(), failed.lsn());
            assertTrue(store.check("document:b", "view", "user:alice"));
            assertTrue(kernel.shipWatermark().shippedLsn() < failed.lsn());
        } finally {
            kernel.close();
        }
    }

    @Test
    void closingStopsEveryShippingThreadItStarted() throws IOException {
        Set<Thread> before = shippingThreads();
        ShippingFixture fixture = ShippingFixture.in(root).withIceberg("\"enabled\":true,\"commit_interval_s\":1");
        GraphKernel kernel = open(root.resolve("graph"), fixture);
        schemaStore(kernel).write(grant("document:a"), Durability.LAKE);
        assertFalse(noNewThreads(before));

        kernel.close();

        await(() -> noNewThreads(before), "the shipping threads to stop");
    }

    @Test
    void aFailedOpenLeavesNoThreadsAndNoLock() throws IOException {
        Set<Thread> before = shippingThreads();
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphKernel shipped = open(root.resolve("graph"), fixture);
        schemaStore(shipped).write(grant("document:a"), Durability.LAKE);
        shipped.close();
        await(() -> noNewThreads(before), "the first graph's threads to stop");

        assertThrows(WriterFencedException.class, () -> open(root.resolve("fresh"), fixture));

        assertTrue(noNewThreads(before));
        DurableGraph.open(root.resolve("fresh"), SYNC).kernel().close();
    }

    @Test
    void aWrongSigningKeyForAnExistingChainRefusesTheOpen() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel first = open(graph, fixture);
        schemaStore(first).write(grant("document:a"), Durability.LAKE);
        first.close();
        Path strangerPrivate = root.resolve("stranger.pem");
        Path strangerPublic = root.resolve("stranger.pub");
        KeyPair stranger = KeyFiles.generate();
        KeyFiles.writePrivate(strangerPrivate, stranger.getPrivate());
        KeyFiles.writePublic(strangerPublic, stranger.getPublic());
        ShippingFixture wrongKey = fixture.withSigning(strangerPrivate, strangerPublic);

        assertThrows(ChainTrustException.class, () -> open(graph, wrongKey));

        DurableGraph.open(graph, SYNC).kernel().close();
    }

    @Test
    void aWriterWhoseEpochWasSupersededIsFencedAndRefusesEveryFurtherWrite() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphKernel kernel = open(root.resolve("graph"), fixture);
        try {
            TupleStore store = schemaStore(kernel);
            Token shipped = store.write(grant("document:a"), Durability.LAKE);
            new EpochClaims(fixture.store(), 0x5EC0DL, ChainBuilder.KEY_ID, () -> 1_700_000_000_000_000L)
                    .claim(shipped.epoch() + 1, 4);

            WriterFencedException fenced = assertThrows(WriterFencedException.class,
                    () -> store.write(grant("document:b"), Durability.LAKE));

            assertTrue(fenced.getMessage().contains("superseded"), fenced.getMessage());
            assertThrows(WriterFencedException.class, () -> store.add("document:c", "viewer", "user:alice"));
            assertThrows(WriterFencedException.class, () -> store.awaitShipped(kernel.token(), Duration.ofSeconds(1)));
        } finally {
            kernel.close();
        }
    }

    @Test
    void reopeningWithShippingAfterAnUnshippedStretchIsRefusedBecauseTheGapCannotBeFilled() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel shipped = open(graph, fixture);
        schemaStore(shipped).write(grant("document:a"), Durability.LAKE);
        shipped.close();
        GraphKernel unshipped = DurableGraph.open(graph, SYNC).kernel();
        TupleStore plain = TupleStore.open(unshipped);
        for (int i = 0; i < 20; i++) {
            plain.add("document:gap" + i, "viewer", "user:alice");
        }
        unshipped.close();

        Set<Thread> before = shippingThreads();
        long claims = count(fixture.store(), ChainLayout.EPOCH_PREFIX);

        WriterFencedException refused = assertThrows(WriterFencedException.class, () -> open(graph, fixture));

        assertTrue(refused.getMessage().contains("never shipped"), refused.getMessage());
        assertEquals(claims, count(fixture.store(), ChainLayout.EPOCH_PREFIX));
        assertTrue(noNewThreads(before));
        DurableGraph.open(graph, SYNC).kernel().close();
    }

    private static long count(ObjectStore store, String prefix) {
        long count = 0;
        String after = "";
        ListPage page;
        do {
            page = store.list(prefix, after, 1000);
            count += page.entries().size();
            after = page.lastKey();
        } while (page.truncated());
        return count;
    }

    private static void deleteTree(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            for (Path entry : (Iterable<Path>) walk.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(entry);
            }
        }
    }
}
