package io.nodusdb.ship;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.log.io.DirectoryLogFileSystem;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.storage.DurableGraph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableGraphShippingTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final long WAIT_NANOS = TimeUnit.SECONDS.toNanos(20);

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

    @Test
    void aLakeWriteIsInTheBucketWhenItReturns() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel kernel = open(graph, fixture);
        try {
            TupleStore store = schemaStore(kernel);

            Token token = store.write(grant("document:a"), Durability.LAKE);

            ChainAudit.Result audit = ChainAudit.verify(fixture.store(),
                    new DirectoryLogFileSystem(graph.resolve("log")), token.lsn());
            assertEquals(token.lsn(), audit.lastLsn());
            assertEquals(List.of(token.epoch()), audit.epochs());
            assertTrue(kernel.shipWatermark().configured());
            assertTrue(kernel.shipWatermark().shippedLsn() >= token.lsn());
        } finally {
            kernel.close();
        }
    }

    @Test
    void aLocalWriteIsShippedInTheBackground() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel kernel = open(graph, fixture);
        try {
            TupleStore store = schemaStore(kernel);

            Token token = store.add("document:a", "viewer", "user:alice");
            kernel.shipWatermark().awaitShipped(token.epoch(), token.lsn(), WAIT_NANOS);

            ChainAudit.verify(fixture.store(), new DirectoryLogFileSystem(graph.resolve("log")), token.lsn());
        } finally {
            kernel.close();
        }
    }

    @Test
    void closingShipsEverythingThatWasAcknowledgedLocally() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root).withShip("\"interval_ms\":3600000");
        Path graph = root.resolve("graph");
        GraphKernel kernel = open(graph, fixture);
        TupleStore store = schemaStore(kernel);
        kernel.shipWatermark().awaitShipped(kernel.epoch(), kernel.appliedLsn(), WAIT_NANOS);
        Token last = null;
        for (int i = 0; i < 50; i++) {
            last = store.add("document:d" + i, "viewer", "user:alice");
        }
        assertTrue(kernel.shipWatermark().shippedLsn() < last.lsn(), "the writes must still be waiting to ship");

        kernel.close();

        ChainAudit.Result audit = ChainAudit.verify(fixture.store(), null, last.lsn());
        assertEquals(last.lsn(), audit.lastLsn());
    }

    @Test
    void aRestartContinuesTheSameChainInANewEpoch() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        Path graph = root.resolve("graph");
        GraphKernel first = open(graph, fixture);
        Token before = schemaStore(first).write(grant("document:a"), Durability.LAKE);
        first.close();

        GraphKernel second = open(graph, fixture);
        try {
            Token after = TupleStore.open(second).write(grant("document:b"), Durability.LAKE);

            assertTrue(after.epoch() > before.epoch());
            ChainAudit.Result audit = ChainAudit.verify(fixture.store(), null, after.lsn());
            assertEquals(List.of(before.epoch(), after.epoch()), audit.epochs());
        } finally {
            second.close();
        }
    }

    @Test
    void aDirectoryThatIsBehindTheChainIsRefusedAndTheBucketIsLeftAlone() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphKernel shipped = open(root.resolve("graph"), fixture);
        schemaStore(shipped).write(grant("document:a"), Durability.LAKE);
        shipped.close();
        ObjectStore store = fixture.store();
        long epochClaimsBefore = count(store, ChainLayout.EPOCH_PREFIX);

        WriterFencedException refused = assertThrows(WriterFencedException.class,
                () -> open(root.resolve("another"), fixture));

        assertTrue(refused.getMessage().contains("older than the chain"), refused.getMessage());
        assertEquals(epochClaimsBefore, count(store, ChainLayout.EPOCH_PREFIX));
        DurableGraph.open(root.resolve("another"), SYNC).kernel().close();
    }

    @Test
    void anOrphanEpochClaimIsSkippedAndTheClaimedEpochIsTheOneInUse() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        ObjectStore bucket = fixture.store();
        EpochClaims orphan = new EpochClaims(bucket, 7, ChainBuilder.KEY_ID, () -> 1_700_000_000_000_000L);
        assertEquals(1, orphan.claim(1, 4));
        assertEquals(2, orphan.claim(2, 4));
        GraphKernel kernel = open(root.resolve("graph"), fixture);
        try {
            Token token = schemaStore(kernel).write(grant("document:a"), Durability.LAKE);

            assertEquals(3, kernel.epoch());
            assertEquals(3, token.epoch());
            assertEquals(List.of(3L), ChainAudit.verify(bucket, null, token.lsn()).epochs());
        } finally {
            kernel.close();
        }
    }

    @Test
    void aGraphWithoutShippingStillRefusesLakeWrites() throws IOException {
        Path graph = root.resolve("plain");
        GraphKernel kernel = DurableGraph.open(graph, SYNC).kernel();
        try {
            TupleStore store = schemaStore(kernel);

            assertThrows(UnsupportedFeatureException.class,
                    () -> store.write(grant("document:a"), Durability.LAKE));
        } finally {
            kernel.close();
        }
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
}
