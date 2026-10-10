package io.nodusdb.replica;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;
import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.DurableGraph;
import io.nodusdb.storage.GraphSurvey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalvageTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final int SHIPPED_WRITES = 10;
    private static final int LOCAL_WRITES = 5;

    @TempDir
    Path root;

    private static void copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> paths = Files.walk(from)) {
            for (Path source : paths.toList()) {
                Path target = to.resolve(from.relativize(source).toString());
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static List<String> listing(Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            return paths.map(path -> directory.relativize(path) + ":" + path.toFile().length()).sorted().toList();
        }
    }

    private static List<GraphSurvey.Transaction> additions(SalvageReport report) {
        return report.transactions().stream()
                .filter(transaction -> transaction.changes().stream().anyMatch(change -> change.kind().equals("add")))
                .toList();
    }

    private Salvage salvage(ShippingFixture shipping) {
        return new Salvage(shipping.store(), ChainBuilder.keyring());
    }

    private GraphKernel writerWithUnshippedTail(ShippingFixture shipping, Path directory) throws IOException {
        GraphKernel shipped = DurableGraph.open(directory, SYNC, GraphKernel.NO_MEMORY_LIMIT, shipping.config())
                .kernel();
        TupleStore tuples = TupleStore.open(shipped);
        tuples.applySchema(SchemaFixtures.DOCUMENTS);
        for (int i = 0; i < SHIPPED_WRITES; i++) {
            tuples.write(new TupleTransaction().add("document:s" + i, "viewer", "user:alice"), Durability.LAKE);
        }
        shipped.close();
        GraphKernel unshipped = DurableGraph.open(directory, SYNC).kernel();
        TupleStore tail = TupleStore.open(unshipped);
        for (int i = 0; i < LOCAL_WRITES; i++) {
            tail.add("document:l" + i, "viewer", "user:alice");
        }
        return unshipped;
    }

    @Test
    void withoutATakeoverTheChainHeadIsAProvisionalHandoffAndTheUnshippedTailIsListed() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path image = root.resolve("image");
            copyTree(root.resolve("graph"), image);

            SalvageReport report = salvage(shipping).salvage(image);

            assertTrue(report.provisional());
            assertEquals(LOCAL_WRITES, additions(report).size());
            assertEquals(writer.appliedLsn(), report.localLastLsn());
            assertEquals(report.handoffLsn() + 1, report.transactions().get(0).firstLsn());
            assertEquals(0, report.unavailableThroughLsn());
            GraphSurvey.Change first = additions(report).get(0).changes().get(0);
            assertEquals("add", first.kind());
            assertEquals("document:l0", first.object());
            assertEquals("viewer", first.relation());
            assertEquals("user:alice", first.subject());
            assertEquals("", first.subjectRelation());
        } finally {
            writer.close();
        }
    }

    @Test
    void afterATakeoverTheHandoffComesFromTheEpochRecordAndIsFinal() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path image = root.resolve("image");
            copyTree(root.resolve("graph"), image);
            WriterTakeover.Taken taken = WriterTakeover.takeOver(shipping.config(), root.resolve("taken"), SYNC,
                    GraphKernel.NO_MEMORY_LIMIT);
            try {
                SalvageReport report = salvage(shipping).salvage(image);

                assertFalse(report.provisional());
                assertEquals(taken.handoffLsn(), report.handoffLsn());
                List<GraphSurvey.Transaction> lost = additions(report);
                assertEquals(LOCAL_WRITES, lost.size());
                for (int i = 0; i < LOCAL_WRITES; i++) {
                    List<GraphSurvey.Change> changes = lost.get(i).changes();
                    assertEquals(1, changes.size());
                    assertEquals("document:l" + i, changes.get(0).object());
                }
            } finally {
                taken.kernel().close();
            }
        } finally {
            writer.close();
        }
    }

    @Test
    void salvagingNeverChangesTheDirectoryEvenWithATornTail() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path image = root.resolve("image");
            copyTree(root.resolve("graph"), image);
            Path segment;
            try (Stream<Path> files = Files.list(image.resolve("log"))) {
                segment = files.filter(path -> path.getFileName().toString().endsWith(".nlog"))
                        .max(Path::compareTo).orElseThrow();
            }
            Files.write(segment, new byte[] {7, 7, 7, 7, 7, 7, 7}, StandardOpenOption.APPEND);
            List<String> before = listing(image);

            SalvageReport report = salvage(shipping).salvage(image);

            assertEquals(LOCAL_WRITES, additions(report).size());
            assertEquals(before, listing(image));
        } finally {
            writer.close();
        }
    }

    @Test
    void aDirectoryThatAnotherProcessHasOpenIsRefused() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            assertThrows(IllegalStateException.class, () -> salvage(shipping).salvage(root.resolve("graph")));
        } finally {
            writer.close();
        }
    }

    @Test
    void aRangeTrimmedByACheckpointIsReportedAsUnavailableInsteadOfBeingListed() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        long lastLsn = writer.appliedLsn();
        writer.close();

        SalvageReport report = salvage(shipping).salvage(root.resolve("graph"));

        assertTrue(report.transactions().isEmpty());
        assertEquals(lastLsn, report.unavailableThroughLsn());
        assertTrue(report.unavailableThroughLsn() > report.handoffLsn());
    }

    @Test
    void anEmptyBucketIsRefused() {
        Salvage empty = new Salvage(new MemoryObjectStore(), ChainBuilder.keyring());

        assertThrows(IllegalStateException.class, () -> empty.salvage(root));
    }

    @Test
    void aChainSignedByAnotherKeyIsRefused() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path image = root.resolve("image");
            copyTree(root.resolve("graph"), image);
            Keyring stranger = Keyring.single(ChainBuilder.KEY_ID, KeyFiles.generate().getPublic());

            assertThrows(ChainTrustException.class,
                    () -> new Salvage(shipping.store(), stranger).salvage(image));
        } finally {
            writer.close();
        }
    }

    @Test
    void theReportRendersAsJsonThatNamesEveryLostChange() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path image = root.resolve("image");
            copyTree(root.resolve("graph"), image);

            JsonObject json = JsonParser.parseObject(salvage(shipping).salvage(image).toJson());

            assertTrue(json.boolOr("provisional", false));
            JsonArray transactions = json.requireArray("transactions");
            int additions = 0;
            for (int i = 0; i < transactions.size(); i++) {
                JsonObject transaction = (JsonObject) transactions.get(i);
                assertTrue(transaction.requireLong("commit_micros") > 0);
                JsonArray changes = transaction.requireArray("changes");
                for (int c = 0; c < changes.size(); c++) {
                    JsonObject change = (JsonObject) changes.get(c);
                    if (change.requireString("kind").equals("add")) {
                        assertEquals("document:l" + additions, change.requireString("object"));
                        additions++;
                    }
                }
            }
            assertEquals(LOCAL_WRITES, additions);
        } finally {
            writer.close();
        }
    }

    @Test
    void aConfigDocumentNamesTheStoreAndTheTrustedKey() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path image = root.resolve("image");
            copyTree(root.resolve("graph"), image);

            SalvageReport report = Salvage.salvage(FollowerConfig.parse(shipping.followerJson()), image);

            assertEquals(LOCAL_WRITES, additions(report).size());
        } finally {
            writer.close();
        }
    }

    @Test
    void aDirectoryWithoutAnyGraphFilesHasNothingToSalvage() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path empty = Files.createDirectory(root.resolve("empty"));

            SalvageReport report = salvage(shipping).salvage(empty);

            assertTrue(report.transactions().isEmpty());
            assertEquals(0, report.localLastLsn());
        } finally {
            writer.close();
        }
    }

    @Test
    void theTokenOfAnUnshippedWriteIsAboveTheHandoff() throws IOException {
        ShippingFixture shipping = ShippingFixture.in(root);
        GraphKernel writer = writerWithUnshippedTail(shipping, root.resolve("graph"));
        try {
            Path image = root.resolve("image");
            copyTree(root.resolve("graph"), image);
            Token unshipped = new Token(writer.epoch(), writer.appliedLsn());

            SalvageReport report = salvage(shipping).salvage(image);

            assertTrue(unshipped.lsn() > report.handoffLsn());
            assertEquals(unshipped.lsn(), report.transactions().get(report.transactions().size() - 1).lastLsn());
        } finally {
            writer.close();
        }
    }
}
