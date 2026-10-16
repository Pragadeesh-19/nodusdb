package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.erasure.ErasureCoverage;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.ship.ShippingFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionErasureTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final String NAME = "user:alice-bucket-erasure";
    private static final byte[] NAME_BYTES = NAME.substring(NAME.indexOf(':') + 1).getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path root;

    private final GraphSessions sessions = new GraphSessions();
    private final List<Long> handles = new ArrayList<>();

    @AfterEach
    void closeAll() {
        for (long handle : handles) {
            try {
                sessions.close(handle);
            } catch (RuntimeException alreadyClosed) {
                continue;
            }
        }
    }

    private long writerHandle(ShippingFixture fixture) throws IOException {
        long handle = sessions.openDurable(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT, fixture.config());
        handles.add(handle);
        return handle;
    }

    private GraphSession follower(ShippingFixture fixture) throws IOException {
        String json = fixture.followerJson().replace("}}", "},\"follow\":{\"poll_interval_ms\":10,"
                + "\"read_wait_ms\":5000}}");
        long handle = sessions.openFollower(FollowerConfig.parse(json), GraphKernel.NO_MEMORY_LIMIT);
        handles.add(handle);
        return sessions.get(handle);
    }

    private static Token grantAndErase(GraphSession writer) {
        writer.applySchema(SchemaFixtures.DOCUMENTS);
        writer.writeTuples(new TupleTransaction().add("document:readme", "viewer", NAME, null)
                .add("group:eng", "member", NAME, null)
                .add("document:plan", "viewer", "user:bob", null), Durability.LAKE);
        return writer.erase(NAME);
    }

    private static List<Path> filesHolding(Path directory) throws IOException {
        List<Path> holding = new ArrayList<>();
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                if (file.getFileName().toString().equals("nodus.lock")) {
                    continue;
                }
                byte[] content;
                try {
                    content = Files.readAllBytes(file);
                } catch (NoSuchFileException finishedUpload) {
                    continue;
                }
                for (int start = 0; start + NAME_BYTES.length <= content.length; start++) {
                    if (Arrays.equals(content, start, start + NAME_BYTES.length, NAME_BYTES, 0, NAME_BYTES.length)) {
                        holding.add(file);
                        break;
                    }
                }
            }
        }
        return holding;
    }

    private static List<ChainObject> chain(ObjectStore store) {
        List<ChainObject> objects = new ArrayList<>();
        String after = "";
        while (true) {
            ListPage page = store.list(ChainLayout.CHAIN_PREFIX, after, 1000);
            for (ObjectInfo entry : page.entries()) {
                objects.add(ChainCodec.decode(store.get(entry.key()).orElseThrow()));
            }
            if (!page.truncated()) {
                return objects;
            }
            after = page.lastKey();
        }
    }

    private static JsonObject erasure(GraphSession session) {
        return JsonParser.parseObject(session.statsJson()).requireObject("erasure");
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
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

    @Test
    void afterTheWaitNeitherTheDirectoryNorTheBucketHoldsTheName() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = sessions.get(writerHandle(fixture));
        Token token = grantAndErase(writer);
        assertFalse(filesHolding(fixture.bucket()).isEmpty());

        writer.awaitErased(token, WAIT);

        assertEquals(List.of(), filesHolding(root.resolve("graph")));
        assertEquals(List.of(), filesHolding(fixture.bucket()));
        JsonObject bucket = erasure(writer).requireObject("bucket");
        assertTrue(bucket.boolOr("configured", false));
        assertFalse(bucket.boolOr("pending", true));
        List<ChainBody.Redaction> cuts = chain(fixture.store()).stream()
                .filter(object -> object.body() instanceof ChainBody.Redaction)
                .map(object -> (ChainBody.Redaction) object.body()).toList();
        assertEquals(1, cuts.size());
        assertEquals(1, cuts.get(0).erasedCount());
        assertTrue(cuts.get(0).snapshotLsn() >= token.lsn());
        assertTrue(Files.isRegularFile(root.resolve("graph").resolve(ErasureCoverage.FILE)));
    }

    @Test
    void withIcebergTheTableIsRewrittenAndNoLayerHoldsTheNameAfterTheWait() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root).withIceberg("\"enabled\":true,\"commit_interval_s\":1");
        GraphSession writer = sessions.get(writerHandle(fixture));
        Token token = grantAndErase(writer);

        writer.awaitErased(token, WAIT);

        assertEquals(List.of(), filesHolding(fixture.bucket()));
        assertEquals(List.of(), filesHolding(root.resolve("graph")));
        JsonObject lake = erasure(writer).requireObject("lake");
        assertTrue(lake.boolOr("configured", false));
        assertFalse(lake.boolOr("pending", true));
        assertTrue(Files.isDirectory(fixture.bucket().resolve("iceberg").resolve("data")));
    }

    @Test
    void aFollowerStartedAfterTheCutBootstrapsAndSeesOnlyThePseudonym() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        GraphSession writer = sessions.get(writerHandle(fixture));
        Token token = grantAndErase(writer);
        writer.awaitErased(token, WAIT);
        Token after = writer.writeTuples(new TupleTransaction().add("document:later", "viewer", "user:carol", null),
                Durability.LAKE);

        GraphSession follower = follower(fixture);

        await(() -> {
            try {
                return follower.check("document:later", "view", "user:carol", after);
            } catch (RuntimeException notYet) {
                return false;
            }
        }, "the follower to reach the write after the cut");
        assertFalse(follower.check("document:readme", "view", NAME, null));
        assertTrue(follower.check("document:plan", "view", "user:bob", null));
        assertThrows(UnsupportedFeatureException.class, () -> follower.awaitErased(token, WAIT));
        assertThrows(UnsupportedFeatureException.class, () -> follower.erase("user:bob"));
    }

    @Test
    void aRestartKeepsTheBucketCoveredWithoutCuttingAgain() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        long handle = writerHandle(fixture);
        GraphSession writer = sessions.get(handle);
        Token token = grantAndErase(writer);
        writer.awaitErased(token, WAIT);
        sessions.close(handle);
        handles.remove(handle);

        GraphSession reopened = sessions.get(writerHandle(fixture));

        reopened.awaitErased(token, Duration.ZERO);
        assertFalse(erasure(reopened).requireObject("bucket").boolOr("pending", true));
        assertEquals(1, chain(fixture.store()).stream()
                .filter(object -> object.body() instanceof ChainBody.Redaction).count());
    }

    @Test
    void anEraseThatTheWriterClosedBeforeCuttingIsFinishedAfterTheRestart() throws IOException {
        ShippingFixture fixture = ShippingFixture.in(root);
        long handle = writerHandle(fixture);
        Token token = grantAndErase(sessions.get(handle));
        sessions.close(handle);
        handles.remove(handle);
        assertFalse(filesHolding(fixture.bucket()).isEmpty());

        GraphSession reopened = sessions.get(writerHandle(fixture));
        reopened.awaitErased(token, WAIT);

        assertEquals(List.of(), filesHolding(fixture.bucket()));
        assertEquals(List.of(), filesHolding(root.resolve("graph")));
    }
}
