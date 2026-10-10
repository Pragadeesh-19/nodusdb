package io.nodusdb.objectstore.s3;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.s3.FaultProxy.Fault;
import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.replica.FollowerRuntime;
import io.nodusdb.replica.FollowerState.Phase;
import io.nodusdb.ship.ShippingConfig;
import io.nodusdb.ship.ShippingFixture;
import io.nodusdb.storage.DurableGraph;
import io.nodusdb.storage.GraphDigest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ChaosTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);

    private record Upstream(URI endpoint, String region, String bucket, String accessKey, String secretKey,
                            String prefix) {
    }

    @TempDir
    Path root;

    private FakeS3Server fake;
    private FaultProxy proxy;
    private Upstream upstream;
    private ShippingFixture keys;
    private final List<AutoCloseable> open = new ArrayList<>();

    private static String setting(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    @BeforeEach
    void start() throws IOException {
        keys = ShippingFixture.in(root);
        String endpoint = System.getenv("NODUS_S3_ENDPOINT");
        if (endpoint != null && !endpoint.isBlank()) {
            upstream = new Upstream(URI.create(endpoint), setting("NODUS_S3_REGION", "us-east-1"),
                    setting("NODUS_S3_BUCKET", "nodus-test"), setting("NODUS_S3_ACCESS_KEY", "nodus"),
                    setting("NODUS_S3_SECRET_KEY", "nodus-secret-key"), "chaos-" + UUID.randomUUID() + "/");
        } else {
            fake = FakeS3Server.start();
            upstream = new Upstream(fake.endpoint(), FakeS3Server.REGION, FakeS3Server.BUCKET,
                    FakeS3Server.CREDENTIALS.accessKeyId(), FakeS3Server.CREDENTIALS.secretAccessKey(), "chaos/");
        }
        proxy = new FaultProxy(upstream.endpoint());
    }

    @AfterEach
    void stop() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) {
            open.get(i).close();
        }
        proxy.close();
        if (fake != null) {
            fake.close();
        }
    }

    private String store(URI endpoint) {
        return "{\"type\":\"s3\",\"bucket\":\"" + upstream.bucket() + "\",\"region\":\"" + upstream.region()
                + "\",\"endpoint\":\"" + endpoint + "\",\"path_style\":true,\"prefix\":\"" + upstream.prefix()
                + "\"}";
    }

    private String credentials() {
        return "{\"source\":\"static\",\"access_key_id\":\"" + upstream.accessKey()
                + "\",\"secret_access_key\":\"" + upstream.secretKey() + "\"}";
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }

    private ShippingConfig shipping(URI endpoint) {
        return ShippingConfig.parse("{\"store\":" + store(endpoint) + ",\"credentials\":" + credentials()
                + ",\"signing\":{\"key_file\":\"" + escape(keys.privateKeyFile()) + "\",\"key_id\":3,"
                + "\"public_key_file\":\"" + escape(keys.publicKeyFile()) + "\"},"
                + "\"ship\":{\"interval_ms\":10,\"request_timeout_ms\":5000}}");
    }

    private FollowerConfig follower(URI endpoint) {
        return FollowerConfig.parse("{\"store\":" + store(endpoint) + ",\"credentials\":" + credentials()
                + ",\"trust\":{\"key_id\":3,\"public_key_file\":\"" + escape(keys.publicKeyFile()) + "\"},"
                + "\"follow\":{\"poll_interval_ms\":10,\"request_timeout_ms\":5000}}");
    }

    private GraphKernel writer(URI endpoint) throws IOException {
        GraphKernel kernel = DurableGraph.open(root.resolve("graph"), SYNC, GraphKernel.NO_MEMORY_LIMIT,
                shipping(endpoint)).kernel();
        open.add(kernel);
        return kernel;
    }

    private FollowerRuntime follow(URI endpoint) throws IOException {
        FollowerRuntime runtime = FollowerRuntime.start(follower(endpoint), GraphKernel.NO_MEMORY_LIMIT);
        open.add(runtime);
        return runtime;
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
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

    private List<String> chainKeys() {
        S3Config config = S3Config.of(upstream.endpoint(), upstream.region(), upstream.bucket())
                .withPathStyle(true).withPrefix(upstream.prefix());
        S3ObjectStore direct = new S3ObjectStore(config, new StaticCredentialsProvider(
                new Credentials(upstream.accessKey(), upstream.secretKey())), Clock.systemUTC());
        try {
            List<String> keys = new ArrayList<>();
            String after = "";
            ListPage page;
            do {
                page = direct.list(ChainLayout.CHAIN_PREFIX, after, 1000);
                page.entries().stream().map(ObjectInfo::key).forEach(keys::add);
                after = page.lastKey();
            } while (page.truncated());
            return keys;
        } finally {
            direct.close();
        }
    }

    @Test
    void aLostReplyToAChainObjectWriteIsRetriedWithoutLosingOrDuplicatingAnObject() throws IOException {
        proxy.inject(request -> request.method().equals("PUT") && request.target().contains("_nodus/chain/"),
                Fault.DROP_RESPONSE, 3);
        GraphKernel writer = writer(proxy.endpoint());
        TupleStore tuples = TupleStore.open(writer);
        tuples.applySchema(SchemaFixtures.DOCUMENTS);
        Token last = null;
        for (int i = 0; i < 6; i++) {
            last = tuples.write(new TupleTransaction().add("document:d" + i, "viewer", "user:alice"),
                    Durability.LAKE);
        }
        FollowerRuntime follower = follow(upstream.endpoint());
        Token reached = last;

        await(() -> follower.state().phase() != Phase.BOOTSTRAPPING && follower.kernel().appliedLsn() >= reached.lsn(),
                "the follower to reach the last write");

        assertEquals(GraphDigest.of(writer), GraphDigest.of(follower.kernel()));
        List<String> chain = chainKeys();
        for (int i = 0; i < chain.size(); i++) {
            assertEquals(ChainLayout.chainKey(i + 1L), chain.get(i), "the chain has a gap or a stray object");
        }
        long chainPuts = proxy.requests().stream()
                .filter(request -> request.method().equals("PUT") && request.target().contains("_nodus/chain/"))
                .count();
        assertTrue(chainPuts > chain.size(), "the proxy dropped replies, so some objects were written twice");
    }

    @Test
    void aCutSnapshotDownloadIsRetriedAndTheFollowerStillBootstraps() throws IOException {
        GraphKernel writer = writer(upstream.endpoint());
        TupleStore tuples = TupleStore.open(writer);
        tuples.applySchema(SchemaFixtures.DOCUMENTS);
        Token last = tuples.write(new TupleTransaction().add("document:a", "viewer", "user:alice"),
                Durability.LAKE);
        proxy.inject(request -> request.method().equals("GET") && request.target().contains("_nodus/snapshots/"),
                Fault.TRUNCATE_RESPONSE, 2);
        FollowerRuntime follower = follow(proxy.endpoint());

        await(() -> follower.state().phase() == Phase.CURRENT && follower.kernel().appliedLsn() >= last.lsn(),
                "the follower to become current");

        assertEquals(GraphDigest.of(writer), GraphDigest.of(follower.kernel()));
        long snapshotGets = proxy.requests().stream()
                .filter(request -> request.method().equals("GET") && request.target().contains("_nodus/snapshots/"))
                .count();
        assertTrue(snapshotGets >= 3, "the cut downloads were retried: " + snapshotGets);
    }

    @Test
    void aFollowerBehindAFlakyNetworkNeverAppliesAHalfObject() throws IOException {
        GraphKernel writer = writer(upstream.endpoint());
        TupleStore tuples = TupleStore.open(writer);
        tuples.applySchema(SchemaFixtures.DOCUMENTS);
        FollowerRuntime follower = follow(proxy.endpoint());
        proxy.inject(request -> request.method().equals("GET") && request.target().contains("_nodus/chain/"),
                Fault.TRUNCATE_RESPONSE, 4);
        proxy.inject(request -> request.method().equals("GET") && request.target().contains("_nodus/chain/"),
                Fault.DROP_RESPONSE, 4);
        Token last = null;
        for (int i = 0; i < 8; i++) {
            last = tuples.write(new TupleTransaction().add("document:d" + i, "viewer", "user:alice"),
                    Durability.LAKE);
        }
        Token reached = last;

        await(() -> follower.state().phase() != Phase.BOOTSTRAPPING && follower.kernel().appliedLsn() >= reached.lsn(),
                "the follower to reach the last write");

        assertEquals(GraphDigest.of(writer), GraphDigest.of(follower.kernel()));
        assertTrue(follower.state().phase() != Phase.STALLED, follower.state().snapshot(System.nanoTime()).toString());
    }
}
