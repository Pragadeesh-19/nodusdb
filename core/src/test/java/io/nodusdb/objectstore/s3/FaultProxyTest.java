package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.s3.FaultProxy.Fault;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FaultProxyTest {

    private FakeS3Server upstream;
    private FaultProxy proxy;
    private S3ObjectStore store;

    @BeforeEach
    void start() throws IOException {
        upstream = FakeS3Server.start();
        proxy = new FaultProxy(upstream.endpoint());
        S3Config config = S3Config.of(proxy.endpoint(), FakeS3Server.REGION, FakeS3Server.BUCKET)
                .withPathStyle(true)
                .withTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(5));
        store = new S3ObjectStore(config, new StaticCredentialsProvider(FakeS3Server.CREDENTIALS),
                Clock.systemUTC());
    }

    @AfterEach
    void stop() {
        store.close();
        proxy.close();
        upstream.close();
    }

    @Test
    void anUnfaultedRequestPassesThroughInBothDirections() {
        byte[] content = "hello through the proxy".getBytes();

        store.put("a/key", content);

        assertArrayEquals(content, store.get("a/key").orElseThrow());
        assertFalse(proxy.requests().isEmpty());
    }

    @Test
    void aDroppedResponseLeavesTheWriteAppliedAndTheCallerWithAnError() {
        proxy.inject(request -> request.method().equals("PUT"), Fault.DROP_RESPONSE, 1);

        assertThrows(ObjectStoreException.class, () -> store.put("a/key", new byte[] {1, 2, 3}));

        assertTrue(upstreamHas("a/key"));
    }

    @Test
    void aDroppedRequestNeverReachesTheServer() {
        proxy.inject(request -> request.method().equals("PUT"), Fault.DROP_REQUEST, 1);

        assertThrows(ObjectStoreException.class, () -> store.put("a/key", new byte[] {1, 2, 3}));

        assertFalse(upstreamHas("a/key"));
    }

    @Test
    void aTruncatedResponseIsAnErrorForARead() {
        byte[] content = new byte[4_096];
        store.put("a/key", content);
        proxy.inject(request -> request.method().equals("GET") && request.target().contains("a/key"),
                Fault.TRUNCATE_RESPONSE, 1);

        assertThrows(ObjectStoreException.class, () -> store.get("a/key"));
        assertEquals(content.length, store.get("a/key").orElseThrow().length);
    }

    @Test
    void aDelayedRequestStillSucceedsAfterTheDelay() {
        proxy.delay(request -> request.method().equals("PUT"), 1, 200);
        long begin = System.nanoTime();

        store.put("a/key", new byte[] {1});

        assertTrue(System.nanoTime() - begin >= Duration.ofMillis(150).toNanos());
        assertEquals(Optional.of(1), store.get("a/key").map(bytes -> bytes.length));
    }

    @Test
    void aRuleAppliesOnlyToTheRequestsItMatchesAndOnlyTheGivenNumberOfTimes() {
        proxy.inject(request -> request.method().equals("PUT"), Fault.DROP_RESPONSE, 2);

        assertThrows(ObjectStoreException.class, () -> store.put("one", new byte[] {1}));
        assertThrows(ObjectStoreException.class, () -> store.put("two", new byte[] {1}));
        store.put("three", new byte[] {1});

        assertTrue(store.get("three").isPresent());
        assertTrue(store.get("one").isPresent());
    }

    private boolean upstreamHas(String key) {
        S3ObjectStore direct = new S3ObjectStore(upstream.config().withPathStyle(true),
                new StaticCredentialsProvider(FakeS3Server.CREDENTIALS), Clock.systemUTC());
        try {
            return direct.exists(key);
        } finally {
            direct.close();
        }
    }
}
