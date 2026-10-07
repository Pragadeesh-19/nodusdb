package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ExpiredCredentialsException;
import io.nodusdb.objectstore.FatalStoreException;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.PutResult;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ObjectStoreTest {

    private static final Predicate<FakeS3Server.Req> ANY = request -> true;

    @TempDir
    Path scratch;

    private FakeS3Server server;
    private S3ObjectStore store;

    @BeforeEach
    void start() {
        server = FakeS3Server.start();
        store = storeFor(server.config(), FakeS3Server.CREDENTIALS);
    }

    @AfterEach
    void stop() {
        store.close();
        server.close();
    }

    private static S3ObjectStore storeFor(S3Config config, Credentials credentials) {
        return new S3ObjectStore(config, new StaticCredentialsProvider(credentials));
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static Predicate<FakeS3Server.Req> method(String method) {
        return request -> request.method().equals(method);
    }

    @Test
    void aConditionalWriteSendsIfNoneMatchAndItsMetadataAsSignedHeaders() {
        store.putIfAbsent("chain/1.obj", bytes("x"), Map.of("chain-seq", "7"));

        FakeS3Server.Req request = server.requests().get(0);
        assertEquals("PUT", request.method());
        assertEquals("/test-bucket/chain/1.obj", request.path());
        assertEquals("*", request.headers().get("if-none-match"));
        assertEquals("7", request.headers().get("x-amz-meta-chain-seq"));
        String authorization = request.headers().get("authorization");
        assertTrue(authorization.contains("if-none-match"), authorization);
        assertTrue(authorization.contains("x-amz-meta-chain-seq"), authorization);
        assertTrue(authorization.contains("Credential=TESTACCESSKEY/"), authorization);
        assertEquals(FakeS3Server.REGION + "/s3/aws4_request",
                authorization.split("Credential=")[1].split(",")[0].split("/", 3)[2]);
    }

    @Test
    void aRangeReadAsksForTheInclusiveByteRange() {
        store.put("data/blob", bytes("0123456789"));

        store.getRange("data/blob", 2, 3);

        assertEquals("bytes=2-4", server.requests().get(1).headers().get("range"));
    }

    @Test
    void aListingAsksForVersionTwoWithUrlEncodingAndTheGivenLimits() {
        store.list("_nodus/chain/", "_nodus/chain/5", 25);

        String query = server.requests().get(0).query();
        assertTrue(query.contains("list-type=2"), query);
        assertTrue(query.contains("max-keys=25"), query);
        assertTrue(query.contains("encoding-type=url"), query);
        assertTrue(query.contains("prefix=_nodus%2Fchain%2F"), query);
        assertTrue(query.contains("start-after=_nodus%2Fchain%2F5"), query);
    }

    @Test
    void aWrongSecretIsRefusedAsFatalWithoutAnyRetryLoop() {
        S3ObjectStore wrong = storeFor(server.config(), new Credentials("TESTACCESSKEY", "not-the-secret"));

        FatalStoreException refused = assertThrows(FatalStoreException.class, () -> wrong.put("a/b", bytes("x")));

        assertEquals(403, refused.status());
        assertTrue(refused.getMessage().contains("SignatureDoesNotMatch"), refused.getMessage());
        assertFalse(refused.getMessage().contains("not-the-secret"));
        assertEquals(1, server.requests().size());
        wrong.close();
    }

    @ParameterizedTest
    @CsvSource({
            "408, RequestTimeout, transient", "429, SlowDown, transient", "500, InternalError, transient",
            "502, BadGateway, transient", "503, SlowDown, transient", "504, GatewayTimeout, transient",
            "400, InvalidRequest, fatal", "403, AccessDenied, fatal", "404, NoSuchBucket, fatal",
            "405, MethodNotAllowed, fatal", "501, NotImplemented, fatal", "301, PermanentRedirect, fatal",
            "400, ExpiredToken, expired"
    })
    void everyStatusMapsToTheRightKindOfFailure(int status, String code, String kind) {
        server.failNext(ANY, 1, status, code);

        Class<? extends ObjectStoreException> expected = switch (kind) {
            case "transient" -> TransientStoreException.class;
            case "fatal" -> FatalStoreException.class;
            default -> ExpiredCredentialsException.class;
        };
        ObjectStoreException failure = assertThrows(expected, () -> store.put("a/b", bytes("x")));

        assertEquals(status, failure.status());
        assertTrue(failure.getMessage().contains(code), failure.getMessage());
    }

    @Test
    void aMissingKeyReadsAsAbsentButAMissingBucketIsAnError() {
        assertEquals(Optional.empty(), store.get("never/written"));
        server.failNext(ANY, 1, 404, "NoSuchBucket");

        assertThrows(FatalStoreException.class, () -> store.get("never/written"));
    }

    @Test
    void aConditionalWriteMapsPreconditionAndConflictToTheirOutcomes() {
        assertEquals(PutResult.CREATED, store.putIfAbsent("a/b", bytes("1")));
        assertEquals(PutResult.ALREADY_EXISTS, store.putIfAbsent("a/b", bytes("2")));
        server.failNext(method("PUT"), 1, 409, "ConditionalRequestConflict");

        assertEquals(PutResult.CONFLICT, store.putIfAbsent("a/c", bytes("3")));
        assertEquals(PutResult.CREATED, store.putIfAbsent("a/c", bytes("3")));
    }

    @Test
    void aConflictOnAnUnconditionalWriteIsTransient() {
        server.failNext(method("PUT"), 1, 409, "OperationAborted");

        assertThrows(TransientStoreException.class, () -> store.put("a/b", bytes("1")));
    }

    @Test
    void aResponseLostAfterTheWriteLandedIsTransientAndTheObjectIsThere() {
        server.dropNext(method("PUT"), 1, true);

        assertThrows(TransientStoreException.class, () -> store.putIfAbsent("chain/1.obj", bytes("landed")));

        assertEquals(1, server.count(request -> request.path().endsWith("/chain/1.obj")));
        assertArrayEquals(bytes("landed"), server.content("chain/1.obj"));
        assertEquals(PutResult.ALREADY_EXISTS, store.putIfAbsent("chain/1.obj", bytes("again")));
    }

    @Test
    void aRequestDroppedBeforeItWasProcessedIsTransientAndLeavesNothingBehind() {
        server.dropNext(method("PUT"), 1, false);

        assertThrows(TransientStoreException.class, () -> store.putIfAbsent("chain/1.obj", bytes("dropped")));

        assertEquals(1, server.count(request -> request.path().endsWith("/chain/1.obj")));
        assertNull(server.content("chain/1.obj"));
        assertEquals(PutResult.CREATED, store.putIfAbsent("chain/1.obj", bytes("retried")));
    }

    @Test
    void aSlowServerTimesOutAsATransientFailure() {
        S3Config impatient = server.config().withTimeouts(Duration.ofSeconds(2), Duration.ofMillis(300),
                Duration.ofSeconds(5));
        S3ObjectStore quick = storeFor(impatient, FakeS3Server.CREDENTIALS);
        server.delayNext(ANY, 1, 1_500);
        long started = System.nanoTime();

        TransientStoreException timeout = assertThrows(TransientStoreException.class, () -> quick.head("a/b"));

        assertTrue(Duration.ofNanos(System.nanoTime() - started).toMillis() < 1_300);
        assertTrue(timeout.getMessage().contains("did not complete"), timeout.getMessage());
        quick.close();
    }

    @Test
    void aServerThatIsGoneIsATransientFailure() {
        server.close();

        assertThrows(TransientStoreException.class, () -> store.get("a/b"));
        assertThrows(TransientStoreException.class, () -> store.putIfAbsent("a/b", bytes("x")));
    }

    @Test
    void rotatedCredentialsAreAdoptedAfterOneRefusedRequest() throws IOException {
        Path file = scratch.resolve("credentials");
        writeCredentials(file, FakeS3Server.CREDENTIALS);
        S3ObjectStore rotating = new S3ObjectStore(server.config(), new FileCredentialsProvider(file, "default"));
        rotating.put("k/1", bytes("x"));
        Credentials rotated = new Credentials("ROTATEDKEY", "rotated-secret");
        server.rotateCredentials(rotated);
        writeCredentials(file, rotated);
        int before = server.requests().size();

        rotating.put("k/2", bytes("y"));

        assertEquals(2, server.requests().size() - before);
        assertArrayEquals(bytes("y"), server.content("k/2"));
        rotating.close();
    }

    @Test
    void credentialsThatDidNotChangeAreNotRetried() throws IOException {
        Path file = scratch.resolve("credentials");
        writeCredentials(file, FakeS3Server.CREDENTIALS);
        S3ObjectStore stale = new S3ObjectStore(server.config(), new FileCredentialsProvider(file, "default"));
        server.rotateCredentials(new Credentials("ROTATEDKEY", "rotated-secret"));
        int before = server.requests().size();

        assertThrows(FatalStoreException.class, () -> stale.put("k/1", bytes("x")));

        assertEquals(1, server.requests().size() - before);
        stale.close();
    }

    @Test
    void anExpiredTokenIsReportedAsExpiredCredentialsAndRecoversWhenRenewed() {
        Credentials temporary = new Credentials("TESTACCESSKEY", "test-secret-access-key", "token-1");
        server.rotateCredentials(temporary);
        S3ObjectStore withToken = storeFor(server.config(), temporary);
        withToken.put("k/1", bytes("x"));
        server.expireTokens(true);

        ExpiredCredentialsException expired = assertThrows(ExpiredCredentialsException.class,
                () -> withToken.put("k/2", bytes("y")));
        assertEquals(400, expired.status());

        server.expireTokens(false);
        withToken.put("k/2", bytes("y"));
        assertArrayEquals(bytes("y"), server.content("k/2"));
        withToken.close();
    }

    @Test
    void theSessionTokenTravelsOnEveryRequest() {
        Credentials temporary = new Credentials("TESTACCESSKEY", "test-secret-access-key", "token-1");
        server.rotateCredentials(temporary);
        S3ObjectStore withToken = storeFor(server.config(), temporary);

        withToken.put("k/1", bytes("x"));
        withToken.get("k/1");

        for (FakeS3Server.Req request : server.requests()) {
            assertEquals("token-1", request.headers().get("x-amz-security-token"));
        }
        withToken.close();
    }

    @Test
    void foreignKeysInAListingAreSkippedNotFailed() {
        store.put("chain/1.obj", bytes("x"));
        server.putRaw("Chain/Upper.obj", bytes("x"));
        server.putRaw("chain/with space.obj", bytes("x"));
        server.putRaw("chain/.hidden", bytes("x"));
        store.put("chain/2.obj", bytes("x"));

        ListPage page = store.list("", "", 100);

        assertEquals(List.of("chain/1.obj", "chain/2.obj"), page.entries().stream().map(e -> e.key()).toList());
    }

    @Test
    void aConfiguredPrefixScopesEveryOperation() {
        S3ObjectStore tenantA = storeFor(server.config().withPrefix("tenant/a"), FakeS3Server.CREDENTIALS);
        S3ObjectStore tenantB = storeFor(server.config().withPrefix("tenant/b/"), FakeS3Server.CREDENTIALS);

        tenantA.put("chain/1.obj", bytes("a1"));
        tenantA.put("chain/2.obj", bytes("a2"));
        tenantB.put("chain/1.obj", bytes("b1"));

        assertArrayEquals(bytes("a1"), server.content("tenant/a/chain/1.obj"));
        assertEquals(List.of("chain/1.obj", "chain/2.obj"),
                tenantA.list("chain/", "", 10).entries().stream().map(e -> e.key()).toList());
        assertEquals(List.of("chain/2.obj"),
                tenantA.list("chain/", "chain/1.obj", 10).entries().stream().map(e -> e.key()).toList());
        assertEquals(List.of("chain/1.obj"),
                tenantB.list("", "", 10).entries().stream().map(e -> e.key()).toList());
        tenantA.delete("chain/1.obj");
        assertNull(server.content("tenant/a/chain/1.obj"));
        assertNotNull(server.content("tenant/b/chain/1.obj"));
        tenantA.close();
        tenantB.close();
    }

    @Test
    void deletingMoreThanOneBatchSendsSeveralRequests() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 2_500; i++) {
            String key = String.format("bulk/%05d", i);
            server.putRaw(key, bytes("x"));
            keys.add(key);
        }
        server.putRaw("keep/me", bytes("x"));

        store.deleteAll(keys);

        assertEquals(3, server.count(request -> request.method().equals("POST") && request.has("delete")));
        assertEquals(1, server.objectCount());
        assertNotNull(server.content("keep/me"));
    }

    @Test
    void anObjectThatCannotBeDeletedIsReportedAfterTheOthersAreRemoved() {
        for (String key : new String[]{"d/1", "d/2", "d/3"}) {
            server.putRaw(key, bytes("x"));
        }
        server.failDeleteOf("d/2");

        TransientStoreException failure = assertThrows(TransientStoreException.class,
                () -> store.deleteAll(List.of("d/1", "d/2", "d/3")));

        assertTrue(failure.getMessage().contains("1 of 3"), failure.getMessage());
        assertTrue(failure.getMessage().contains("AccessDenied"), failure.getMessage());
        assertNull(server.content("d/1"));
        assertNotNull(server.content("d/2"));
        assertNull(server.content("d/3"));
    }

    @Test
    void deletingNothingSendsNothing() {
        store.deleteAll(List.of());

        assertEquals(0, server.requests().size());
    }

    @Test
    void aServerThatIgnoresTheRangeHeaderStillYieldsTheRequestedBytes() {
        server.ignoreRange();
        store.put("data/blob", bytes("0123456789"));

        assertArrayEquals(bytes("234"), store.getRange("data/blob", 2, 3).orElseThrow());
        assertArrayEquals(bytes("789"), store.getRange("data/blob", 7, 50).orElseThrow());
        assertThrows(FatalStoreException.class, () -> store.getRange("data/blob", 10, 1));
    }

    @Test
    void anInvalidRangeIsFatalWith416() {
        store.put("data/blob", bytes("0123"));

        FatalStoreException refused = assertThrows(FatalStoreException.class, () -> store.getRange("data/blob", 4, 1));

        assertEquals(416, refused.status());
    }

    @Test
    void headReportsMetadataSizeAndTheServersModificationTime() {
        server.clock(() -> 1_700_000_000_000L);
        store.putIfAbsent("snap/1", bytes("abcdef"), Map.of("chain-seq", "9"));

        var info = store.head("snap/1").orElseThrow();

        assertEquals(6, info.size());
        assertEquals(1_700_000_000_000L, info.lastModifiedMillis());
        assertEquals(Map.of("chain-seq", "9"), info.metadata());
    }

    @Test
    void aFileUnderTheMultipartThresholdIsSentAsOneSignedRequest() throws IOException {
        Path file = scratch.resolve("blob.bin");
        byte[] content = new byte[3 << 20];
        new Random(11).nextBytes(content);
        Files.write(file, content);

        store.putFile("snap/blob", file, Map.of("chain-seq", "4"));

        assertArrayEquals(content, server.content("snap/blob"));
        assertEquals(1, server.count(method("PUT")));
        assertEquals(Map.of("chain-seq", "4"), server.metadataOf("snap/blob"));
    }

    @Test
    void anEmptyFileIsStoredAsAnEmptyObject() throws IOException {
        Path file = scratch.resolve("empty.bin");
        Files.write(file, new byte[0]);

        store.putFile("snap/empty", file, Map.of());

        assertEquals(0, server.content("snap/empty").length);
    }

    @Test
    void aMissingFileIsAnErrorBeforeAnyRequest() {
        assertThrows(UncheckedIOException.class,
                () -> store.putFile("snap/none", scratch.resolve("absent.bin"), Map.of()));
        assertEquals(0, server.requests().size());
    }

    @Test
    void invalidArgumentsFailBeforeAnyRequestIsSent() {
        assertThrows(IllegalArgumentException.class, () -> store.put("Bad Key", bytes("x")));
        assertThrows(IllegalArgumentException.class, () -> store.putIfAbsent("a/b", bytes("x"), Map.of("Bad", "x")));
        assertThrows(IllegalArgumentException.class, () -> store.list("a//b", "", 10));
        assertThrows(IllegalArgumentException.class, () -> store.deleteAll(List.of("ok/key", "Bad Key")));

        assertEquals(0, server.requests().size());
    }

    @Test
    void manyThreadsCanUseOneStoreAtTheSameTime() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(12);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int thread = 0; thread < 12; thread++) {
                int id = thread;
                results.add(pool.submit(() -> {
                    for (int i = 0; i < 25; i++) {
                        String key = "load/" + id + "/" + i;
                        store.put(key, bytes(key));
                        if (!Arrays.equals(bytes(key), store.get(key).orElseThrow())) {
                            return false;
                        }
                    }
                    return true;
                }));
            }
            for (Future<Boolean> result : results) {
                assertTrue(result.get(60, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(300, server.objectCount());
    }

    @Test
    void theClockDecidesTheSigningTime() {
        S3ObjectStore dated = new S3ObjectStore(server.config(), new StaticCredentialsProvider(FakeS3Server.CREDENTIALS),
                Clock.fixed(Instant.parse("2026-03-04T05:06:07Z"), ZoneOffset.UTC),
                MultipartUploader.DEFAULT_PART_BYTES, MultipartUploader.DEFAULT_PART_BYTES, attempt -> { });

        dated.put("a/b", bytes("x"));

        assertEquals("20260304T050607Z", server.requests().get(0).headers().get("x-amz-date"));
        dated.close();
    }

    private static void writeCredentials(Path file, Credentials credentials) throws IOException {
        Files.writeString(file, "[default]\naws_access_key_id=" + credentials.accessKeyId()
                + "\naws_secret_access_key=" + credentials.secretAccessKey() + "\n", StandardCharsets.UTF_8);
    }
}
