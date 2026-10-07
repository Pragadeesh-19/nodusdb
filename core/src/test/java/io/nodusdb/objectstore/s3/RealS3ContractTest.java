package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ConditionalWriteProbe;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.ObjectStoreContractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnabledIfEnvironmentVariable(named = "NODUS_S3_ENDPOINT", matches = ".+")
class RealS3ContractTest extends ObjectStoreContractTest {

    private static final long MEBIBYTE = 1L << 20;

    private S3ObjectStore real;

    private static String setting(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static S3Config freshConfig() {
        return S3Config.of(URI.create(System.getenv("NODUS_S3_ENDPOINT")),
                setting("NODUS_S3_REGION", "us-east-1"), setting("NODUS_S3_BUCKET", "nodus-test"))
                .withPrefix("test-" + UUID.randomUUID() + "/").withPathStyle(true);
    }

    private static CredentialsProvider credentials() {
        return new StaticCredentialsProvider(new Credentials(setting("NODUS_S3_ACCESS_KEY", "nodus"),
                setting("NODUS_S3_SECRET_KEY", "nodus-secret-key")));
    }

    private static S3ObjectStore open(long threshold, long part) {
        return new S3ObjectStore(freshConfig(), credentials(), Clock.systemUTC(), threshold, part,
                MultipartUploader.exponentialBackoff());
    }

    @Override
    protected ObjectStore create() {
        real = open(S3ObjectStore.DEFAULT_PART_BYTES, S3ObjectStore.DEFAULT_PART_BYTES);
        return real;
    }

    @AfterEach
    void removeEverythingTheTestWrote() {
        String after = "";
        while (true) {
            ListPage page = real.list("", after, 1000);
            real.deleteAll(page.entries().stream().map(ObjectInfo::key).toList());
            if (!page.truncated()) {
                return;
            }
            after = page.lastKey();
        }
    }

    @Override
    protected int racers() {
        return 4;
    }

    @Override
    protected int racedKeys() {
        return 10;
    }

    @Override
    @Test
    protected void listingReturnsKeysInBinaryOrder() {
        assumeTrue(Boolean.parseBoolean(setting("NODUS_S3_STRICT_ORDER", "false")),
                "some S3-compatible servers list a folder before sibling keys; the flat folders this project "
                        + "uses are covered by the paging tests");
        super.listingReturnsKeysInBinaryOrder();
    }

    @Test
    void theServerHonoursIfNoneMatch() {
        ConditionalWriteProbe.verify(real);
    }

    @Test
    void aLargeFileIsUploadedInPartsAndReadBackIdentically() throws IOException {
        S3ObjectStore multipart = open(6 * MEBIBYTE, 5 * MEBIBYTE);
        byte[] content = new byte[12 * (int) MEBIBYTE + 345];
        new Random(5).nextBytes(content);
        Path file = scratch.resolve("large.bin");
        Files.write(file, content);

        multipart.putFile("snap/large", file, Map.of("chain-seq", "99"));

        assertArrayEquals(content, multipart.get("snap/large").orElseThrow());
        ObjectInfo info = multipart.head("snap/large").orElseThrow();
        assertEquals(content.length, info.size());
        assertEquals(Map.of("chain-seq", "99"), info.metadata());
        multipart.delete("snap/large");
        multipart.close();
    }

    @Test
    void anAbandonedUploadIsFoundAndAbortedByTheCleanup() {
        S3Config config = freshConfig();
        try (S3ObjectStore store = new S3ObjectStore(config, credentials());
             S3Http http = new S3Http(config, credentials(), Clock.systemUTC())) {
            S3Response started = http.send(new S3Http.Request("POST", config.objectPath("snap/abandoned"), "uploads",
                    Map.of(), S3Payload.Bytes.EMPTY, config.requestTimeout()));
            assertTrue(started.successful(), started.text());

            int aborted = store.abortStaleUploads("snap/", Duration.ofSeconds(1), Instant.now().plusSeconds(60));

            assertEquals(1, aborted);
            assertEquals(0, store.abortStaleUploads("snap/", Duration.ofSeconds(1), Instant.now().plusSeconds(60)));
        }
    }

    @Test
    void manyKeysPageThroughAListingWithoutLossOrRepeats() {
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            String key = String.format("chain/%020d.obj", i);
            real.put(key, bytes("v"));
            expected.add(key);
        }

        List<String> seen = new ArrayList<>();
        String after = "";
        while (true) {
            ListPage page = real.list("chain/", after, 7);
            seen.addAll(keys(page));
            if (!page.truncated()) {
                break;
            }
            after = page.lastKey();
        }

        assertEquals(expected, seen);
    }
}
