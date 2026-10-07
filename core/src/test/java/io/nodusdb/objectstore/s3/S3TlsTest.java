package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3TlsTest {

    @TempDir
    Path scratch;

    private FakeS3Server server;

    private static Path resource(String name) throws URISyntaxException {
        return Path.of(S3TlsTest.class.getResource("/tls/" + name).toURI());
    }

    @BeforeEach
    void start() throws URISyntaxException {
        server = FakeS3Server.startSecure(resource("server.p12"));
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private S3ObjectStore storeTrusting(Path bundle) {
        S3Config config = bundle == null ? server.config() : server.config().withCaBundle(bundle);
        return new S3ObjectStore(config, new StaticCredentialsProvider(FakeS3Server.CREDENTIALS), Clock.systemUTC());
    }

    @Test
    void aServerWhoseCertificateIsInTheBundleIsTrusted() throws Exception {
        try (S3ObjectStore store = storeTrusting(resource("ca.pem"))) {
            store.put("a/b", "secure".getBytes(StandardCharsets.UTF_8));

            assertArrayEquals("secure".getBytes(StandardCharsets.UTF_8), store.get("a/b").orElseThrow());
            assertEquals(2, server.requests().size());
        }
    }

    @Test
    void aSelfSignedServerIsRefusedWithoutABundle() {
        try (S3ObjectStore store = storeTrusting(null)) {
            TransientStoreException refused = assertThrows(TransientStoreException.class,
                    () -> store.put("a/b", "x".getBytes(StandardCharsets.UTF_8)));

            assertTrue(refused.getMessage().contains("did not complete"), refused.getMessage());
            assertEquals(0, server.requests().size());
        }
    }

    @Test
    void aBundleThatDoesNotCoverTheServerIsRefused() throws Exception {
        try (S3ObjectStore store = storeTrusting(resource("other-ca.pem"))) {
            assertThrows(TransientStoreException.class, () -> store.put("a/b", "x".getBytes(StandardCharsets.UTF_8)));

            assertEquals(0, server.requests().size());
        }
    }

    @Test
    void aBundleMayHoldSeveralCertificates() throws Exception {
        Path combined = scratch.resolve("combined.pem");
        Files.writeString(combined, Files.readString(resource("other-ca.pem")) + "\n"
                + Files.readString(resource("ca.pem")), StandardCharsets.US_ASCII);

        try (S3ObjectStore store = storeTrusting(combined)) {
            store.put("a/b", "x".getBytes(StandardCharsets.UTF_8));

            assertTrue(store.exists("a/b"));
        }
    }

    @Test
    void aBundleThatIsNotACertificateIsRefusedWhenTheStoreIsCreated() throws IOException {
        Path garbage = scratch.resolve("garbage.pem");
        Files.writeString(garbage, "this is not a certificate", StandardCharsets.US_ASCII);

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> storeTrusting(garbage));

        assertTrue(refused.getMessage().contains("garbage.pem"), refused.getMessage());
    }

    @Test
    void aMissingBundleIsRefusedWhenTheStoreIsCreated() {
        assertThrows(IllegalArgumentException.class, () -> storeTrusting(scratch.resolve("absent.pem")));
    }

    @Test
    void anEmptyBundleIsRefused() throws IOException {
        Path empty = scratch.resolve("empty.pem");
        Files.write(empty, new byte[0]);

        assertThrows(IllegalArgumentException.class, () -> storeTrusting(empty));
    }
}
