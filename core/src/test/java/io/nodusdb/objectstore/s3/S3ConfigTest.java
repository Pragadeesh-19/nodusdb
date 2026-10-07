package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ObjectKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ConfigTest {

    private static S3Config local() {
        return S3Config.of(URI.create("http://127.0.0.1:9000"), "us-east-1", "my-bucket");
    }

    private static S3Config amazon() {
        return S3Config.of(URI.create("https://s3.eu-west-2.amazonaws.com"), "eu-west-2", "my-bucket");
    }

    @Test
    void customEndpointsDefaultToPathStyleAndAmazonToVirtualHosted() {
        assertTrue(local().pathStyle());
        assertFalse(amazon().pathStyle());
        assertTrue(S3Config.of(URI.create("https://minio.internal.example.com"), "us-east-1", "bucket-1").pathStyle());
    }

    @Test
    void aPathStyleAddressNamesTheBucketInThePath() {
        S3Config config = local().withPrefix("prod/authz");

        assertEquals("127.0.0.1:9000", config.hostHeader());
        assertEquals("/my-bucket/prod/authz/_nodus/chain/1.obj", config.objectPath("_nodus/chain/1.obj"));
        assertEquals("/my-bucket/", config.bucketPath());
        assertEquals("http://127.0.0.1:9000/my-bucket/prod/authz/k?a=1", config.url(config.objectPath("k"), "a=1"));
    }

    @Test
    void aVirtualHostedAddressNamesTheBucketInTheHost() {
        S3Config config = amazon().withPrefix("prod");

        assertEquals("my-bucket.s3.eu-west-2.amazonaws.com", config.hostHeader());
        assertEquals("/prod/k", config.objectPath("k"));
        assertEquals("/", config.bucketPath());
        assertEquals("https://my-bucket.s3.eu-west-2.amazonaws.com/prod/k", config.url(config.objectPath("k"), ""));
    }

    @Test
    void theHostHeaderKeepsOnlyANonDefaultPort() {
        assertEquals("host.example", S3Config.of(URI.create("https://host.example"), "us-east-1", "bucket-1").hostHeader());
        assertEquals("host.example", S3Config.of(URI.create("https://host.example:443"), "us-east-1", "bucket-1").hostHeader());
        assertEquals("host.example", S3Config.of(URI.create("http://host.example:80"), "us-east-1", "bucket-1").hostHeader());
        assertEquals("host.example:8443", S3Config.of(URI.create("https://host.example:8443"), "us-east-1", "bucket-1").hostHeader());
        assertEquals("host.example:443", S3Config.of(URI.create("http://host.example:443"), "us-east-1", "bucket-1").hostHeader());
    }

    @Test
    void aPrefixGetsATrailingSlashExactlyOnce() {
        assertEquals("a/", local().withPrefix("a").prefix());
        assertEquals("a/", local().withPrefix("a/").prefix());
        assertEquals("a/b/", local().withPrefix("a/b").prefix());
        assertEquals("", local().withPrefix("").prefix());
        assertEquals("", local().withPrefix(null).prefix());
    }

    @Test
    void aPrefixAtTheLimitIsKeptAndOneLongerIsRefused() {
        String atLimit = "p".repeat(S3Config.MAX_PREFIX_LENGTH - 1);

        assertEquals(S3Config.MAX_PREFIX_LENGTH, local().withPrefix(atLimit).prefix().length());
        assertThrows(IllegalArgumentException.class, () -> local().withPrefix(atLimit + "p"));
    }

    @Test
    void aPrefixAndAKeyAtTheirLimitsStayInsideWhatS3Allows() {
        int longest = S3Config.MAX_PREFIX_LENGTH + ObjectKeys.MAX_KEY_LENGTH;

        assertTrue(longest <= 1024, "prefix plus key could reach " + longest);
    }

    @ParameterizedTest
    @ValueSource(strings = {"a//b", "A", "a b", "/a", "../x", "a/.b", "nul"})
    void aMalformedPrefixIsRefused(String prefix) {
        assertThrows(IllegalArgumentException.class, () -> local().withPrefix(prefix));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ftp://host", "host.example", "//host", "http://user:pass@host", "http://host/path",
            "http://host?x=1", "http://host#frag", "file:///tmp"})
    void anEndpointMustBeAPlainHttpOrHttpsOrigin(String endpoint) {
        assertThrows(IllegalArgumentException.class, () -> S3Config.of(URI.create(endpoint), "us-east-1", "bucket-1"));
    }

    @Test
    void anEndpointWithATrailingSlashIsAccepted() {
        S3Config.of(URI.create("http://host/"), "us-east-1", "bucket-1");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "US-EAST-1", "us east", "-us", "us_east_1"})
    void aMalformedRegionIsRefused(String region) {
        assertThrows(IllegalArgumentException.class, () -> S3Config.of(URI.create("http://host"), region, "bucket-1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ab", "A-bucket", "bucket_name", "-bucket", "bucket-", ".bucket", "bucket.",
            "a-very-long-bucket-name-that-goes-on-and-on-past-the-sixty-three-character-limit"})
    void aMalformedBucketIsRefused(String bucket) {
        assertThrows(IllegalArgumentException.class, () -> S3Config.of(URI.create("http://host"), "us-east-1", bucket));
    }

    @Test
    void bucketNamesWithDotsAndDigitsAreAccepted() {
        S3Config.of(URI.create("http://host"), "us-east-1", "my.bucket.1");
        S3Config.of(URI.create("http://host"), "us-east-1", "123");
    }

    @Test
    void aDottedBucketOverHttpsNeedsPathStyle() {
        assertThrows(IllegalArgumentException.class, () -> S3Config.of(URI.create("https://s3.amazonaws.com"),
                "us-east-1", "my.bucket"));
        assertTrue(S3Config.of(URI.create("https://storage.example.com"), "us-east-1", "my.bucket").pathStyle());
        assertThrows(IllegalArgumentException.class, () -> S3Config.of(URI.create("https://storage.example.com"),
                "us-east-1", "my.bucket").withPathStyle(false));
    }

    @Test
    void timeoutsMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> local().withTimeouts(Duration.ZERO, Duration.ofSeconds(1), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> local().withTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(-1), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> local().withTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(1), null));
    }

    @Test
    void aCaBundleNeedsAnHttpsEndpoint() {
        assertThrows(IllegalArgumentException.class, () -> local().withCaBundle(Path.of("ca.pem")));
        amazon().withCaBundle(Path.of("ca.pem"));
    }

    @Test
    void theWithersChangeOnlyTheirOwnField() {
        S3Config base = amazon().withPrefix("p");

        S3Config changed = base.withTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(3));

        assertEquals(base.endpoint(), changed.endpoint());
        assertEquals(base.prefix(), changed.prefix());
        assertEquals(base.pathStyle(), changed.pathStyle());
        assertEquals(Duration.ofSeconds(2), changed.requestTimeout());
        assertEquals(Duration.ofSeconds(3), changed.transferTimeout());
    }

    @Test
    void pathsAreEncodedSegmentBySegmentAndKeepTheirSlashes() {
        assertEquals("a%20b/c%2Bd/e.f-g_h~i", SigV4Signer.encodePath("a b/c+d/e.f-g_h~i"));
        assertEquals("", SigV4Signer.encodePath(""));
        assertEquals("a/", SigV4Signer.encodePath("a/"));
        assertEquals("/a", SigV4Signer.encodePath("/a"));
    }
}
