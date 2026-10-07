package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ObjectKeys;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

public record S3Config(URI endpoint, String region, String bucket, String prefix, boolean pathStyle,
                       Duration connectTimeout, Duration requestTimeout, Duration transferTimeout, Path caBundle) {

    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration DEFAULT_TRANSFER_TIMEOUT = Duration.ofSeconds(120);
    public static final int MAX_PREFIX_LENGTH = 256;

    private static final Pattern REGION = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");
    private static final Pattern BUCKET = Pattern.compile("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]");
    private static final String AMAZON_SUFFIX = "amazonaws.com";

    public S3Config {
        Objects.requireNonNull(endpoint, "endpoint");
        requireEndpoint(endpoint);
        if (region == null || !REGION.matcher(region).matches()) {
            throw new IllegalArgumentException("region must match [a-z0-9-]");
        }
        if (bucket == null || !BUCKET.matcher(bucket).matches()) {
            throw new IllegalArgumentException("bucket must be 3 to 63 characters from [a-z0-9.-]");
        }
        prefix = normalizePrefix(prefix);
        requirePositive(connectTimeout, "connect timeout");
        requirePositive(requestTimeout, "request timeout");
        requirePositive(transferTimeout, "transfer timeout");
        if (caBundle != null && !"https".equals(endpoint.getScheme())) {
            throw new IllegalArgumentException("a CA bundle needs an https endpoint");
        }
        if (!pathStyle && bucket.indexOf('.') >= 0 && "https".equals(endpoint.getScheme())) {
            throw new IllegalArgumentException("a bucket name with dots needs path-style addressing over https");
        }
    }

    public static S3Config of(URI endpoint, String region, String bucket) {
        boolean pathStyle = endpoint.getHost() == null || !endpoint.getHost().endsWith(AMAZON_SUFFIX);
        return new S3Config(endpoint, region, bucket, "", pathStyle, DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_REQUEST_TIMEOUT, DEFAULT_TRANSFER_TIMEOUT, null);
    }

    public S3Config withPrefix(String newPrefix) {
        return new S3Config(endpoint, region, bucket, newPrefix, pathStyle, connectTimeout, requestTimeout,
                transferTimeout, caBundle);
    }

    public S3Config withPathStyle(boolean newPathStyle) {
        return new S3Config(endpoint, region, bucket, prefix, newPathStyle, connectTimeout, requestTimeout,
                transferTimeout, caBundle);
    }

    public S3Config withTimeouts(Duration connect, Duration request, Duration transfer) {
        return new S3Config(endpoint, region, bucket, prefix, pathStyle, connect, request, transfer, caBundle);
    }

    public S3Config withCaBundle(Path newCaBundle) {
        return new S3Config(endpoint, region, bucket, prefix, pathStyle, connectTimeout, requestTimeout,
                transferTimeout, newCaBundle);
    }

    public boolean secure() {
        return "https".equals(endpoint.getScheme());
    }

    public String hostHeader() {
        String authority = pathStyle ? endpoint.getHost() : bucket + "." + endpoint.getHost();
        int port = endpoint.getPort();
        boolean defaultPort = port < 0 || (secure() && port == 443) || (!secure() && port == 80);
        return defaultPort ? authority : authority + ":" + port;
    }

    public String objectPath(String key) {
        return bucketPath() + SigV4Signer.encodePath(prefix + key);
    }

    public String bucketPath() {
        return pathStyle ? "/" + bucket + "/" : "/";
    }

    public String url(String path, String query) {
        return endpoint.getScheme() + "://" + hostHeader() + path + (query.isEmpty() ? "" : "?" + query);
    }

    private static void requireEndpoint(URI endpoint) {
        String scheme = endpoint.getScheme();
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new IllegalArgumentException("the endpoint must be an http or https URL");
        }
        if (endpoint.getHost() == null || endpoint.getRawUserInfo() != null || endpoint.getRawQuery() != null
                || endpoint.getRawFragment() != null) {
            throw new IllegalArgumentException("the endpoint must be a plain scheme, host and optional port");
        }
        String path = endpoint.getRawPath();
        if (path != null && !path.isEmpty() && !"/".equals(path)) {
            throw new IllegalArgumentException("the endpoint must not carry a path");
        }
    }

    private static String normalizePrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return "";
        }
        String normalized = prefix.endsWith("/") ? prefix : prefix + "/";
        if (normalized.length() > MAX_PREFIX_LENGTH) {
            throw new IllegalArgumentException("the prefix must not be longer than " + MAX_PREFIX_LENGTH
                    + " characters, so that prefix and key fit the 1024 bytes S3 allows");
        }
        return ObjectKeys.requirePrefix(normalized);
    }

    private static void requirePositive(Duration duration, String name) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("the " + name + " must be positive");
        }
    }
}
