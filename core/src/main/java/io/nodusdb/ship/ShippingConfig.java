package io.nodusdb.ship;

import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

public record ShippingConfig(Store store, CredentialSource credentials, Signing signing, ShipSettings ship,
                             Duration requestTimeout, Iceberg iceberg) {

    public enum StoreType {
        DIRECTORY, S3
    }

    public enum CredentialKind {
        STATIC, ENVIRONMENT, FILE
    }

    public record Store(StoreType type, Path directory, String bucket, String prefix, String region, URI endpoint,
                        Boolean pathStyle, Path caBundle) {
    }

    public record CredentialSource(CredentialKind kind, String accessKeyId, String secretAccessKey,
                                   String sessionToken, Path file, String profile) {

        @Override
        public String toString() {
            return "CredentialSource[kind=" + kind + "]";
        }
    }

    public record Signing(Path keyFile, int keyId, Path publicKeyFile) {
    }

    public record Iceberg(boolean enabled, Duration commitInterval, Duration tableRetention) {
    }

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private static final Set<String> TOP_LEVEL = Set.of("store", "credentials", "signing", "ship", "iceberg");
    private static final Set<String> DIRECTORY_STORE = Set.of("type", "directory");
    private static final Set<String> S3_STORE = Set.of("type", "bucket", "prefix", "region", "endpoint", "path_style",
            "ca_bundle");
    private static final Set<String> STATIC_CREDENTIALS = Set.of("source", "access_key_id", "secret_access_key",
            "session_token");
    private static final Set<String> ENVIRONMENT_CREDENTIALS = Set.of("source");
    private static final Set<String> FILE_CREDENTIALS = Set.of("source", "path", "profile");
    private static final Set<String> SIGNING = Set.of("key_file", "key_id", "public_key_file");
    private static final Set<String> SHIP = Set.of("interval_ms", "max_object_bytes", "backlog_cap_bytes",
            "retention_days", "request_timeout_ms");
    private static final Set<String> ICEBERG = Set.of("enabled", "commit_interval_s", "table_retention_days");
    private static final String DEFAULT_PROFILE = "default";
    private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(60);
    private static final Duration DEFAULT_TABLE_RETENTION = Duration.ofDays(7);
    private static final long MAX_MILLIS = 3_600_000L;
    private static final long MAX_SECONDS = 86_400L;
    private static final long MAX_DAYS = 36_500L;
    private static final long MAX_BYTES = 1L << 50;

    public static ShippingConfig parse(String document) {
        try {
            JsonObject root = JsonParser.parseObject(document.getBytes(StandardCharsets.UTF_8));
            root.requireKnownKeys(TOP_LEVEL);
            Store store = store(root.requireObject("store"));
            CredentialSource credentials = credentials(root, store);
            Signing signing = signing(root.requireObject("signing"));
            JsonObject shipBlock = root.objectOr("ship", JsonObject.EMPTY);
            shipBlock.requireKnownKeys(SHIP);
            return new ShippingConfig(store, credentials, signing, ship(shipBlock), requestTimeout(shipBlock),
                    iceberg(root.objectOr("iceberg", JsonObject.EMPTY)));
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("invalid shipping configuration: " + invalid.getMessage());
        }
    }

    private static Store store(JsonObject block) {
        String type = block.requireString("type");
        return switch (type) {
            case "directory" -> directoryStore(block);
            case "s3" -> s3Store(block);
            default -> throw new IllegalArgumentException("field 'type' must be 'directory' or 's3'");
        };
    }

    private static Store directoryStore(JsonObject block) {
        block.requireKnownKeys(DIRECTORY_STORE);
        return new Store(StoreType.DIRECTORY, path(block.requireString("directory"), "directory"), null, null, null,
                null, null, null);
    }

    private static Store s3Store(JsonObject block) {
        block.requireKnownKeys(S3_STORE);
        URI endpoint = block.has("endpoint") ? endpoint(block.requireString("endpoint")) : null;
        Path caBundle = block.has("ca_bundle") ? path(block.requireString("ca_bundle"), "ca_bundle") : null;
        Store store = new Store(StoreType.S3, null, block.requireString("bucket"), block.stringOr("prefix", ""),
                block.requireString("region"), endpoint,
                block.has("path_style") ? block.boolOr("path_style", false) : null, caBundle);
        ShippingStores.s3Config(store, DEFAULT_REQUEST_TIMEOUT);
        return store;
    }

    private static URI endpoint(String text) {
        try {
            URI endpoint = new URI(text);
            if (endpoint.getScheme() == null || endpoint.getHost() == null) {
                throw new IllegalArgumentException("field 'endpoint' must be an absolute URL with a host");
            }
            if (!endpoint.getScheme().equals("http") && !endpoint.getScheme().equals("https")) {
                throw new IllegalArgumentException("field 'endpoint' must use http or https");
            }
            return endpoint;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("field 'endpoint' is not a valid URL");
        }
    }

    private static CredentialSource credentials(JsonObject root, Store store) {
        if (store.type() == StoreType.DIRECTORY) {
            if (root.has("credentials")) {
                throw new IllegalArgumentException("field 'credentials' is not valid for a directory store");
            }
            return null;
        }
        JsonObject block = root.requireObject("credentials");
        String source = block.requireString("source");
        return switch (source) {
            case "static" -> staticCredentials(block);
            case "environment" -> {
                block.requireKnownKeys(ENVIRONMENT_CREDENTIALS);
                yield new CredentialSource(CredentialKind.ENVIRONMENT, null, null, null, null, null);
            }
            case "file" -> fileCredentials(block);
            default -> throw new IllegalArgumentException("field 'source' must be 'static', 'environment' or 'file'");
        };
    }

    private static CredentialSource staticCredentials(JsonObject block) {
        block.requireKnownKeys(STATIC_CREDENTIALS);
        String token = block.has("session_token") ? block.requireString("session_token") : null;
        return new CredentialSource(CredentialKind.STATIC, nonEmpty(block, "access_key_id"),
                nonEmpty(block, "secret_access_key"), token, null, null);
    }

    private static CredentialSource fileCredentials(JsonObject block) {
        block.requireKnownKeys(FILE_CREDENTIALS);
        return new CredentialSource(CredentialKind.FILE, null, null, null, path(block.requireString("path"), "path"),
                block.stringOr("profile", DEFAULT_PROFILE));
    }

    private static Signing signing(JsonObject block) {
        block.requireKnownKeys(SIGNING);
        long keyId = block.requireLong("key_id");
        if (keyId < 0 || keyId > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("field 'key_id' must be between 0 and " + Integer.MAX_VALUE);
        }
        Path publicKey = block.has("public_key_file") ? path(block.requireString("public_key_file"),
                "public_key_file") : null;
        return new Signing(path(block.requireString("key_file"), "key_file"), (int) keyId, publicKey);
    }

    private static ShipSettings ship(JsonObject block) {
        ShipSettings settings = ShipSettings.defaults();
        if (block.has("interval_ms")) {
            settings = settings.withInterval(Duration.ofMillis(bounded(block, "interval_ms", MAX_MILLIS)));
        }
        if (block.has("max_object_bytes")) {
            settings = settings.withMaxObjectBytes(Math.toIntExact(bounded(block, "max_object_bytes", Integer.MAX_VALUE)));
        }
        if (block.has("backlog_cap_bytes")) {
            settings = settings.withBacklogCapBytes(bounded(block, "backlog_cap_bytes", MAX_BYTES));
        }
        if (block.has("retention_days")) {
            settings = settings.withRetention(Duration.ofDays(bounded(block, "retention_days", MAX_DAYS)));
        }
        return settings;
    }

    private static Duration requestTimeout(JsonObject block) {
        return block.has("request_timeout_ms") ? Duration.ofMillis(bounded(block, "request_timeout_ms", MAX_MILLIS))
                : DEFAULT_REQUEST_TIMEOUT;
    }

    private static Iceberg iceberg(JsonObject block) {
        block.requireKnownKeys(ICEBERG);
        Duration interval = block.has("commit_interval_s")
                ? Duration.ofSeconds(bounded(block, "commit_interval_s", MAX_SECONDS)) : DEFAULT_COMMIT_INTERVAL;
        Duration retention = block.has("table_retention_days")
                ? Duration.ofDays(bounded(block, "table_retention_days", MAX_DAYS)) : DEFAULT_TABLE_RETENTION;
        return new Iceberg(block.boolOr("enabled", false), interval, retention);
    }

    private static long bounded(JsonObject block, String key, long maximum) {
        long value = block.requireLong(key);
        if (value < 1 || value > maximum) {
            throw new IllegalArgumentException("field '" + key + "' must be between 1 and " + maximum);
        }
        return value;
    }

    private static String nonEmpty(JsonObject block, String key) {
        String value = block.requireString(key);
        if (value.isEmpty()) {
            throw new IllegalArgumentException("field '" + key + "' must not be empty");
        }
        return value;
    }

    private static Path path(String text, String field) {
        if (text.isEmpty()) {
            throw new IllegalArgumentException("field '" + field + "' must not be empty");
        }
        try {
            return Path.of(text);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("field '" + field + "' is not a valid path");
        }
    }
}
