package io.nodusdb.config;

import io.nodusdb.config.StoreConfig.CredentialKind;
import io.nodusdb.config.StoreConfig.CredentialSource;
import io.nodusdb.config.StoreConfig.Store;
import io.nodusdb.config.StoreConfig.StoreType;
import io.nodusdb.json.JsonObject;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Set;

import static io.nodusdb.config.ConfigFields.DEFAULT_REQUEST_TIMEOUT;
import static io.nodusdb.config.ConfigFields.nonEmpty;
import static io.nodusdb.config.ConfigFields.path;

public final class StoreConfigParser {

    private static final Set<String> DIRECTORY_STORE = Set.of("type", "directory");
    private static final Set<String> S3_STORE = Set.of("type", "bucket", "prefix", "region", "endpoint", "path_style",
            "ca_bundle");
    private static final Set<String> STATIC_CREDENTIALS = Set.of("source", "access_key_id", "secret_access_key",
            "session_token");
    private static final Set<String> ENVIRONMENT_CREDENTIALS = Set.of("source");
    private static final Set<String> FILE_CREDENTIALS = Set.of("source", "path", "profile");
    private static final String DEFAULT_PROFILE = "default";

    private StoreConfigParser() {
    }

    public static StoreConfig parse(JsonObject root) {
        Store store = store(root.requireObject("store"));
        return new StoreConfig(store, credentials(root, store), DEFAULT_REQUEST_TIMEOUT);
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
        StoreFactory.s3Config(store, DEFAULT_REQUEST_TIMEOUT);
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
}
