package io.nodusdb.replica;

import io.nodusdb.config.StoreConfig;
import io.nodusdb.config.StoreConfigParser;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

import static io.nodusdb.config.ConfigFields.MAX_DAYS;
import static io.nodusdb.config.ConfigFields.MAX_MILLIS;
import static io.nodusdb.config.ConfigFields.between;
import static io.nodusdb.config.ConfigFields.bounded;
import static io.nodusdb.config.ConfigFields.path;
import static io.nodusdb.config.ConfigFields.requestTimeout;

public record FollowerConfig(StoreConfig storage, Trust trust, Follow follow) {

    public record Trust(int keyId, Path publicKeyFile) {
    }

    public record Follow(Duration pollInterval, Duration readWait, Duration maxStaleness, int downloadParallelism,
                         Path stateDirectory) {

        public boolean hasStalenessBound() {
            return maxStaleness != null;
        }

        public boolean persistsRollbackMemory() {
            return stateDirectory != null;
        }
    }

    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(100);
    public static final Duration DEFAULT_READ_WAIT = Duration.ofSeconds(1);
    public static final int DEFAULT_DOWNLOAD_PARALLELISM = 4;

    private static final Set<String> TOP_LEVEL = Set.of("store", "credentials", "trust", "follow");
    private static final Set<String> TRUST = Set.of("key_id", "public_key_file");
    private static final Set<String> FOLLOW = Set.of("poll_interval_ms", "read_wait_ms", "max_staleness_ms",
            "download_parallelism", "request_timeout_ms", "state_directory");
    private static final long MIN_POLL_MILLIS = 10;
    private static final long MAX_PARALLELISM = 16;
    private static final long MAX_STALENESS_MILLIS = MAX_DAYS * 86_400_000L;

    public static FollowerConfig parse(String document) {
        try {
            JsonObject root = JsonParser.parseObject(document.getBytes(StandardCharsets.UTF_8));
            root.requireKnownKeys(TOP_LEVEL);
            StoreConfig storage = StoreConfigParser.parse(root);
            Trust trust = trust(root.requireObject("trust"));
            JsonObject followBlock = root.objectOr("follow", JsonObject.EMPTY);
            followBlock.requireKnownKeys(FOLLOW);
            return new FollowerConfig(storage.withRequestTimeout(requestTimeout(followBlock)), trust,
                    follow(followBlock));
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("invalid follower configuration: " + invalid.getMessage());
        }
    }

    private static Trust trust(JsonObject block) {
        block.requireKnownKeys(TRUST);
        long keyId = block.requireLong("key_id");
        if (keyId < 0 || keyId > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("field 'key_id' must be between 0 and " + Integer.MAX_VALUE);
        }
        return new Trust((int) keyId, path(block.requireString("public_key_file"), "public_key_file"));
    }

    private static Follow follow(JsonObject block) {
        Duration poll = block.has("poll_interval_ms")
                ? Duration.ofMillis(between(block, "poll_interval_ms", MIN_POLL_MILLIS, MAX_MILLIS))
                : DEFAULT_POLL_INTERVAL;
        Duration readWait = block.has("read_wait_ms")
                ? Duration.ofMillis(bounded(block, "read_wait_ms", MAX_MILLIS)) : DEFAULT_READ_WAIT;
        Duration staleness = block.has("max_staleness_ms")
                ? Duration.ofMillis(bounded(block, "max_staleness_ms", MAX_STALENESS_MILLIS)) : null;
        int parallelism = block.has("download_parallelism")
                ? (int) bounded(block, "download_parallelism", MAX_PARALLELISM) : DEFAULT_DOWNLOAD_PARALLELISM;
        Path state = block.has("state_directory") ? path(block.requireString("state_directory"), "state_directory")
                : null;
        return new Follow(poll, readWait, staleness, parallelism, state);
    }
}
