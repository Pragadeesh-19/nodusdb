package io.nodusdb.config;

import io.nodusdb.json.JsonObject;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;

public final class ConfigFields {

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    public static final long MAX_MILLIS = 3_600_000L;
    public static final long MAX_SECONDS = 86_400L;
    public static final long MAX_DAYS = 36_500L;
    public static final long MAX_BYTES = 1L << 50;

    private ConfigFields() {
    }

    public static long bounded(JsonObject block, String key, long maximum) {
        return between(block, key, 1, maximum);
    }

    public static long between(JsonObject block, String key, long minimum, long maximum) {
        long value = block.requireLong(key);
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException("field '" + key + "' must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    public static String nonEmpty(JsonObject block, String key) {
        String value = block.requireString(key);
        if (value.isEmpty()) {
            throw new IllegalArgumentException("field '" + key + "' must not be empty");
        }
        return value;
    }

    public static Path path(String text, String field) {
        if (text.isEmpty()) {
            throw new IllegalArgumentException("field '" + field + "' must not be empty");
        }
        try {
            return Path.of(text);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("field '" + field + "' is not a valid path");
        }
    }

    public static Duration requestTimeout(JsonObject block) {
        return block.has("request_timeout_ms") ? Duration.ofMillis(bounded(block, "request_timeout_ms", MAX_MILLIS))
                : DEFAULT_REQUEST_TIMEOUT;
    }
}
