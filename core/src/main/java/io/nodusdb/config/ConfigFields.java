package io.nodusdb.config;

import io.nodusdb.json.JsonObject;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

public final class ConfigFields {

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    public static final long MAX_MILLIS = 3_600_000L;
    public static final long MAX_SECONDS = 86_400L;
    public static final long MAX_DAYS = 36_500L;
    public static final long MAX_BYTES = 1L << 50;
    public static final long HISTORY_LIMIT_OF_GRAPH = -1;

    private static final int MEGABYTE_SHIFT = 20;
    private static final long MAX_MEGABYTES = MAX_BYTES >> MEGABYTE_SHIFT;
    private static final Set<String> HISTORY = Set.of("max_memory_mb");

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

    public static long historyMemoryBytes(JsonObject root) {
        JsonObject block = root.objectOr("history", JsonObject.EMPTY);
        block.requireKnownKeys(HISTORY);
        return block.has("max_memory_mb") ? bounded(block, "max_memory_mb", MAX_MEGABYTES) << MEGABYTE_SHIFT
                : HISTORY_LIMIT_OF_GRAPH;
    }

    public static long historyPoolLimit(long configuredBytes, long graphLimitBytes) {
        return configuredBytes == HISTORY_LIMIT_OF_GRAPH ? graphLimitBytes : configuredBytes;
    }
}
