package io.nodusdb.ship;

import io.nodusdb.config.StoreConfig;
import io.nodusdb.config.StoreConfigParser;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.nodusdb.config.ConfigFields.MAX_BYTES;
import static io.nodusdb.config.ConfigFields.MAX_DAYS;
import static io.nodusdb.config.ConfigFields.MAX_MILLIS;
import static io.nodusdb.config.ConfigFields.MAX_SECONDS;
import static io.nodusdb.config.ConfigFields.bounded;
import static io.nodusdb.config.ConfigFields.path;
import static io.nodusdb.config.ConfigFields.requestTimeout;

public record ShippingConfig(StoreConfig storage, Signing signing, ShipSettings ship, Iceberg iceberg) {

    public record Signing(Path keyFile, int keyId, Path publicKeyFile) {
    }

    public record Iceberg(boolean enabled, Duration commitInterval, Duration tableRetention) {
    }

    private static final Set<String> TOP_LEVEL = Set.of("store", "credentials", "signing", "ship", "iceberg");
    private static final Set<String> SIGNING = Set.of("key_file", "key_id", "public_key_file");
    private static final Set<String> SHIP = Set.of("interval_ms", "max_object_bytes", "backlog_cap_bytes",
            "retention_days", "request_timeout_ms");
    private static final Set<String> ICEBERG = Set.of("enabled", "commit_interval_s", "table_retention_days");
    private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(60);
    private static final Duration DEFAULT_TABLE_RETENTION = Duration.ofDays(7);

    public static ShippingConfig parse(String document) {
        try {
            JsonObject root = JsonParser.parseObject(document.getBytes(StandardCharsets.UTF_8));
            root.requireKnownKeys(TOP_LEVEL);
            StoreConfig storage = StoreConfigParser.parse(root);
            Signing signing = signing(root.requireObject("signing"));
            JsonObject shipBlock = root.objectOr("ship", JsonObject.EMPTY);
            shipBlock.requireKnownKeys(SHIP);
            ShipSettings ship = ship(shipBlock);
            return new ShippingConfig(storage.withRequestTimeout(requestTimeout(shipBlock)), signing, ship,
                    iceberg(root.objectOr("iceberg", JsonObject.EMPTY)));
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("invalid shipping configuration: " + invalid.getMessage());
        }
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
            Duration interval = Duration.ofMillis(bounded(block, "interval_ms", MAX_MILLIS));
            settings = named("interval_ms", settings, current -> current.withInterval(interval));
        }
        if (block.has("max_object_bytes")) {
            int bytes = Math.toIntExact(bounded(block, "max_object_bytes", Integer.MAX_VALUE));
            settings = named("max_object_bytes", settings, current -> current.withMaxObjectBytes(bytes));
        }
        if (block.has("backlog_cap_bytes")) {
            long bytes = bounded(block, "backlog_cap_bytes", MAX_BYTES);
            settings = named("backlog_cap_bytes", settings, current -> current.withBacklogCapBytes(bytes));
        }
        if (block.has("retention_days")) {
            Duration retention = Duration.ofDays(bounded(block, "retention_days", MAX_DAYS));
            settings = named("retention_days", settings, current -> current.withRetention(retention));
        }
        return settings;
    }

    private static ShipSettings named(String key, ShipSettings current, UnaryOperator<ShipSettings> change) {
        try {
            return change.apply(current);
        } catch (IllegalArgumentException refused) {
            throw new IllegalArgumentException("field '" + key + "': " + refused.getMessage());
        }
    }

    private static Iceberg iceberg(JsonObject block) {
        block.requireKnownKeys(ICEBERG);
        Duration interval = block.has("commit_interval_s")
                ? Duration.ofSeconds(bounded(block, "commit_interval_s", MAX_SECONDS)) : DEFAULT_COMMIT_INTERVAL;
        Duration retention = block.has("table_retention_days")
                ? Duration.ofDays(bounded(block, "table_retention_days", MAX_DAYS)) : DEFAULT_TABLE_RETENTION;
        return new Iceberg(block.boolOr("enabled", false), interval, retention);
    }
}
