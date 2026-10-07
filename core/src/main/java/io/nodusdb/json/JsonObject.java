package io.nodusdb.json;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public record JsonObject(Map<String, JsonValue> members) implements JsonValue {

    public static final JsonObject EMPTY = new JsonObject(Map.of());

    public JsonObject {
        members = Collections.unmodifiableMap(new LinkedHashMap<>(members));
    }

    public boolean has(String key) {
        return members.containsKey(key);
    }

    public JsonValue get(String key) {
        return members.get(key);
    }

    public String requireString(String key) {
        return required(key, JsonString.class, "a string").value();
    }

    public String stringOr(String key, String fallback) {
        return has(key) ? requireString(key) : fallback;
    }

    public long requireLong(String key) {
        try {
            return required(key, JsonNumber.class, "an integer").asLong();
        } catch (JsonException e) {
            throw new JsonException("field '" + key + "' must be an integer in the 64-bit range");
        }
    }

    public long longOr(String key, long fallback) {
        return has(key) ? requireLong(key) : fallback;
    }

    public boolean boolOr(String key, boolean fallback) {
        return has(key) ? required(key, JsonBoolean.class, "true or false").value() : fallback;
    }

    public JsonObject requireObject(String key) {
        return required(key, JsonObject.class, "an object");
    }

    public JsonObject objectOr(String key, JsonObject fallback) {
        return has(key) ? requireObject(key) : fallback;
    }

    public JsonArray requireArray(String key) {
        return required(key, JsonArray.class, "an array");
    }

    public void requireKnownKeys(Set<String> allowed) {
        for (String key : members.keySet()) {
            if (!allowed.contains(key)) {
                throw new JsonException("unknown field '" + key + "'");
            }
        }
    }

    private <T extends JsonValue> T required(String key, Class<T> type, String description) {
        JsonValue value = members.get(key);
        if (value == null) {
            throw new JsonException("field '" + key + "' is required");
        }
        if (!type.isInstance(value)) {
            throw new JsonException("field '" + key + "' must be " + description);
        }
        return type.cast(value);
    }
}
