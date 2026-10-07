package io.nodusdb.json;

import java.util.Objects;

public record JsonNumber(String text) implements JsonValue {

    public JsonNumber {
        Objects.requireNonNull(text, "text");
    }

    public static JsonNumber of(long value) {
        return new JsonNumber(Long.toString(value));
    }

    public boolean isIntegral() {
        return text.indexOf('.') < 0 && text.indexOf('e') < 0 && text.indexOf('E') < 0;
    }

    public long asLong() {
        if (!isIntegral()) {
            throw new JsonException("number is not an integer");
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new JsonException("integer is outside the 64-bit range");
        }
    }

    public double asDouble() {
        double value = Double.parseDouble(text);
        if (!Double.isFinite(value)) {
            throw new JsonException("number is outside the double range");
        }
        return value;
    }
}
