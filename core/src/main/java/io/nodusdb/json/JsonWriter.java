package io.nodusdb.json;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class JsonWriter {

    private enum Scope {
        ROOT, OBJECT, ARRAY
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final StringBuilder out = new StringBuilder();
    private final Scope[] scopes = new Scope[JsonParser.MAX_DEPTH + 2];
    private final boolean[] hasMembers = new boolean[JsonParser.MAX_DEPTH + 2];
    private int depth;
    private boolean expectingValue;
    private boolean rootWritten;

    public JsonWriter() {
        scopes[0] = Scope.ROOT;
    }

    public JsonWriter beginObject() {
        beforeValue();
        push(Scope.OBJECT);
        out.append('{');
        return this;
    }

    public JsonWriter endObject() {
        end(Scope.OBJECT);
        out.append('}');
        return this;
    }

    public JsonWriter beginArray() {
        beforeValue();
        push(Scope.ARRAY);
        out.append('[');
        return this;
    }

    public JsonWriter endArray() {
        end(Scope.ARRAY);
        out.append(']');
        return this;
    }

    public JsonWriter name(String name) {
        if (scopes[depth] != Scope.OBJECT || expectingValue) {
            throw new IllegalStateException("a field name is not allowed here");
        }
        if (hasMembers[depth]) {
            out.append(',');
        }
        hasMembers[depth] = true;
        quote(name);
        out.append(':');
        expectingValue = true;
        return this;
    }

    public JsonWriter value(String value) {
        beforeValue();
        quote(value);
        return this;
    }

    public JsonWriter value(long value) {
        beforeValue();
        out.append(value);
        return this;
    }

    public JsonWriter value(double value) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("JSON cannot hold a non-finite number");
        }
        beforeValue();
        out.append(value);
        return this;
    }

    public JsonWriter value(boolean value) {
        beforeValue();
        out.append(value);
        return this;
    }

    public JsonWriter nullValue() {
        beforeValue();
        out.append("null");
        return this;
    }

    public JsonWriter value(JsonValue value) {
        switch (value) {
            case JsonObject object -> {
                beginObject();
                for (Map.Entry<String, JsonValue> member : object.members().entrySet()) {
                    name(member.getKey());
                    value(member.getValue());
                }
                endObject();
            }
            case JsonArray array -> {
                beginArray();
                for (JsonValue item : array.items()) {
                    value(item);
                }
                endArray();
            }
            case JsonString string -> value(string.value());
            case JsonNumber number -> {
                beforeValue();
                out.append(number.text());
            }
            case JsonBoolean bool -> value(bool.value());
            case JsonNull ignored -> nullValue();
        }
        return this;
    }

    public byte[] toBytes() {
        return toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        if (depth != 0 || !rootWritten) {
            throw new IllegalStateException("the document is not complete");
        }
        return out.toString();
    }

    private void beforeValue() {
        switch (scopes[depth]) {
            case ROOT -> {
                if (rootWritten) {
                    throw new IllegalStateException("a document holds one value");
                }
                rootWritten = true;
            }
            case OBJECT -> {
                if (!expectingValue) {
                    throw new IllegalStateException("a value needs a field name first");
                }
                expectingValue = false;
            }
            case ARRAY -> {
                if (hasMembers[depth]) {
                    out.append(',');
                }
                hasMembers[depth] = true;
            }
        }
    }

    private void push(Scope scope) {
        if (depth + 1 > JsonParser.MAX_DEPTH) {
            throw new IllegalStateException("nesting is deeper than " + JsonParser.MAX_DEPTH);
        }
        depth++;
        scopes[depth] = scope;
        hasMembers[depth] = false;
    }

    private void end(Scope scope) {
        if (scopes[depth] != scope || expectingValue) {
            throw new IllegalStateException("the container cannot be closed here");
        }
        depth--;
    }

    private void quote(String text) {
        out.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append("\\u00").append(HEX[c >> 4]).append(HEX[c & 0xF]);
                    } else if (Character.isHighSurrogate(c)) {
                        if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(i + 1))) {
                            throw new IllegalArgumentException("a string holds an unpaired surrogate");
                        }
                        out.append(c).append(text.charAt(++i));
                    } else if (Character.isLowSurrogate(c)) {
                        throw new IllegalArgumentException("a string holds an unpaired surrogate");
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
