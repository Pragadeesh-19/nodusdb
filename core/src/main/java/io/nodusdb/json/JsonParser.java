package io.nodusdb.json;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JsonParser {

    public static final int MAX_DEPTH = 64;
    public static final int MAX_DOCUMENT_BYTES = 32 << 20;

    private static final int MAX_NUMBER_CHARS = 64;
    private static final int UNICODE_ESCAPE_DIGITS = 4;
    private static final int HEX_RADIX = 16;

    private final String text;
    private int position;

    private JsonParser(String text) {
        this.text = text;
    }

    public static JsonValue parse(byte[] utf8) {
        if (utf8.length > MAX_DOCUMENT_BYTES) {
            throw new JsonException("document is larger than " + MAX_DOCUMENT_BYTES + " bytes");
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return parse(decoder.decode(ByteBuffer.wrap(utf8)).toString());
        } catch (CharacterCodingException e) {
            throw new JsonException("document is not valid UTF-8");
        }
    }

    public static JsonValue parse(String text) {
        if (text.length() > MAX_DOCUMENT_BYTES) {
            throw new JsonException("document is larger than " + MAX_DOCUMENT_BYTES + " characters");
        }
        JsonParser parser = new JsonParser(text);
        parser.skipWhitespace();
        JsonValue value = parser.value(0);
        parser.skipWhitespace();
        if (parser.position != text.length()) {
            throw parser.error("unexpected content after the document");
        }
        return value;
    }

    public static JsonObject parseObject(byte[] utf8) {
        if (parse(utf8) instanceof JsonObject object) {
            return object;
        }
        throw new JsonException("document must be an object");
    }

    public static JsonObject parseObject(String text) {
        if (parse(text) instanceof JsonObject object) {
            return object;
        }
        throw new JsonException("document must be an object");
    }

    private JsonValue value(int depth) {
        if (position >= text.length()) {
            throw error("the document ends early");
        }
        char c = text.charAt(position);
        return switch (c) {
            case '{' -> object(depth + 1);
            case '[' -> array(depth + 1);
            case '"' -> new JsonString(string());
            case 't' -> literal("true", JsonBoolean.TRUE);
            case 'f' -> literal("false", JsonBoolean.FALSE);
            case 'n' -> literal("null", JsonNull.INSTANCE);
            default -> {
                if (c == '-' || isDigit(c)) {
                    yield number();
                }
                throw error("unexpected character");
            }
        };
    }

    private JsonObject object(int depth) {
        requireDepth(depth);
        position++;
        Map<String, JsonValue> members = new LinkedHashMap<>();
        skipWhitespace();
        if (peek() == '}') {
            position++;
            return new JsonObject(members);
        }
        while (true) {
            skipWhitespace();
            if (peek() != '"') {
                throw error("a field name in quotes is expected");
            }
            String name = string();
            skipWhitespace();
            expect(':');
            skipWhitespace();
            if (members.put(name, value(depth)) != null) {
                throw error("duplicate field '" + name + "'");
            }
            skipWhitespace();
            char next = take();
            if (next == '}') {
                return new JsonObject(members);
            }
            if (next != ',') {
                position--;
                throw error("a comma or a closing brace is expected");
            }
        }
    }

    private JsonArray array(int depth) {
        requireDepth(depth);
        position++;
        List<JsonValue> items = new ArrayList<>();
        skipWhitespace();
        if (peek() == ']') {
            position++;
            return new JsonArray(items);
        }
        while (true) {
            skipWhitespace();
            items.add(value(depth));
            skipWhitespace();
            char next = take();
            if (next == ']') {
                return new JsonArray(items);
            }
            if (next != ',') {
                position--;
                throw error("a comma or a closing bracket is expected");
            }
        }
    }

    private String string() {
        position++;
        StringBuilder out = new StringBuilder();
        while (true) {
            if (position >= text.length()) {
                throw error("a string is not closed");
            }
            char c = text.charAt(position++);
            if (c == '"') {
                return out.toString();
            }
            if (c < 0x20) {
                position--;
                throw error("a control character inside a string");
            }
            if (c == '\\') {
                out.append(escape());
            } else if (Character.isHighSurrogate(c)) {
                requireLowSurrogateAt(position);
                out.append(c).append(text.charAt(position++));
            } else if (Character.isLowSurrogate(c)) {
                position--;
                throw error("an unpaired surrogate");
            } else {
                out.append(c);
            }
        }
    }

    private String escape() {
        if (position >= text.length()) {
            throw error("a string is not closed");
        }
        char c = text.charAt(position++);
        return switch (c) {
            case '"' -> "\"";
            case '\\' -> "\\";
            case '/' -> "/";
            case 'b' -> "\b";
            case 'f' -> "\f";
            case 'n' -> "\n";
            case 'r' -> "\r";
            case 't' -> "\t";
            case 'u' -> unicodeEscape();
            default -> {
                position--;
                throw error("an invalid escape");
            }
        };
    }

    private String unicodeEscape() {
        char first = hexQuad();
        if (Character.isLowSurrogate(first)) {
            throw error("an unpaired surrogate");
        }
        if (!Character.isHighSurrogate(first)) {
            return String.valueOf(first);
        }
        if (!text.startsWith("\\u", position)) {
            throw error("an unpaired surrogate");
        }
        position += 2;
        char second = hexQuad();
        if (!Character.isLowSurrogate(second)) {
            throw error("an unpaired surrogate");
        }
        return new String(new char[]{first, second});
    }

    private char hexQuad() {
        if (position + UNICODE_ESCAPE_DIGITS > text.length()) {
            throw error("a unicode escape needs four hex digits");
        }
        int value = 0;
        for (int i = 0; i < UNICODE_ESCAPE_DIGITS; i++) {
            int digit = Character.digit(text.charAt(position), HEX_RADIX);
            if (digit < 0 || text.charAt(position) > 'f') {
                throw error("a unicode escape needs four hex digits");
            }
            value = value * HEX_RADIX + digit;
            position++;
        }
        return (char) value;
    }

    private JsonNumber number() {
        int start = position;
        if (peek() == '-') {
            position++;
        }
        if (peek() == '0') {
            position++;
        } else if (isNonZeroDigit(peek())) {
            skipDigits();
        } else {
            throw error("a digit is expected");
        }
        if (peek() == '.') {
            position++;
            requireDigits();
        }
        if (peek() == 'e' || peek() == 'E') {
            position++;
            if (peek() == '+' || peek() == '-') {
                position++;
            }
            requireDigits();
        }
        if (position - start > MAX_NUMBER_CHARS) {
            throw error("a number is longer than " + MAX_NUMBER_CHARS + " characters");
        }
        return new JsonNumber(text.substring(start, position));
    }

    private void requireDigits() {
        if (!isDigit(peek())) {
            throw error("a digit is expected");
        }
        skipDigits();
    }

    private void skipDigits() {
        while (isDigit(peek())) {
            position++;
        }
    }

    private JsonValue literal(String word, JsonValue value) {
        if (!text.startsWith(word, position)) {
            throw error("an unknown literal");
        }
        position += word.length();
        return value;
    }

    private void requireLowSurrogateAt(int index) {
        if (index >= text.length() || !Character.isLowSurrogate(text.charAt(index))) {
            throw error("an unpaired surrogate");
        }
    }

    private void requireDepth(int depth) {
        if (depth > MAX_DEPTH) {
            throw error("nesting is deeper than " + MAX_DEPTH);
        }
    }

    private void skipWhitespace() {
        while (position < text.length()) {
            char c = text.charAt(position);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return;
            }
            position++;
        }
    }

    private void expect(char expected) {
        if (position >= text.length() || text.charAt(position) != expected) {
            throw error("'" + expected + "' is expected");
        }
        position++;
    }

    private char peek() {
        return position < text.length() ? text.charAt(position) : '\0';
    }

    private char take() {
        if (position >= text.length()) {
            throw error("the document ends early");
        }
        return text.charAt(position++);
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isNonZeroDigit(char c) {
        return c >= '1' && c <= '9';
    }

    private JsonException error(String what) {
        return new JsonException(what + " at offset " + position);
    }
}
