package io.nodusdb.json;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonParserTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "[]", "{}", "[1]", "[-0]", "[0]", "[0.5e+10]", "[1E22]", "[1e-2]", "[-1.5E+3]",
            "[\"\\u0060\\u012a\\u12AB\"]", "[\"\\uD801\\udc37\"]", "[\" \"]", "[\"\"]", "{\"\":1}",
            "{\"a\":null}", "[true,false,null]", " \t\r\n[ 1 , 2 ]\n", "{ \"a\" : { \"b\" : [ ] } }",
            "\"text\"", "12", "true", "null", "[\"\\/\\\\\\\"\\b\\f\\n\\r\\t\"]", "[\"\u00e9\u4e2d\"]",
            "[\"\ud83d\ude00\"]", "{\"a\":1,\"b\":2,\"c\":3}"
    })
    void aValidDocumentParses(String text) {
        assertEquals(JsonParser.parse(text), JsonParser.parse(text.getBytes(StandardCharsets.UTF_8)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", " ", "[", "]", "{", "}", "[1,]", "[,1]", "[1 2]", "[1,,2]", "{\"a\":1,}", "{,\"a\":1}",
            "{a:1}", "{'a':1}", "{\"a\"}", "{\"a\":}", "{\"a\" 1}", "{\"a\":1 \"b\":2}", "{\"a\":1}x", "[]]",
            "{\"a\":1}{\"b\":2}", "[01]", "[1.]", "[.5]", "[-]", "[+1]", "[1e]", "[1e+]", "[--1]", "[0x10]",
            "[NaN]", "[Infinity]", "[-Infinity]", "[tru]", "[nul]", "[fals]", "[truex]", "[True]", "[NULL]",
            "[\"\\x\"]", "[\"\\u12\"]", "[\"\\u00zz\"]", "[\"\\ud800\"]", "[\"\\udc00\"]", "[\"\\ud800\\u0041\"]",
            "[\"\\ud800\\ud800\"]", "[\"a\tb\"]", "[\"a\nb\"]", "[\"a\u0000b\"]", "[\"abc]", "['a']",
            "/*c*/[]", "[1]//c", "\ufeff[]", "[1,2", "{\"a\":1", "{\"a\":1,\"a\":2}", "[\"\\u00e9\\\"]",
            "[\"\ud800\"]", "[\"\udc00\"]", "[\"\ud800a\"]", "\\", "\"", "[1e5.5]", "[1.5.5]", "[1-2]"
    })
    void anInvalidDocumentIsRejected(String text) {
        assertThrows(JsonException.class, () -> JsonParser.parse(text));
    }

    @Test
    void aNumberKeepsItsExactText() {
        JsonArray array = (JsonArray) JsonParser.parse("[12345678901234567890, 1.50, -0, 1e2]");

        assertEquals("12345678901234567890", ((JsonNumber) array.get(0)).text());
        assertEquals("1.50", ((JsonNumber) array.get(1)).text());
        assertEquals("-0", ((JsonNumber) array.get(2)).text());
        assertEquals("1e2", ((JsonNumber) array.get(3)).text());
    }

    @Test
    void anIntegerOutsideTheLongRangeIsRefusedWhenReadAsALong() {
        JsonNumber big = (JsonNumber) ((JsonArray) JsonParser.parse("[9223372036854775808]")).get(0);

        assertTrue(big.isIntegral());
        assertThrows(JsonException.class, big::asLong);
        assertEquals(9.223372036854775808E18, big.asDouble());
    }

    @Test
    void theLongExtremesAreRepresentable() {
        JsonArray array = (JsonArray) JsonParser.parse("[9223372036854775807, -9223372036854775808]");

        assertEquals(Long.MAX_VALUE, ((JsonNumber) array.get(0)).asLong());
        assertEquals(Long.MIN_VALUE, ((JsonNumber) array.get(1)).asLong());
    }

    @Test
    void aFractionOrExponentIsNotAnInteger() {
        for (String text : new String[]{"[1.5]", "[1e2]", "[1E2]"}) {
            JsonNumber number = (JsonNumber) ((JsonArray) JsonParser.parse(text)).get(0);
            assertFalse(number.isIntegral());
            assertThrows(JsonException.class, number::asLong);
        }
    }

    @Test
    void aNumberLongerThanTheLimitIsRejected() {
        String digits = "1".repeat(65);

        assertThrows(JsonException.class, () -> JsonParser.parse("[" + digits + "]"));
        assertEquals(new JsonNumber("1".repeat(64)), ((JsonArray) JsonParser.parse("[" + "1".repeat(64) + "]")).get(0));
    }

    @Test
    void anOverflowingDoubleIsRefusedWhenReadAsADouble() {
        JsonNumber huge = (JsonNumber) ((JsonArray) JsonParser.parse("[1e999]")).get(0);

        assertThrows(JsonException.class, huge::asDouble);
    }

    @Test
    void escapesAreDecoded() {
        JsonString text = (JsonString) ((JsonArray) JsonParser.parse("[\"a\\n\\t\\\"\\\\\\/\\u0041\\ud83d\\ude00\"]")).get(0);

        assertEquals("a\n\t\"\\/A\ud83d\ude00", text.value());
    }

    @Test
    void membersKeepTheirOrder() {
        JsonObject object = JsonParser.parseObject("{\"z\":1,\"a\":2,\"m\":3}");

        assertEquals("[z, a, m]", object.members().keySet().toString());
    }

    @Test
    void aTopLevelScalarIsNotAnObject() {
        assertThrows(JsonException.class, () -> JsonParser.parseObject("[]"));
        assertThrows(JsonException.class, () -> JsonParser.parseObject("1"));
        assertThrows(JsonException.class, () -> JsonParser.parseObject("null".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void nestingAtTheLimitParsesAndOnePastItDoesNot() {
        String atLimit = "[".repeat(JsonParser.MAX_DEPTH) + "]".repeat(JsonParser.MAX_DEPTH);
        String pastLimit = "[".repeat(JsonParser.MAX_DEPTH + 1) + "]".repeat(JsonParser.MAX_DEPTH + 1);

        assertInstanceOf(JsonArray.class, JsonParser.parse(atLimit));
        assertThrows(JsonException.class, () -> JsonParser.parse(pastLimit));
    }

    @Test
    void absurdNestingFailsCleanlyInsteadOfOverflowingTheStack() {
        assertThrows(JsonException.class, () -> JsonParser.parse("[".repeat(200_000)));
        assertThrows(JsonException.class, () -> JsonParser.parse("{\"a\":".repeat(200_000)));
    }

    @Test
    void invalidUtf8IsRejected() {
        byte[][] invalid = {
                {'[', '"', (byte) 0xFF, '"', ']'},
                {'[', '"', (byte) 0xC0, (byte) 0x80, '"', ']'},
                {'[', '"', (byte) 0xE2, (byte) 0x82, '"', ']'},
                {'[', '"', (byte) 0xED, (byte) 0xA0, (byte) 0x80, '"', ']'},
                {'[', '"', (byte) 0x80, '"', ']'},
                {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, '[', ']'},
        };

        for (byte[] bytes : invalid) {
            assertThrows(JsonException.class, () -> JsonParser.parse(bytes));
        }
    }

    @Test
    void validMultiByteUtf8IsAccepted() {
        byte[] bytes = "[\"\u00e9\u20ac\ud83d\ude00\"]".getBytes(StandardCharsets.UTF_8);

        assertEquals("\u00e9\u20ac\ud83d\ude00", ((JsonString) ((JsonArray) JsonParser.parse(bytes)).get(0)).value());
    }

    @Test
    void aDocumentOverTheSizeLimitIsRejectedBeforeParsing() {
        assertThrows(JsonException.class, () -> JsonParser.parse(new byte[JsonParser.MAX_DOCUMENT_BYTES + 1]));
    }

    @Test
    void anErrorNamesTheOffsetOfTheProblem() {
        JsonException failure = assertThrows(JsonException.class, () -> JsonParser.parse("[1, 2 3]"));

        assertTrue(failure.getMessage().endsWith("at offset 6"), failure.getMessage());
    }

    @Test
    void anErrorNeverEchoesTheDocumentContent() {
        String secret = "hunter2-secret";
        String[] broken = {
                "{\"password\":\"" + secret + "\" \"x\":1}",
                "{\"password\":\"" + secret + "\\q\"}",
                "{\"password\":\"" + secret + "",
                "{\"password\":" + secret + "}",
                "[\"" + secret + "\"" + secret + "]",
                "{\"" + secret + "\":1,\"" + secret + "x\" 2}",
        };

        for (String text : broken) {
            JsonException failure = assertThrows(JsonException.class, () -> JsonParser.parse(text), text);
            assertFalse(failure.getMessage().contains(secret), failure.getMessage());
        }
    }

    @Test
    void aDuplicateFieldIsRefusedByName() {
        JsonException failure = assertThrows(JsonException.class,
                () -> JsonParser.parse("{\"store\":1,\"other\":2,\"store\":3}"));

        assertTrue(failure.getMessage().contains("duplicate field 'store'"), failure.getMessage());
    }
}
