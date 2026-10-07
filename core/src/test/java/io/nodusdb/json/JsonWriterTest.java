package io.nodusdb.json;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonWriterTest {

    @Test
    void anObjectIsWrittenCompactlyInInsertionOrder() {
        String text = new JsonWriter().beginObject()
                .name("z").value(1L)
                .name("a").value("x")
                .name("flag").value(true)
                .name("none").nullValue()
                .name("list").beginArray().value(1L).value(2L).endArray()
                .name("empty").beginObject().endObject()
                .endObject().toString();

        assertEquals("{\"z\":1,\"a\":\"x\",\"flag\":true,\"none\":null,\"list\":[1,2],\"empty\":{}}", text);
    }

    @Test
    void everyControlCharacterIsEscapedAndRoundTrips() {
        StringBuilder all = new StringBuilder();
        for (char c = 0; c < 0x20; c++) {
            all.append(c);
        }
        all.append("\"\\/ \u007f  ");

        String text = new JsonWriter().value(all.toString()).toString();

        assertEquals(all.toString(), ((JsonString) JsonParser.parse(text)).value());
        for (int i = 0; i < text.length(); i++) {
            assertFalse(text.charAt(i) < 0x20, "raw control character at " + i);
        }
    }

    @Test
    void namedEscapesUseTheShortForm() {
        assertEquals("\"\\b\\f\\n\\r\\t\\\"\\\\\"", new JsonWriter().value("\b\f\n\r\t\"\\").toString());
        assertEquals("\"\\u0000\\u001f\"", new JsonWriter().value("\u0000\u001f").toString());
    }

    @Test
    void unicodeIsWrittenAsItselfAndPairsSurvive() {
        String text = "é中😀";

        assertEquals("\"" + text + "\"", new JsonWriter().value(text).toString());
        assertArrayEquals(("\"" + text + "\"").getBytes(StandardCharsets.UTF_8), new JsonWriter().value(text).toBytes());
    }

    @Test
    void anUnpairedSurrogateIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new JsonWriter().value("\ud800"));
        assertThrows(IllegalArgumentException.class, () -> new JsonWriter().value("\udc00"));
        assertThrows(IllegalArgumentException.class, () -> new JsonWriter().value("a\ud800b"));
        assertThrows(IllegalArgumentException.class, () -> new JsonWriter().beginObject().name("\ud800"));
    }

    @Test
    void numbersKeepTheirExtremesAndFormat() {
        assertEquals("[9223372036854775807,-9223372036854775808,0]",
                new JsonWriter().beginArray().value(Long.MAX_VALUE).value(Long.MIN_VALUE).value(0L).endArray().toString());
        assertEquals(1.5, ((JsonNumber) JsonParser.parse(new JsonWriter().value(1.5).toString())).asDouble());
        assertEquals(1.0E-5, ((JsonNumber) JsonParser.parse(new JsonWriter().value(1.0E-5).toString())).asDouble());
        assertEquals(Double.MIN_VALUE,
                ((JsonNumber) JsonParser.parse(new JsonWriter().value(Double.MIN_VALUE).toString())).asDouble());
    }

    @Test
    void aNonFiniteNumberIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new JsonWriter().value(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> new JsonWriter().value(Double.POSITIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> new JsonWriter().value(Double.NEGATIVE_INFINITY));
    }

    @Test
    void aValueInAnObjectNeedsAName() {
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginObject().value(1L));
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginObject().beginArray());
    }

    @Test
    void aNameNeedsAnObjectAndNoPendingName() {
        assertThrows(IllegalStateException.class, () -> new JsonWriter().name("a"));
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginArray().name("a"));
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginObject().name("a").name("b"));
    }

    @Test
    void containersMustCloseInOrderAndWithTheirOwnKind() {
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginObject().endArray());
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginArray().endObject());
        assertThrows(IllegalStateException.class, () -> new JsonWriter().endObject());
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginObject().name("a").endObject());
    }

    @Test
    void aDocumentHoldsOneRootValue() {
        assertThrows(IllegalStateException.class, () -> new JsonWriter().value(1L).value(2L));
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginArray().endArray().beginArray());
    }

    @Test
    void anIncompleteDocumentCannotBeRead() {
        assertThrows(IllegalStateException.class, () -> new JsonWriter().toString());
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginObject().toString());
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginArray().value(1L).toBytes());
    }

    @Test
    void nestingPastTheParserLimitIsRefused() {
        JsonWriter writer = new JsonWriter();
        for (int i = 0; i < JsonParser.MAX_DEPTH; i++) {
            writer.beginArray();
        }

        assertThrows(IllegalStateException.class, writer::beginArray);
    }

    @Test
    void whatTheWriterProducesAtTheLimitTheParserAccepts() {
        JsonWriter writer = new JsonWriter();
        for (int i = 0; i < JsonParser.MAX_DEPTH; i++) {
            writer.beginArray();
        }
        for (int i = 0; i < JsonParser.MAX_DEPTH; i++) {
            writer.endArray();
        }

        JsonParser.parse(writer.toString());
    }

    @Test
    void aParsedTreeIsWrittenBackIdentically() {
        String text = "{\"a\":[1,2.50,-0,true,false,null,\"x\\n\"],\"b\":{\"c\":{}},\"d\":[]}";

        assertEquals(text, new JsonWriter().value(JsonParser.parse(text)).toString());
    }
}
