package io.nodusdb.json;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonObjectTest {

    private static final JsonObject SAMPLE = JsonParser.parseObject("""
            {"name":"nodus","count":42,"big":9223372036854775808,"ratio":1.5,"on":true,"off":false,
             "nested":{"inner":"x"},"list":[1,2],"nothing":null}""");

    @Test
    void presentFieldsAreReadWithTheirType() {
        assertEquals("nodus", SAMPLE.requireString("name"));
        assertEquals(42, SAMPLE.requireLong("count"));
        assertTrue(SAMPLE.boolOr("on", false));
        assertFalse(SAMPLE.boolOr("off", true));
        assertEquals("x", SAMPLE.requireObject("nested").requireString("inner"));
        assertEquals(2, SAMPLE.requireArray("list").size());
    }

    @Test
    void absentFieldsFallBackWhenAnOrdinaryDefaultIsAllowed() {
        assertEquals("fallback", SAMPLE.stringOr("missing", "fallback"));
        assertEquals(7, SAMPLE.longOr("missing", 7));
        assertTrue(SAMPLE.boolOr("missing", true));
        assertSame(JsonObject.EMPTY, SAMPLE.objectOr("missing", JsonObject.EMPTY));
    }

    @Test
    void aPresentFieldIsNeverReplacedByTheFallback() {
        assertEquals("nodus", SAMPLE.stringOr("name", "fallback"));
        assertEquals(42, SAMPLE.longOr("count", 7));
    }

    @Test
    void aMissingRequiredFieldIsNamed() {
        JsonException failure = assertThrows(JsonException.class, () -> SAMPLE.requireString("bucket"));

        assertEquals("field 'bucket' is required", failure.getMessage());
    }

    @Test
    void aWrongTypeIsNamedAndDescribed() {
        assertEquals("field 'count' must be a string",
                assertThrows(JsonException.class, () -> SAMPLE.requireString("count")).getMessage());
        assertEquals("field 'name' must be an integer in the 64-bit range",
                assertThrows(JsonException.class, () -> SAMPLE.requireLong("name")).getMessage());
        assertEquals("field 'name' must be true or false",
                assertThrows(JsonException.class, () -> SAMPLE.boolOr("name", false)).getMessage());
        assertEquals("field 'name' must be an object",
                assertThrows(JsonException.class, () -> SAMPLE.requireObject("name")).getMessage());
        assertEquals("field 'name' must be an array",
                assertThrows(JsonException.class, () -> SAMPLE.requireArray("name")).getMessage());
    }

    @Test
    void anIntegerFieldRejectsFractionsAndOverflow() {
        assertThrows(JsonException.class, () -> SAMPLE.requireLong("ratio"));
        assertThrows(JsonException.class, () -> SAMPLE.requireLong("big"));
    }

    @Test
    void aNullFieldIsNotAnyOtherType() {
        assertTrue(SAMPLE.has("nothing"));
        assertThrows(JsonException.class, () -> SAMPLE.requireString("nothing"));
        assertThrows(JsonException.class, () -> SAMPLE.stringOr("nothing", "fallback"));
    }

    @Test
    void unknownKeysAreRefusedByName() {
        Set<String> allowed = Set.of("name", "count", "big", "ratio", "on", "off", "nested", "list");

        JsonException failure = assertThrows(JsonException.class, () -> SAMPLE.requireKnownKeys(allowed));

        assertEquals("unknown field 'nothing'", failure.getMessage());
        SAMPLE.requireKnownKeys(Set.of("name", "count", "big", "ratio", "on", "off", "nested", "list", "nothing"));
    }

    @Test
    void anErrorNeverContainsTheValueItRejected() {
        JsonObject object = JsonParser.parseObject("{\"password\":12345,\"token\":\"s3cr3t\"}");

        assertFalse(assertThrows(JsonException.class, () -> object.requireString("password"))
                .getMessage().contains("12345"));
        assertFalse(assertThrows(JsonException.class, () -> object.requireLong("token"))
                .getMessage().contains("s3cr3t"));
        assertFalse(assertThrows(JsonException.class, () -> object.requireObject("token"))
                .getMessage().contains("s3cr3t"));
    }

    @Test
    void theMembersCannotBeChangedAfterConstruction() {
        Map<String, JsonValue> source = new LinkedHashMap<>();
        source.put("a", new JsonString("1"));
        JsonObject object = new JsonObject(source);

        source.put("b", new JsonString("2"));

        assertFalse(object.has("b"));
        assertThrows(UnsupportedOperationException.class, () -> object.members().put("c", JsonNull.INSTANCE));
    }
}
