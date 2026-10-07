package io.nodusdb.json;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonRoundTripOracleTest {

    private static final int DOCUMENTS_PER_SEED = 2_000;
    private static final int MAX_TREE_DEPTH = 8;
    private static final String ALPHABET =
            "abcXYZ019 _-./\"\\\b\f\n\r\t\u0000\u001f\u007fé中 😀";

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void aWrittenTreeParsesBackToTheSameTree(long seed) {
        Random random = new Random(seed);
        for (int document = 0; document < DOCUMENTS_PER_SEED; document++) {
            JsonValue tree = randomValue(random, 0);
            String text = new JsonWriter().value(tree).toString();

            JsonValue parsed = JsonParser.parse(text);

            assertEquals(tree, parsed, "seed=" + seed + " document=" + document + " text=" + text);
            assertEquals(text, new JsonWriter().value(parsed).toString(),
                    "seed=" + seed + " document=" + document + " is not a fixed point");
        }
    }

    private static JsonValue randomValue(Random random, int depth) {
        int kind = random.nextInt(depth >= MAX_TREE_DEPTH ? 5 : 7);
        return switch (kind) {
            case 0 -> JsonNull.INSTANCE;
            case 1 -> JsonBoolean.of(random.nextBoolean());
            case 2 -> JsonNumber.of(randomLong(random));
            case 3 -> new JsonNumber(randomFraction(random));
            case 4 -> new JsonString(randomText(random));
            case 5 -> randomArray(random, depth);
            default -> randomObject(random, depth);
        };
    }

    private static JsonArray randomArray(Random random, int depth) {
        List<JsonValue> items = new ArrayList<>();
        int size = random.nextInt(5);
        for (int i = 0; i < size; i++) {
            items.add(randomValue(random, depth + 1));
        }
        return new JsonArray(items);
    }

    private static JsonObject randomObject(Random random, int depth) {
        Map<String, JsonValue> members = new LinkedHashMap<>();
        int size = random.nextInt(5);
        for (int i = 0; i < size; i++) {
            members.put(randomText(random), randomValue(random, depth + 1));
        }
        return new JsonObject(members);
    }

    private static long randomLong(Random random) {
        return switch (random.nextInt(4)) {
            case 0 -> Long.MIN_VALUE + random.nextInt(3);
            case 1 -> Long.MAX_VALUE - random.nextInt(3);
            case 2 -> random.nextInt(2001) - 1000;
            default -> random.nextLong();
        };
    }

    private static String randomFraction(Random random) {
        return (random.nextBoolean() ? "-" : "") + random.nextInt(1000) + "." + random.nextInt(1000)
                + (random.nextBoolean() ? "e" + (random.nextInt(41) - 20) : "");
    }

    private static String randomText(Random random) {
        StringBuilder text = new StringBuilder();
        int length = random.nextInt(12);
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(ALPHABET.length());
            char c = ALPHABET.charAt(index);
            if (Character.isHighSurrogate(c)) {
                text.append(c).append(ALPHABET.charAt(index + 1));
            } else if (!Character.isLowSurrogate(c)) {
                text.append(c);
            }
        }
        return text.toString();
    }
}
