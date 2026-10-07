package io.nodusdb.json;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonFuzzTest {

    private static final int RANDOM_INPUTS = 20_000;
    private static final int MAX_RANDOM_BYTES = 96;
    private static final byte[] PROBES = {
            0, '"', '\\', '{', '}', '[', ']', ',', ':', '-', '+', '.', '0', '9', 'e', 'E', 'u', 't', 'n', ' ',
            '\n', (byte) 0x7f, (byte) 0x80, (byte) 0xC0, (byte) 0xED, (byte) 0xFF
    };
    private static final String[] CORPUS = {
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"b\",\"prefix\":\"p/\",\"path_style\":true},"
                    + "\"ship\":{\"interval_ms\":100,\"backlog_cap_bytes\":1073741824},\"keys\":[1,-2,3.5e2,null]}",
            "[\"\\u00e9\\ud83d\\ude00\\n\\\"\",{\"a\":[[],{}]},-0,1E+2,false]",
            "{\"nested\":{\"a\":{\"b\":{\"c\":[1,[2,[3,[4]]]]}}}}",
    };

    @Test
    void everyTruncationOfAValidDocumentEitherParsesOrFailsCleanly() {
        for (String document : CORPUS) {
            byte[] bytes = document.getBytes(StandardCharsets.UTF_8);
            for (int length = 0; length <= bytes.length; length++) {
                requireCleanOutcome(Arrays.copyOf(bytes, length));
            }
        }
    }

    @Test
    void everySingleByteReplacementEitherParsesOrFailsCleanly() {
        for (String document : CORPUS) {
            byte[] bytes = document.getBytes(StandardCharsets.UTF_8);
            for (int index = 0; index < bytes.length; index++) {
                for (byte probe : PROBES) {
                    byte[] mutated = bytes.clone();
                    mutated[index] = probe;
                    requireCleanOutcome(mutated);
                }
            }
        }
    }

    @Test
    void everySingleByteDeletionEitherParsesOrFailsCleanly() {
        for (String document : CORPUS) {
            byte[] bytes = document.getBytes(StandardCharsets.UTF_8);
            for (int index = 0; index < bytes.length; index++) {
                byte[] shorter = new byte[bytes.length - 1];
                System.arraycopy(bytes, 0, shorter, 0, index);
                System.arraycopy(bytes, index + 1, shorter, index, bytes.length - index - 1);
                requireCleanOutcome(shorter);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {11, 12, 13, 14, 15})
    void randomBytesEitherParseOrFailCleanly(long seed) {
        Random random = new Random(seed);
        for (int input = 0; input < RANDOM_INPUTS; input++) {
            byte[] bytes = new byte[random.nextInt(MAX_RANDOM_BYTES)];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = random.nextInt(4) == 0 ? (byte) random.nextInt(256) : PROBES[random.nextInt(PROBES.length)];
            }
            requireCleanOutcome(bytes);
        }
    }

    private static void requireCleanOutcome(byte[] bytes) {
        JsonValue parsed;
        try {
            parsed = JsonParser.parse(bytes);
        } catch (JsonException expected) {
            return;
        }
        String written = new JsonWriter().value(parsed).toString();
        assertEquals(parsed, JsonParser.parse(written), "accepted input does not survive a rewrite: "
                + Arrays.toString(bytes));
    }
}
