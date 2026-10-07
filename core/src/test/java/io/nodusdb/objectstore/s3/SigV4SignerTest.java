package io.nodusdb.objectstore.s3;

import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.json.JsonString;
import io.nodusdb.json.JsonValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SigV4SignerTest {

    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    private static final String EMPTY_PAYLOAD = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final Credentials DOCUMENTATION_CREDENTIALS =
            new Credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

    private static java.time.Instant instant(String amzDate) {
        return LocalDateTime.parse(amzDate, AMZ_DATE).toInstant(ZoneOffset.UTC);
    }

    private static JsonArray vectors() throws IOException {
        try (InputStream in = SigV4SignerTest.class.getResourceAsStream("/sigv4/vectors.json")) {
            JsonObject document = JsonParser.parseObject(in.readAllBytes());
            return document.requireArray("vectors");
        }
    }

    private static Map<String, String> stringMap(JsonObject object) {
        Map<String, String> map = new LinkedHashMap<>();
        for (Map.Entry<String, JsonValue> entry : object.members().entrySet()) {
            map.put(entry.getKey(), ((JsonString) entry.getValue()).value());
        }
        return map;
    }

    @Test
    void theAwsDocumentationExampleProducesTheDocumentedSignature() {
        SigV4Signer signer = new SigV4Signer("us-east-1", "s3");

        SigV4Signer.Signed signed = signer.sign("GET", "examplebucket.s3.amazonaws.com", "/test.txt", "",
                Map.of("range", "bytes=0-9"), EMPTY_PAYLOAD, DOCUMENTATION_CREDENTIALS, instant("20130524T000000Z"));

        assertEquals("GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\n"
                + "x-amz-content-sha256:" + EMPTY_PAYLOAD + "\nx-amz-date:20130524T000000Z\n\n"
                + "host;range;x-amz-content-sha256;x-amz-date\n" + EMPTY_PAYLOAD, signed.canonicalRequest());
        assertEquals("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", signed.signature());
        assertEquals("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
                + "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=" + signed.signature(),
                signed.headers().get("authorization").replace(", ", ","));
    }

    @Test
    void everyBotocoreVectorMatchesExactly() throws IOException {
        JsonArray vectors = vectors();
        assertTrue(vectors.size() >= 150, "the corpus is smaller than expected: " + vectors.size());

        for (JsonValue item : vectors.items()) {
            JsonObject vector = (JsonObject) item;
            String name = vector.requireString("name");
            JsonObject expected = vector.requireObject("expected");
            byte[] body = HexFormat.of().parseHex(vector.requireString("body_hex"));
            String payload = SigV4Signer.sha256Hex(body);
            Credentials credentials = new Credentials(vector.requireString("access_key"),
                    vector.requireString("secret_key"), vector.has("token") && vector.get("token") instanceof JsonString
                    ? vector.requireString("token") : null);

            SigV4Signer.Signed signed = new SigV4Signer(vector.requireString("region"), "s3").sign(
                    vector.requireString("method"), vector.requireString("host"), vector.requireString("path"),
                    vector.requireString("query"), stringMap(vector.requireObject("headers")), payload, credentials,
                    instant(vector.requireString("date")));

            assertEquals(expected.requireString("payload_sha256"), payload, name);
            assertEquals(expected.requireString("canonical_request"), signed.canonicalRequest(), name);
            assertEquals(expected.requireString("string_to_sign"), signed.stringToSign(), name);
            assertEquals(expected.requireString("signature"), signed.signature(), name);
            assertEquals(stringMap(expected.requireObject("headers")), signed.headers(), name);
        }
    }

    @Test
    void aSessionTokenIsSignedAndSent() {
        Credentials temporary = new Credentials("AKID", "secret", "token-value");

        SigV4Signer.Signed signed = new SigV4Signer("us-east-1", "s3").sign("GET", "s3.amazonaws.com", "/b/k", "",
                Map.of(), EMPTY_PAYLOAD, temporary, instant("20260101T000000Z"));

        assertEquals("token-value", signed.headers().get("x-amz-security-token"));
        assertTrue(signed.canonicalRequest().contains("x-amz-security-token:token-value\n"));
        assertTrue(signed.headers().get("authorization").contains("x-amz-security-token"));
    }

    @Test
    void theSameInputAlwaysGivesTheSameSignature() {
        SigV4Signer signer = new SigV4Signer("us-east-1", "s3");

        String first = signer.sign("PUT", "h", "/b/k", "a=1", Map.of("if-none-match", "*"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature();
        String second = signer.sign("PUT", "h", "/b/k", "a=1", Map.of("if-none-match", "*"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature();

        assertEquals(first, second);
    }

    @Test
    void changingAnySignedInputChangesTheSignature() {
        SigV4Signer signer = new SigV4Signer("us-east-1", "s3");
        String base = signer.sign("PUT", "h", "/b/k", "a=1", Map.of("x-amz-meta-n", "1"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature();

        assertTrue(!base.equals(signer.sign("GET", "h", "/b/k", "a=1", Map.of("x-amz-meta-n", "1"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature()));
        assertTrue(!base.equals(signer.sign("PUT", "h2", "/b/k", "a=1", Map.of("x-amz-meta-n", "1"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature()));
        assertTrue(!base.equals(signer.sign("PUT", "h", "/b/k2", "a=1", Map.of("x-amz-meta-n", "1"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature()));
        assertTrue(!base.equals(signer.sign("PUT", "h", "/b/k", "a=2", Map.of("x-amz-meta-n", "1"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature()));
        assertTrue(!base.equals(signer.sign("PUT", "h", "/b/k", "a=1", Map.of("x-amz-meta-n", "2"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")).signature()));
        assertTrue(!base.equals(signer.sign("PUT", "h", "/b/k", "a=1", Map.of("x-amz-meta-n", "1"),
                SigV4Signer.sha256Hex("x".getBytes(StandardCharsets.UTF_8)), DOCUMENTATION_CREDENTIALS,
                instant("20260101T000000Z")).signature()));
        assertTrue(!base.equals(signer.sign("PUT", "h", "/b/k", "a=1", Map.of("x-amz-meta-n", "1"), EMPTY_PAYLOAD,
                DOCUMENTATION_CREDENTIALS, instant("20260101T000001Z")).signature()));
        assertTrue(!base.equals(new SigV4Signer("eu-west-1", "s3").sign("PUT", "h", "/b/k", "a=1",
                Map.of("x-amz-meta-n", "1"), EMPTY_PAYLOAD, DOCUMENTATION_CREDENTIALS,
                instant("20260101T000000Z")).signature()));
    }

    @Test
    void aPlusSignInAQueryStringIsRefusedBecauseItIsAmbiguous() {
        SigV4Signer signer = new SigV4Signer("us-east-1", "s3");

        assertThrows(IllegalArgumentException.class, () -> signer.sign("GET", "h", "/b/", "prefix=a+b",
                Map.of(), EMPTY_PAYLOAD, DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")));
    }

    @Test
    void malformedPercentEscapesInAQueryAreRefused() {
        SigV4Signer signer = new SigV4Signer("us-east-1", "s3");

        for (String query : new String[]{"a=%", "a=%2", "a=%zz", "a=%2z", "%"}) {
            assertThrows(IllegalArgumentException.class, () -> signer.sign("GET", "h", "/b/", query,
                    Map.of(), EMPTY_PAYLOAD, DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")), query);
        }
    }

    @Test
    void aNonAsciiQueryIsRefused() {
        SigV4Signer signer = new SigV4Signer("us-east-1", "s3");

        assertThrows(IllegalArgumentException.class, () -> signer.sign("GET", "h", "/b/", "prefix=café",
                Map.of(), EMPTY_PAYLOAD, DOCUMENTATION_CREDENTIALS, instant("20260101T000000Z")));
    }

    @Test
    void aQueryIsNormalizedTheWayTheSpecificationRequires() {
        assertEquals("a=%2F&b=~&c=%20", SigV4Signer.canonicalQuery("c=%20&b=%7E&a=%2f"));
        assertEquals("a=1&a=2&b=", SigV4Signer.canonicalQuery("b&a=2&a=1"));
        assertEquals("", SigV4Signer.canonicalQuery(""));
        assertEquals("", SigV4Signer.canonicalQuery(null));
        assertEquals("uploads=", SigV4Signer.canonicalQuery("uploads"));
        assertEquals("a=%3D&a=b", SigV4Signer.canonicalQuery("a=b&a=%3D"));
    }

    @Test
    void anAbsentQueryAndAnEmptyQueryAreTheSame() {
        SigV4Signer signer = new SigV4Signer("us-east-1", "s3");

        assertEquals(
                signer.sign("GET", "h", "/b/k", "", Map.of(), EMPTY_PAYLOAD, DOCUMENTATION_CREDENTIALS,
                        instant("20260101T000000Z")).signature(),
                signer.sign("GET", "h", "/b/k", null, Map.of(), EMPTY_PAYLOAD, DOCUMENTATION_CREDENTIALS,
                        instant("20260101T000000Z")).signature());
    }

    @Test
    void encodingKeepsUnreservedCharactersAndUppercasesEscapes() {
        assertEquals("AZaz09-_.~", SigV4Signer.encode("AZaz09-_.~"));
        assertEquals("%20%2F%2B%3D%25%26", SigV4Signer.encode(" /+=%&"));
        assertEquals("caf%C3%A9%E2%82%AC%F0%9F%98%80", SigV4Signer.encode("café€😀"));
    }

    @Test
    void credentialsNeverRevealTheirSecretsInText() {
        Credentials credentials = new Credentials("AKID", "super-secret-key", "super-secret-token");

        String text = credentials.toString();

        assertTrue(text.contains("AKID"));
        assertTrue(!text.contains("super-secret"), text);
    }

    @Test
    void credentialsNeedAKeyAndASecret() {
        assertThrows(IllegalArgumentException.class, () -> new Credentials("", "secret"));
        assertThrows(IllegalArgumentException.class, () -> new Credentials("id", ""));
        assertThrows(IllegalArgumentException.class, () -> new Credentials("id", "secret", ""));
        assertThrows(NullPointerException.class, () -> new Credentials(null, "secret"));
    }

    @Test
    void aSignerNeedsARegionAndAService() {
        assertThrows(IllegalArgumentException.class, () -> new SigV4Signer("", "s3"));
        assertThrows(IllegalArgumentException.class, () -> new SigV4Signer("us-east-1", ""));
        assertThrows(IllegalArgumentException.class, () -> new SigV4Signer(null, "s3"));
    }
}
