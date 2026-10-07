package io.nodusdb.objectstore.s3;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public final class SigV4Signer {

    public static final String DATE_HEADER = "x-amz-date";
    public static final String CONTENT_HASH_HEADER = "x-amz-content-sha256";
    public static final String SECURITY_TOKEN_HEADER = "x-amz-security-token";
    public static final String AUTHORIZATION_HEADER = "authorization";

    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String HMAC = "HmacSHA256";
    private static final String TERMINATOR = "aws4_request";
    private static final DateTimeFormatter AMZ_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter SCOPE_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
    private static final HexFormat HEX = HexFormat.of();
    private static final char[] UPPER_HEX = "0123456789ABCDEF".toCharArray();

    public record Signed(Map<String, String> headers, String canonicalRequest, String stringToSign,
                         String signature) {

        public Signed {
            headers = Map.copyOf(headers);
        }
    }

    private final String region;
    private final String service;

    public SigV4Signer(String region, String service) {
        if (region == null || region.isEmpty() || service == null || service.isEmpty()) {
            throw new IllegalArgumentException("a region and a service are required");
        }
        this.region = region;
        this.service = service;
    }

    public static String sha256Hex(byte[] content) {
        return HEX.formatHex(sha256().digest(content));
    }

    public Signed sign(String method, String host, String rawPath, String rawQuery, Map<String, String> headers,
                       String payloadSha256Hex, Credentials credentials, Instant now) {
        String amzDate = AMZ_DATE.format(now);
        String scopeDate = SCOPE_DATE.format(now);
        Map<String, String> signedHeaders = new TreeMap<>();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            signedHeaders.put(header.getKey().toLowerCase(Locale.ROOT), normalizeValue(header.getValue()));
        }
        signedHeaders.put("host", normalizeValue(host));
        signedHeaders.put(DATE_HEADER, amzDate);
        signedHeaders.put(CONTENT_HASH_HEADER, payloadSha256Hex);
        if (credentials.hasSessionToken()) {
            signedHeaders.put(SECURITY_TOKEN_HEADER, credentials.sessionToken());
        }

        String signedHeaderNames = String.join(";", signedHeaders.keySet());
        String canonicalRequest = method + "\n" + rawPath + "\n" + canonicalQuery(rawQuery) + "\n"
                + canonicalHeaders(signedHeaders) + "\n" + signedHeaderNames + "\n" + payloadSha256Hex;
        String scope = scopeDate + "/" + region + "/" + service + "/" + TERMINATOR;
        String stringToSign = ALGORITHM + "\n" + amzDate + "\n" + scope + "\n"
                + sha256Hex(canonicalRequest.getBytes(StandardCharsets.UTF_8));
        String signature = HEX.formatHex(hmac(signingKey(credentials.secretAccessKey(), scopeDate),
                stringToSign));

        Map<String, String> produced = new LinkedHashMap<>();
        produced.put(DATE_HEADER, amzDate);
        produced.put(CONTENT_HASH_HEADER, payloadSha256Hex);
        if (credentials.hasSessionToken()) {
            produced.put(SECURITY_TOKEN_HEADER, credentials.sessionToken());
        }
        produced.put(AUTHORIZATION_HEADER, ALGORITHM + " Credential=" + credentials.accessKeyId() + "/" + scope
                + ", SignedHeaders=" + signedHeaderNames + ", Signature=" + signature);
        return new Signed(produced, canonicalRequest, stringToSign, signature);
    }

    static String canonicalQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }
        if (rawQuery.indexOf('+') >= 0) {
            throw new IllegalArgumentException("a query string must encode a space as %20, not '+'");
        }
        List<String[]> pairs = new ArrayList<>();
        for (String piece : rawQuery.split("&", -1)) {
            int separator = piece.indexOf('=');
            String name = separator < 0 ? piece : piece.substring(0, separator);
            String value = separator < 0 ? "" : piece.substring(separator + 1);
            pairs.add(new String[]{encode(decode(name)), encode(decode(value))});
        }
        pairs.sort((a, b) -> {
            int byName = a[0].compareTo(b[0]);
            return byName != 0 ? byName : a[1].compareTo(b[1]);
        });
        StringBuilder query = new StringBuilder();
        for (String[] pair : pairs) {
            if (query.length() > 0) {
                query.append('&');
            }
            query.append(pair[0]).append('=').append(pair[1]);
        }
        return query.toString();
    }

    public static String encode(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        StringBuilder out = new StringBuilder(bytes.length);
        for (byte b : bytes) {
            int c = b & 0xFF;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.' || c == '~') {
                out.append((char) c);
            } else {
                out.append('%').append(UPPER_HEX[c >> 4]).append(UPPER_HEX[c & 0xF]);
            }
        }
        return out.toString();
    }

    static String decode(String text) {
        byte[] out = new byte[text.length()];
        int length = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '%') {
                if (i + 2 >= text.length()) {
                    throw new IllegalArgumentException("a truncated percent escape in a query string");
                }
                int high = Character.digit(text.charAt(i + 1), 16);
                int low = Character.digit(text.charAt(i + 2), 16);
                if (high < 0 || low < 0) {
                    throw new IllegalArgumentException("an invalid percent escape in a query string");
                }
                out[length++] = (byte) (high * 16 + low);
                i += 2;
            } else if (c < 0x80) {
                out[length++] = (byte) c;
            } else {
                throw new IllegalArgumentException("a query string must hold only ASCII characters");
            }
        }
        return new String(out, 0, length, StandardCharsets.UTF_8);
    }

    private static String canonicalHeaders(Map<String, String> sortedHeaders) {
        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, String> header : sortedHeaders.entrySet()) {
            out.append(header.getKey()).append(':').append(header.getValue()).append('\n');
        }
        return out.toString();
    }

    private static String normalizeValue(String value) {
        StringBuilder out = new StringBuilder(value.length());
        boolean pendingSpace = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == ' ' || c == '\t') {
                pendingSpace = out.length() > 0;
            } else {
                if (pendingSpace) {
                    out.append(' ');
                    pendingSpace = false;
                }
                out.append(c);
            }
        }
        return out.toString();
    }

    private byte[] signingKey(String secret, String scopeDate) {
        byte[] dateKey = hmac(("AWS4" + secret).getBytes(StandardCharsets.UTF_8), scopeDate);
        byte[] regionKey = hmac(dateKey, region);
        byte[] serviceKey = hmac(regionKey, service);
        return hmac(serviceKey, TERMINATOR);
    }

    private static byte[] hmac(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance(HMAC);
            mac.init(new SecretKeySpec(key, HMAC));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", e);
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
