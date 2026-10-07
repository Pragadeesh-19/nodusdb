package io.nodusdb.objectstore.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

final class FakeS3Server implements AutoCloseable {

    static final String BUCKET = "test-bucket";
    static final String REGION = "us-east-1";
    static final Credentials CREDENTIALS = new Credentials("TESTACCESSKEY", "test-secret-access-key");

    record Req(String method, String path, String query, Map<String, String> headers, byte[] body) {

        boolean has(String parameter) {
            for (String piece : query.split("&")) {
                String name = piece.contains("=") ? piece.substring(0, piece.indexOf('=')) : piece;
                if (decode(name).equals(parameter)) {
                    return true;
                }
            }
            return false;
        }

        String parameter(String name) {
            for (String piece : query.split("&")) {
                int separator = piece.indexOf('=');
                String key = separator < 0 ? piece : piece.substring(0, separator);
                if (decode(key).equals(name)) {
                    return separator < 0 ? "" : decode(piece.substring(separator + 1));
                }
            }
            return null;
        }
    }

    private enum Kind {
        RESPOND, DROP_BEFORE, DROP_AFTER, DELAY
    }

    private static final class Fault {

        final Predicate<Req> matcher;
        final Kind kind;
        final int status;
        final String code;
        final long delayMillis;
        int remaining;

        Fault(Predicate<Req> matcher, Kind kind, int times, int status, String code, long delayMillis) {
            this.matcher = matcher;
            this.kind = kind;
            this.remaining = times;
            this.status = status;
            this.code = code;
            this.delayMillis = delayMillis;
        }
    }

    private record Stored(byte[] content, long modifiedMillis, Map<String, String> metadata) {
    }

    private record Response(int status, Map<String, String> headers, byte[] body) {

        static Response of(int status, String body) {
            return new Response(status, Map.of("Content-Type", "application/xml"), body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class Upload {

        final String key;
        final Map<String, String> metadata;
        final long initiatedMillis;
        final TreeMap<Integer, byte[]> parts = new TreeMap<>();

        Upload(String key, Map<String, String> metadata, long initiatedMillis) {
            this.key = key;
            this.metadata = metadata;
            this.initiatedMillis = initiatedMillis;
        }
    }

    private static final Set<String> IMPLICIT_HEADERS = Set.of("host", "x-amz-date", "x-amz-content-sha256",
            "x-amz-security-token");

    private static final int WORKERS = 8;

    private final HttpServer server;
    private final boolean secure;
    private final ExecutorService workers;
    private final ConcurrentSkipListMap<String, Stored> objects = new ConcurrentSkipListMap<>();
    private final Map<String, Upload> uploads = new LinkedHashMap<>();
    private final List<Fault> faults = new ArrayList<>();
    private final List<Req> requests = new ArrayList<>();
    private final Set<String> undeletable = new HashSet<>();
    private final AtomicInteger uploadCounter = new AtomicInteger();
    private LongSupplier clockMillis = System::currentTimeMillis;
    private Credentials accepted = CREDENTIALS;
    private boolean verifySignatures = true;
    private boolean ignoreConditional;
    private boolean ignoreRange;
    private boolean tokensExpired;
    private long minPartBytes = 5L << 20;
    private int uploadPageSize = 1000;

    private FakeS3Server(HttpServer server, boolean secure) {
        this.server = server;
        this.secure = secure;
        this.workers = Executors.newFixedThreadPool(WORKERS, runnable -> {
            Thread thread = new Thread(runnable, "fake-s3-worker");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(workers);
        server.createContext("/", this::handle);
        server.start();
    }

    static FakeS3Server start() {
        try {
            return new FakeS3Server(HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0), false);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static FakeS3Server startSecure(Path keystore) {
        try {
            KeyStore store = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(keystore)) {
                store.load(in, "changeit".toCharArray());
            }
            KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, "changeit".toCharArray());
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys.getKeyManagers(), null, null);
            HttpsServer https = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            https.setHttpsConfigurator(new HttpsConfigurator(context));
            return new FakeS3Server(https, true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    URI endpoint() {
        return URI.create((secure ? "https" : "http") + (secure ? "://localhost:" : "://127.0.0.1:")
                + server.getAddress().getPort());
    }

    S3Config config() {
        return S3Config.of(endpoint(), REGION, BUCKET);
    }

    @Override
    public void close() {
        server.stop(0);
        workers.shutdownNow();
    }

    synchronized FakeS3Server clock(LongSupplier clock) {
        this.clockMillis = clock;
        return this;
    }

    synchronized FakeS3Server verifySignatures(boolean verify) {
        this.verifySignatures = verify;
        return this;
    }

    synchronized FakeS3Server ignoreConditionalWrites() {
        this.ignoreConditional = true;
        return this;
    }

    synchronized FakeS3Server ignoreRange() {
        this.ignoreRange = true;
        return this;
    }

    synchronized FakeS3Server minPartBytes(long bytes) {
        this.minPartBytes = bytes;
        return this;
    }

    synchronized FakeS3Server uploadPageSize(int size) {
        this.uploadPageSize = size;
        return this;
    }

    synchronized FakeS3Server failDeleteOf(String key) {
        undeletable.add(key);
        return this;
    }

    synchronized FakeS3Server rotateCredentials(Credentials credentials) {
        this.accepted = credentials;
        return this;
    }

    synchronized FakeS3Server expireTokens(boolean expired) {
        this.tokensExpired = expired;
        return this;
    }

    synchronized FakeS3Server failNext(Predicate<Req> matcher, int times, int status, String code) {
        faults.add(new Fault(matcher, Kind.RESPOND, times, status, code, 0));
        return this;
    }

    synchronized FakeS3Server dropNext(Predicate<Req> matcher, int times, boolean applyFirst) {
        faults.add(new Fault(matcher, applyFirst ? Kind.DROP_AFTER : Kind.DROP_BEFORE, times, 0, "", 0));
        return this;
    }

    synchronized FakeS3Server delayNext(Predicate<Req> matcher, int times, long millis) {
        faults.add(new Fault(matcher, Kind.DELAY, times, 0, "", millis));
        return this;
    }

    synchronized List<Req> requests() {
        return List.copyOf(requests);
    }

    synchronized long count(Predicate<Req> matcher) {
        return requests.stream().filter(matcher).count();
    }

    synchronized long lastModifiedOf(String key) {
        return objects.get(key).modifiedMillis();
    }

    synchronized byte[] content(String key) {
        Stored stored = objects.get(key);
        return stored == null ? null : stored.content();
    }

    synchronized Map<String, String> metadataOf(String key) {
        return objects.get(key).metadata();
    }

    synchronized int objectCount() {
        return objects.size();
    }

    synchronized int uploadCount() {
        return uploads.size();
    }

    synchronized void putRaw(String key, byte[] content) {
        objects.put(key, new Stored(content.clone(), clockMillis.getAsLong(), Map.of()));
    }

    synchronized String startUpload(String key, long initiatedMillis) {
        String id = "upload-" + uploadCounter.incrementAndGet();
        uploads.put(id, new Upload(key, Map.of(), initiatedMillis));
        return id;
    }

    private void handle(HttpExchange exchange) throws IOException {
        Req request = read(exchange);
        Fault fault;
        synchronized (this) {
            requests.add(request);
            fault = takeFault(request);
        }
        if (fault != null && fault.kind == Kind.DELAY) {
            pause(fault.delayMillis);
        }
        if (fault != null && fault.kind == Kind.DROP_BEFORE) {
            exchange.close();
            return;
        }
        if (fault != null && fault.kind == Kind.RESPOND) {
            send(exchange, error(fault.status, fault.code, "injected " + fault.code), request);
            return;
        }
        Response response;
        synchronized (this) {
            response = authenticate(request);
            if (response == null) {
                response = route(request);
            }
        }
        if (fault != null && fault.kind == Kind.DROP_AFTER) {
            exchange.close();
            return;
        }
        send(exchange, response, request);
    }

    private Fault takeFault(Req request) {
        for (Fault fault : faults) {
            if (fault.remaining > 0 && fault.matcher.test(request)) {
                fault.remaining--;
                return fault;
            }
        }
        return null;
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Req read(HttpExchange exchange) throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(Locale.ROOT), values.get(0)));
        URI uri = exchange.getRequestURI();
        String query = uri.getRawQuery() == null ? "" : uri.getRawQuery();
        return new Req(exchange.getRequestMethod(), uri.getRawPath(), query, headers,
                exchange.getRequestBody().readAllBytes());
    }

    private static void send(HttpExchange exchange, Response response, Req request) throws IOException {
        response.headers().forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
        boolean head = "HEAD".equals(request.method());
        byte[] body = response.body();
        if (head || body.length == 0) {
            exchange.sendResponseHeaders(response.status(), -1);
        } else {
            exchange.sendResponseHeaders(response.status(), body.length);
            exchange.getResponseBody().write(body);
        }
        exchange.close();
    }

    private Response authenticate(Req request) {
        if (!verifySignatures) {
            return null;
        }
        String authorization = request.headers().get("authorization");
        if (authorization == null || !authorization.startsWith("AWS4-HMAC-SHA256 ")) {
            return error(403, "AccessDenied", "missing or unsupported Authorization header");
        }
        Map<String, String> parts = new LinkedHashMap<>();
        for (String piece : authorization.substring("AWS4-HMAC-SHA256 ".length()).split(",\\s*")) {
            int separator = piece.indexOf('=');
            parts.put(piece.substring(0, separator), piece.substring(separator + 1));
        }
        String accessKeyId = parts.get("Credential").split("/")[0];
        if (!accessKeyId.equals(accepted.accessKeyId())) {
            return error(403, "InvalidAccessKeyId", "the access key id is unknown");
        }
        String token = request.headers().get("x-amz-security-token");
        if (token != null && tokensExpired) {
            return error(400, "ExpiredToken", "the provided token has expired");
        }
        if (accepted.hasSessionToken() && !accepted.sessionToken().equals(token)) {
            return error(403, "InvalidToken", "the provided token is invalid");
        }
        String payloadHash = request.headers().get("x-amz-content-sha256");
        if (payloadHash == null || !payloadHash.equals(SigV4Signer.sha256Hex(request.body()))) {
            return error(400, "XAmzContentSHA256Mismatch", "the payload hash does not match the body");
        }
        Map<String, String> others = new LinkedHashMap<>();
        for (String name : parts.get("SignedHeaders").split(";")) {
            if (!request.headers().containsKey(name)) {
                return error(403, "SignatureDoesNotMatch", "a signed header is absent: " + name);
            }
            if (!IMPLICIT_HEADERS.contains(name)) {
                others.put(name, request.headers().get(name));
            }
        }
        Instant moment = ZonedDateTime.parse(request.headers().get("x-amz-date"),
                DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)).toInstant();
        SigV4Signer.Signed expected = new SigV4Signer(REGION, "s3").sign(request.method(),
                request.headers().get("host"), request.path(), request.query(), others, payloadHash, accepted,
                moment);
        if (!expected.signature().equals(parts.get("Signature"))) {
            return error(403, "SignatureDoesNotMatch", "the request signature does not match");
        }
        return null;
    }

    private Response route(Req request) {
        String bucketPath = "/" + BUCKET;
        String path = request.path();
        if (!path.equals(bucketPath) && !path.startsWith(bucketPath + "/")) {
            return error(404, "NoSuchBucket", "the bucket does not exist");
        }
        String key = path.length() <= bucketPath.length() + 1 ? ""
                : decode(path.substring(bucketPath.length() + 1));
        boolean bucketLevel = key.isEmpty();
        return switch (request.method()) {
            case "GET" -> bucketLevel
                    ? (request.has("uploads") ? listUploads(request) : listObjects(request))
                    : getObject(request, key);
            case "HEAD" -> headObject(key);
            case "PUT" -> request.has("partNumber") ? uploadPart(request) : putObject(request, key);
            case "POST" -> postObject(request, key, bucketLevel);
            case "DELETE" -> request.has("uploadId") ? abortUpload(request) : deleteObject(key);
            default -> error(405, "MethodNotAllowed", "unsupported method");
        };
    }

    private Response putObject(Req request, String key) {
        if ("*".equals(request.headers().get("if-none-match")) && !ignoreConditional && objects.containsKey(key)) {
            return error(412, "PreconditionFailed", "the object already exists");
        }
        Stored stored = new Stored(request.body().clone(), clockMillis.getAsLong(), metadataOf(request));
        objects.put(key, stored);
        return new Response(200, Map.of("ETag", etag(request.body())), new byte[0]);
    }

    private Response getObject(Req request, String key) {
        Stored stored = objects.get(key);
        if (stored == null) {
            return error(404, "NoSuchKey", "the specified key does not exist");
        }
        String range = request.headers().get("range");
        if (range == null || ignoreRange) {
            return new Response(200, Map.of("ETag", etag(stored.content())), stored.content());
        }
        String[] bounds = range.substring("bytes=".length()).split("-");
        long from = Long.parseLong(bounds[0]);
        long to = Math.min(Long.parseLong(bounds[1]), stored.content().length - 1L);
        if (from >= stored.content().length) {
            return error(416, "InvalidRange", "the requested range is not satisfiable");
        }
        byte[] slice = Arrays.copyOfRange(stored.content(), (int) from, (int) to + 1);
        return new Response(206, Map.of("Content-Range", "bytes " + from + "-" + to + "/" + stored.content().length),
                slice);
    }

    private Response headObject(String key) {
        Stored stored = objects.get(key);
        if (stored == null) {
            return new Response(404, Map.of(), new byte[0]);
        }
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Length", Integer.toString(stored.content().length));
        headers.put("Last-Modified", DateTimeFormatter.RFC_1123_DATE_TIME.format(
                Instant.ofEpochMilli(stored.modifiedMillis()).atZone(ZoneOffset.UTC)));
        headers.put("ETag", etag(stored.content()));
        stored.metadata().forEach((name, value) -> headers.put("x-amz-meta-" + name, value));
        return new Response(200, headers, new byte[0]);
    }

    private Response deleteObject(String key) {
        if (undeletable.contains(key)) {
            return error(403, "AccessDenied", "access denied");
        }
        objects.remove(key);
        return new Response(204, Map.of(), new byte[0]);
    }

    private Response listObjects(Req request) {
        String prefix = orEmpty(request.parameter("prefix"));
        String startAfter = orEmpty(request.parameter("start-after"));
        int maxKeys = Integer.parseInt(orDefault(request.parameter("max-keys"), "1000"));
        boolean encode = "url".equals(request.parameter("encoding-type"));
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>" + BUCKET + "</Name>");
        int count = 0;
        boolean truncated = false;
        for (Map.Entry<String, Stored> entry : objects.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(prefix) || (!startAfter.isEmpty() && key.compareTo(startAfter) <= 0)) {
                continue;
            }
            if (count == maxKeys) {
                truncated = true;
                break;
            }
            count++;
            xml.append("<Contents><Key>").append(XmlText.escape(encode ? URLEncoder.encode(key, StandardCharsets.UTF_8) : key))
                    .append("</Key><LastModified>").append(Instant.ofEpochMilli(entry.getValue().modifiedMillis()))
                    .append("</LastModified><ETag>").append(XmlText.escape(etag(entry.getValue().content())))
                    .append("</ETag><Size>").append(entry.getValue().content().length)
                    .append("</Size><StorageClass>STANDARD</StorageClass></Contents>");
        }
        xml.append("<KeyCount>").append(count).append("</KeyCount><IsTruncated>").append(truncated)
                .append("</IsTruncated></ListBucketResult>");
        return Response.of(200, xml.toString());
    }

    private Response postObject(Req request, String key, boolean bucketLevel) {
        if (bucketLevel && request.has("delete")) {
            return deleteObjects(request);
        }
        if (request.has("uploads")) {
            return createUpload(request, key);
        }
        if (request.has("uploadId")) {
            return completeUpload(request);
        }
        return error(400, "InvalidRequest", "unsupported POST");
    }

    private Response deleteObjects(Req request) {
        String expected = md5Base64(request.body());
        if (!expected.equals(request.headers().get("content-md5"))) {
            return error(400, "BadDigest", "the Content-MD5 does not match the body");
        }
        String xml = new String(request.body(), StandardCharsets.UTF_8);
        StringBuilder result = new StringBuilder("<DeleteResult>");
        for (String object : XmlText.blocks(xml, "Object")) {
            String key = XmlText.text(object, "Key").orElse("");
            if (undeletable.contains(key)) {
                result.append("<Error><Key>").append(XmlText.escape(key))
                        .append("</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>");
            } else {
                objects.remove(key);
            }
        }
        return Response.of(200, result.append("</DeleteResult>").toString());
    }

    private Response createUpload(Req request, String key) {
        String id = "upload-" + uploadCounter.incrementAndGet();
        uploads.put(id, new Upload(key, metadataOf(request), clockMillis.getAsLong()));
        return Response.of(200, "<InitiateMultipartUploadResult><Bucket>" + BUCKET + "</Bucket><Key>"
                + XmlText.escape(key) + "</Key><UploadId>" + id + "</UploadId></InitiateMultipartUploadResult>");
    }

    private Response uploadPart(Req request) {
        Upload upload = uploads.get(request.parameter("uploadId"));
        if (upload == null) {
            return error(404, "NoSuchUpload", "the upload does not exist");
        }
        upload.parts.put(Integer.parseInt(request.parameter("partNumber")), request.body().clone());
        return new Response(200, Map.of("ETag", etag(request.body())), new byte[0]);
    }

    private Response completeUpload(Req request) {
        String id = request.parameter("uploadId");
        Upload upload = uploads.get(id);
        if (upload == null) {
            return error(404, "NoSuchUpload", "the upload does not exist");
        }
        List<String> parts = XmlText.blocks(new String(request.body(), StandardCharsets.UTF_8), "Part");
        ByteArrayOutputStream assembled = new ByteArrayOutputStream();
        int previous = 0;
        for (int i = 0; i < parts.size(); i++) {
            int number = Integer.parseInt(XmlText.text(parts.get(i), "PartNumber").orElse("0"));
            String etag = XmlText.text(parts.get(i), "ETag").orElse("");
            byte[] body = upload.parts.get(number);
            if (number <= previous || body == null || !etag(body).equals(etag)) {
                return error(400, "InvalidPart", "a part is missing, out of order or has another ETag");
            }
            if (i < parts.size() - 1 && body.length < minPartBytes) {
                return error(400, "EntityTooSmall", "a part other than the last is too small");
            }
            previous = number;
            assembled.writeBytes(body);
        }
        objects.put(upload.key, new Stored(assembled.toByteArray(), clockMillis.getAsLong(), upload.metadata));
        uploads.remove(id);
        return Response.of(200, "<CompleteMultipartUploadResult><Bucket>" + BUCKET + "</Bucket><Key>"
                + XmlText.escape(upload.key) + "</Key><ETag>&quot;final-0&quot;</ETag></CompleteMultipartUploadResult>");
    }

    private Response abortUpload(Req request) {
        if (uploads.remove(request.parameter("uploadId")) == null) {
            return error(404, "NoSuchUpload", "the upload does not exist");
        }
        return new Response(204, Map.of(), new byte[0]);
    }

    private Response listUploads(Req request) {
        String prefix = orEmpty(request.parameter("prefix"));
        String keyMarker = orEmpty(request.parameter("key-marker"));
        String uploadMarker = orEmpty(request.parameter("upload-id-marker"));
        List<Map.Entry<String, Upload>> ordered = new ArrayList<>(uploads.entrySet());
        ordered.sort((a, b) -> {
            int byKey = a.getValue().key.compareTo(b.getValue().key);
            return byKey != 0 ? byKey : a.getKey().compareTo(b.getKey());
        });
        StringBuilder xml = new StringBuilder("<ListMultipartUploadsResult><Bucket>" + BUCKET + "</Bucket>");
        int count = 0;
        boolean truncated = false;
        String lastKey = "";
        String lastId = "";
        for (Map.Entry<String, Upload> entry : ordered) {
            Upload upload = entry.getValue();
            boolean afterMarker = keyMarker.isEmpty()
                    || upload.key.compareTo(keyMarker) > 0
                    || (upload.key.equals(keyMarker) && entry.getKey().compareTo(uploadMarker) > 0);
            if (!upload.key.startsWith(prefix) || !afterMarker) {
                continue;
            }
            if (count == uploadPageSize) {
                truncated = true;
                break;
            }
            count++;
            lastKey = upload.key;
            lastId = entry.getKey();
            xml.append("<Upload><Key>").append(XmlText.escape(URLEncoder.encode(upload.key, StandardCharsets.UTF_8)))
                    .append("</Key><UploadId>").append(entry.getKey()).append("</UploadId><Initiated>")
                    .append(Instant.ofEpochMilli(upload.initiatedMillis)).append("</Initiated></Upload>");
        }
        xml.append("<IsTruncated>").append(truncated).append("</IsTruncated>");
        if (truncated) {
            xml.append("<NextKeyMarker>").append(XmlText.escape(URLEncoder.encode(lastKey, StandardCharsets.UTF_8)))
                    .append("</NextKeyMarker><NextUploadIdMarker>").append(lastId).append("</NextUploadIdMarker>");
        }
        return Response.of(200, xml.append("</ListMultipartUploadsResult>").toString());
    }

    private static Map<String, String> metadataOf(Req request) {
        Map<String, String> metadata = new LinkedHashMap<>();
        request.headers().forEach((name, value) -> {
            if (name.startsWith("x-amz-meta-")) {
                metadata.put(name.substring("x-amz-meta-".length()), value);
            }
        });
        return metadata;
    }

    private static Response error(int status, String code, String message) {
        return Response.of(status, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + code + "</Code><Message>"
                + XmlText.escape(message) + "</Message><RequestId>fake</RequestId></Error>");
    }

    private static String etag(byte[] content) {
        try {
            return "\"" + HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(content)) + "\"";
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String md5Base64(byte[] content) {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String decode(String text) {
        return URLDecoder.decode(text.replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    private static String orEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String orDefault(String value, String fallback) {
        return value == null ? fallback : value;
    }
}
