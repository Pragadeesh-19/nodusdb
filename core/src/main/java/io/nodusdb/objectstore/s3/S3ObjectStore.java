package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.FatalStoreException;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectKeys;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.PutResult;
import io.nodusdb.objectstore.TransientStoreException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntConsumer;

public final class S3ObjectStore implements ObjectStore {

    public static final int MAX_LIST_KEYS = 1000;
    public static final int MAX_DELETE_BATCH = 1000;
    public static final long DEFAULT_PART_BYTES = MultipartUploader.DEFAULT_PART_BYTES;

    private static final int PRECONDITION_FAILED = 412;
    private static final int CONFLICT = 409;
    private static final int NOT_FOUND = 404;
    private static final int PARTIAL_CONTENT = 206;
    private static final int RANGE_NOT_SATISFIABLE = 416;
    private static final String NO_SUCH_BUCKET = "NoSuchBucket";

    private final S3Config config;
    private final S3Http http;
    private final MultipartUploader uploader;
    private final long multipartThreshold;

    public S3ObjectStore(S3Config config, CredentialsProvider credentials) {
        this(config, credentials, Clock.systemUTC(), DEFAULT_PART_BYTES, DEFAULT_PART_BYTES,
                MultipartUploader.exponentialBackoff());
    }

    S3ObjectStore(S3Config config, CredentialsProvider credentials, Clock clock, long multipartThreshold,
                  long partBytes, IntConsumer pause) {
        if (multipartThreshold < 1) {
            throw new IllegalArgumentException("the multipart threshold must be at least one byte");
        }
        this.config = config;
        this.http = new S3Http(config, credentials, clock);
        this.uploader = new MultipartUploader(config, http, partBytes, pause);
        this.multipartThreshold = multipartThreshold;
    }

    S3ObjectStore(S3Config config, CredentialsProvider credentials, Clock clock) {
        this(config, credentials, clock, DEFAULT_PART_BYTES, DEFAULT_PART_BYTES,
                MultipartUploader.exponentialBackoff());
    }

    @Override
    public PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata) {
        validate(key, metadata);
        Map<String, String> headers = S3Metadata.headers(metadata);
        headers.put("if-none-match", "*");
        S3Response response = http.send(new S3Http.Request("PUT", config.objectPath(key), "", headers,
                new S3Payload.Bytes(content), config.requestTimeout()));
        if (response.successful()) {
            return PutResult.CREATED;
        }
        return switch (response.status()) {
            case PRECONDITION_FAILED -> PutResult.ALREADY_EXISTS;
            case CONFLICT -> PutResult.CONFLICT;
            default -> throw S3Errors.failure("create " + key, response);
        };
    }

    @Override
    public void put(String key, byte[] content) {
        validate(key, Map.of());
        S3Response response = http.send(new S3Http.Request("PUT", config.objectPath(key), "", Map.of(),
                new S3Payload.Bytes(content), config.requestTimeout()));
        if (!response.successful()) {
            throw S3Errors.failure("write " + key, response);
        }
    }

    @Override
    public void putFile(String key, Path file, Map<String, String> metadata) {
        validate(key, metadata);
        long size;
        try {
            size = Files.size(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (size > multipartThreshold) {
            uploader.upload(key, file, size, metadata);
            return;
        }
        S3Response response = http.send(new S3Http.Request("PUT", config.objectPath(key), "",
                S3Metadata.headers(metadata), new S3Payload.FileSlice(file, 0, size), config.transferTimeout()));
        if (!response.successful()) {
            throw S3Errors.failure("write " + key, response);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        ObjectKeys.requireKey(key);
        S3Response response = http.send(new S3Http.Request("GET", config.objectPath(key), "", Map.of(),
                S3Payload.Bytes.EMPTY, config.transferTimeout()));
        if (response.successful()) {
            return Optional.of(response.body());
        }
        return absentOrFail("read " + key, response);
    }

    @Override
    public Optional<byte[]> getRange(String key, long offset, int length) {
        ObjectKeys.requireKey(key);
        if (offset < 0 || length <= 0) {
            throw new IllegalArgumentException("a range needs a non-negative offset and a positive length");
        }
        S3Response response = http.send(new S3Http.Request("GET", config.objectPath(key), "",
                Map.of("range", "bytes=" + offset + "-" + (offset + length - 1)), S3Payload.Bytes.EMPTY,
                config.transferTimeout()));
        if (response.status() == PARTIAL_CONTENT) {
            return Optional.of(response.body());
        }
        if (response.successful()) {
            return Optional.of(slice(response.body(), offset, length));
        }
        if (response.status() == RANGE_NOT_SATISFIABLE) {
            throw new FatalStoreException("range starts at or past the end of " + key, RANGE_NOT_SATISFIABLE);
        }
        return absentOrFail("read a range of " + key, response);
    }

    @Override
    public Optional<ObjectInfo> head(String key) {
        ObjectKeys.requireKey(key);
        S3Response response = http.send(new S3Http.Request("HEAD", config.objectPath(key), "", Map.of(),
                S3Payload.Bytes.EMPTY, config.requestTimeout()));
        if (response.successful()) {
            return Optional.of(S3Metadata.info(key, response));
        }
        if (response.status() == NOT_FOUND) {
            return Optional.empty();
        }
        throw S3Errors.failure("inspect " + key, response);
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) {
        ObjectKeys.requirePrefix(prefix);
        ObjectKeys.requirePrefix(startAfter);
        if (maxKeys < 1 || maxKeys > MAX_LIST_KEYS) {
            throw new IllegalArgumentException("maxKeys must be between 1 and " + MAX_LIST_KEYS);
        }
        StringBuilder query = new StringBuilder("encoding-type=url&list-type=2&max-keys=").append(maxKeys)
                .append("&prefix=").append(SigV4Signer.encode(config.prefix() + prefix));
        if (!startAfter.isEmpty()) {
            query.append("&start-after=").append(SigV4Signer.encode(config.prefix() + startAfter));
        }
        S3Response response = http.send(new S3Http.Request("GET", config.bucketPath(), query.toString(), Map.of(),
                S3Payload.Bytes.EMPTY, config.requestTimeout()));
        if (!response.successful()) {
            throw S3Errors.failure("list " + prefix, response);
        }
        return parseListing(response.text());
    }

    @Override
    public void delete(String key) {
        ObjectKeys.requireKey(key);
        S3Response response = http.send(new S3Http.Request("DELETE", config.objectPath(key), "", Map.of(),
                S3Payload.Bytes.EMPTY, config.requestTimeout()));
        if (!response.successful() && response.status() != NOT_FOUND) {
            throw S3Errors.failure("delete " + key, response);
        }
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        List<String> pending = new ArrayList<>(keys);
        for (String key : pending) {
            ObjectKeys.requireKey(key);
        }
        for (int from = 0; from < pending.size(); from += MAX_DELETE_BATCH) {
            deleteBatch(pending.subList(from, Math.min(pending.size(), from + MAX_DELETE_BATCH)));
        }
    }

    @Override
    public int abortStaleUploads(String prefix, Duration olderThan, Instant now) {
        ObjectKeys.requirePrefix(prefix);
        return uploader.abortStale(prefix, now.minus(olderThan));
    }

    @Override
    public void close() {
        http.close();
    }

    private void deleteBatch(List<String> batch) {
        StringBuilder body = new StringBuilder("<Delete><Quiet>true</Quiet>");
        for (String key : batch) {
            body.append("<Object><Key>").append(XmlText.escape(config.prefix() + key)).append("</Key></Object>");
        }
        body.append("</Delete>");
        byte[] content = body.toString().getBytes(StandardCharsets.UTF_8);
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("content-md5", md5Base64(content));
        headers.put("content-type", "application/xml");
        S3Response response = http.send(new S3Http.Request("POST", config.bucketPath(), "delete", headers,
                new S3Payload.Bytes(content), config.requestTimeout()));
        if (!response.successful()) {
            throw S3Errors.failure("delete " + batch.size() + " objects", response);
        }
        List<String> failures = XmlText.blocks(response.text(), "Error");
        if (!failures.isEmpty()) {
            throw new TransientStoreException(failures.size() + " of " + batch.size() + " objects were not deleted: "
                    + XmlText.text(failures.get(0), "Code").orElse("unknown error"), response.status());
        }
    }

    private ListPage parseListing(String xml) {
        List<ObjectInfo> entries = new ArrayList<>();
        for (String entry : XmlText.blocks(xml, "Contents")) {
            String rawKey = XmlText.text(entry, "Key").orElse("");
            String full = URLDecoder.decode(rawKey, StandardCharsets.UTF_8);
            if (!full.startsWith(config.prefix())) {
                continue;
            }
            String key = full.substring(config.prefix().length());
            if (!isOurKey(key)) {
                continue;
            }
            long size = XmlText.text(entry, "Size").map(Long::parseLong).orElse(0L);
            long modified = XmlText.text(entry, "LastModified").map(text -> Instant.parse(text).toEpochMilli())
                    .orElse(0L);
            entries.add(ObjectInfo.listing(key, size, modified));
        }
        return new ListPage(entries, "true".equals(XmlText.text(xml, "IsTruncated").orElse("false")));
    }

    private static boolean isOurKey(String key) {
        try {
            ObjectKeys.requireKey(key);
            return true;
        } catch (IllegalArgumentException foreign) {
            return false;
        }
    }

    private static Optional<byte[]> absentOrFail(String operation, S3Response response) {
        if (response.status() == NOT_FOUND && !NO_SUCH_BUCKET.equals(S3Errors.code(response))) {
            return Optional.empty();
        }
        throw S3Errors.failure(operation, response);
    }

    private static byte[] slice(byte[] whole, long offset, int length) {
        if (offset >= whole.length) {
            throw new FatalStoreException("range starts at or past the end of the object", RANGE_NOT_SATISFIABLE);
        }
        return Arrays.copyOfRange(whole, (int) offset, (int) Math.min(whole.length, offset + length));
    }

    private static String md5Base64(byte[] content) {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 is unavailable", e);
        }
    }

    private static void validate(String key, Map<String, String> metadata) {
        ObjectKeys.requireKey(key);
        ObjectKeys.requireMetadata(metadata);
    }
}
