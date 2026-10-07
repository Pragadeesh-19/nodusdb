package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.FatalStoreException;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.TransientStoreException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;

final class MultipartUploader {

    static final long DEFAULT_PART_BYTES = 64L << 20;
    static final int MAX_PARTS = 10_000;
    static final int ATTEMPTS = 5;

    private static final long MEBIBYTE = 1L << 20;
    private static final long BACKOFF_BASE_NANOS = 100_000_000L;
    private static final String NO_SUCH_UPLOAD = "NoSuchUpload";

    private final S3Config config;
    private final S3Http http;
    private final long partBytes;
    private final IntConsumer pause;

    MultipartUploader(S3Config config, S3Http http, long partBytes, IntConsumer pause) {
        if (partBytes < 1) {
            throw new IllegalArgumentException("a part must hold at least one byte");
        }
        this.config = config;
        this.http = http;
        this.partBytes = partBytes;
        this.pause = pause;
    }

    static IntConsumer exponentialBackoff() {
        return attempt -> LockSupport.parkNanos(BACKOFF_BASE_NANOS << Math.min(attempt - 1, 6));
    }

    void upload(String key, Path file, long size, Map<String, String> objectMetadata) {
        long effective = effectivePartBytes(partBytes, size);
        String uploadId = create(key, objectMetadata);
        try {
            List<String> etags = new ArrayList<>();
            long parts = (size + effective - 1) / effective;
            for (int number = 1; number <= parts; number++) {
                long offset = (number - 1L) * effective;
                long length = Math.min(effective, size - offset);
                etags.add(uploadPart(key, uploadId, number, file, offset, length));
            }
            complete(key, uploadId, etags, size);
        } catch (RuntimeException failure) {
            abortQuietly(key, uploadId, failure);
            throw failure;
        }
    }

    int abortStale(String prefix, Instant cutoff) {
        int aborted = 0;
        String keyMarker = "";
        String uploadMarker = "";
        while (true) {
            S3Response response = http.send(new S3Http.Request("GET", config.bucketPath(),
                    listQuery(prefix, keyMarker, uploadMarker), Map.of(), S3Payload.Bytes.EMPTY,
                    config.requestTimeout()));
            if (!response.successful()) {
                throw S3Errors.failure("list multipart uploads", response);
            }
            String xml = response.text();
            for (String upload : XmlText.blocks(xml, "Upload")) {
                aborted += abortIfStale(upload, cutoff);
            }
            if (!"true".equals(XmlText.text(xml, "IsTruncated").orElse("false"))) {
                return aborted;
            }
            keyMarker = XmlText.text(xml, "NextKeyMarker").orElse("");
            uploadMarker = XmlText.text(xml, "NextUploadIdMarker").orElse("");
            if (keyMarker.isEmpty() && uploadMarker.isEmpty()) {
                return aborted;
            }
        }
    }

    private int abortIfStale(String upload, Instant cutoff) {
        String rawKey = XmlText.text(upload, "Key").orElse("");
        String uploadId = XmlText.text(upload, "UploadId").orElse("");
        Optional<Instant> initiated = XmlText.text(upload, "Initiated").map(Instant::parse);
        if (rawKey.isEmpty() || uploadId.isEmpty() || initiated.isEmpty() || !initiated.get().isBefore(cutoff)) {
            return 0;
        }
        String key = URLDecoder.decode(rawKey, StandardCharsets.UTF_8);
        if (!key.startsWith(config.prefix())) {
            return 0;
        }
        abort(key.substring(config.prefix().length()), uploadId);
        return 1;
    }

    private String listQuery(String prefix, String keyMarker, String uploadMarker) {
        StringBuilder query = new StringBuilder("encoding-type=url");
        if (!keyMarker.isEmpty()) {
            query.append("&key-marker=").append(SigV4Signer.encode(keyMarker));
        }
        query.append("&prefix=").append(SigV4Signer.encode(config.prefix() + prefix)).append("&uploads");
        if (!uploadMarker.isEmpty()) {
            query.append("&upload-id-marker=").append(SigV4Signer.encode(uploadMarker));
        }
        return query.toString();
    }

    static long effectivePartBytes(long partBytes, long size) {
        if ((size + partBytes - 1) / partBytes <= MAX_PARTS) {
            return partBytes;
        }
        long needed = (size + MAX_PARTS - 1) / MAX_PARTS;
        return ((needed + MEBIBYTE - 1) / MEBIBYTE) * MEBIBYTE;
    }

    private String create(String key, Map<String, String> objectMetadata) {
        S3Response response = http.send(new S3Http.Request("POST", config.objectPath(key), "uploads",
                S3Metadata.headers(objectMetadata), S3Payload.Bytes.EMPTY, config.requestTimeout()));
        if (!response.successful()) {
            throw S3Errors.failure("start the multipart upload of " + key, response);
        }
        return XmlText.text(response.text(), "UploadId").filter(id -> !id.isEmpty()).orElseThrow(
                () -> new FatalStoreException("the store did not return an upload id for " + key, response.status()));
    }

    private String uploadPart(String key, String uploadId, int number, Path file, long offset, long length) {
        String query = "partNumber=" + number + "&uploadId=" + SigV4Signer.encode(uploadId);
        S3Http.Request request = new S3Http.Request("PUT", config.objectPath(key), query, Map.of(),
                new S3Payload.FileSlice(file, offset, length), config.transferTimeout());
        for (int attempt = 1; ; attempt++) {
            try {
                S3Response response = http.send(request);
                if (!response.successful()) {
                    throw S3Errors.failure("upload part " + number + " of " + key, response);
                }
                String etag = response.header("etag");
                if (etag == null || etag.isEmpty()) {
                    throw new TransientStoreException("part " + number + " of " + key + " returned no ETag",
                            response.status());
                }
                return etag;
            } catch (TransientStoreException failure) {
                if (attempt == ATTEMPTS) {
                    throw failure;
                }
                pause.accept(attempt);
            }
        }
    }

    private void complete(String key, String uploadId, List<String> etags, long size) {
        StringBuilder body = new StringBuilder("<CompleteMultipartUpload>");
        for (int i = 0; i < etags.size(); i++) {
            body.append("<Part><PartNumber>").append(i + 1).append("</PartNumber><ETag>")
                    .append(XmlText.escape(etags.get(i))).append("</ETag></Part>");
        }
        body.append("</CompleteMultipartUpload>");
        S3Http.Request request = new S3Http.Request("POST", config.objectPath(key),
                "uploadId=" + SigV4Signer.encode(uploadId), Map.of("content-type", "application/xml"),
                new S3Payload.Bytes(body.toString().getBytes(StandardCharsets.UTF_8)), config.requestTimeout());
        for (int attempt = 1; ; attempt++) {
            try {
                S3Response response = http.send(request);
                if (response.status() == 404 && NO_SUCH_UPLOAD.equals(S3Errors.code(response))) {
                    requireCompleted(key, size);
                    return;
                }
                if (!response.successful()) {
                    throw S3Errors.failure("complete the multipart upload of " + key, response);
                }
                if (!XmlText.blocks(response.text(), "Error").isEmpty()) {
                    throw new TransientStoreException("completing the multipart upload of " + key
                            + " reported " + XmlText.text(response.text(), "Code").orElse("an error"),
                            response.status());
                }
                return;
            } catch (TransientStoreException failure) {
                if (attempt == ATTEMPTS) {
                    throw failure;
                }
                pause.accept(attempt);
            }
        }
    }

    private void requireCompleted(String key, long size) {
        S3Response response = http.send(new S3Http.Request("HEAD", config.objectPath(key), "", Map.of(),
                S3Payload.Bytes.EMPTY, config.requestTimeout()));
        Optional<ObjectInfo> info = response.successful()
                ? Optional.of(S3Metadata.info(key, response)) : Optional.empty();
        if (info.isEmpty() || info.get().size() != size) {
            throw new FatalStoreException("the multipart upload of " + key + " is gone and the object is missing "
                    + "or has another size", response.status());
        }
    }

    private void abort(String key, String uploadId) {
        S3Response response = http.send(new S3Http.Request("DELETE", config.objectPath(key),
                "uploadId=" + SigV4Signer.encode(uploadId), Map.of(), S3Payload.Bytes.EMPTY,
                config.requestTimeout()));
        if (!response.successful() && response.status() != 404) {
            throw S3Errors.failure("abort the multipart upload of " + key, response);
        }
    }

    private void abortQuietly(String key, String uploadId, RuntimeException original) {
        try {
            abort(key, uploadId);
        } catch (ObjectStoreException secondary) {
            original.addSuppressed(secondary);
        }
    }
}
