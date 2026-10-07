package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ObjectInfo;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;

final class S3Metadata {

    static final String HEADER_PREFIX = "x-amz-meta-";

    private S3Metadata() {
    }

    static Map<String, String> headers(Map<String, String> metadata) {
        Map<String, String> headers = new LinkedHashMap<>();
        metadata.forEach((name, value) -> headers.put(HEADER_PREFIX + name, value));
        return headers;
    }

    static ObjectInfo info(String key, S3Response response) {
        Map<String, String> metadata = new LinkedHashMap<>();
        response.headers().forEach((name, value) -> {
            if (name.startsWith(HEADER_PREFIX) && name.length() > HEADER_PREFIX.length()) {
                metadata.put(name.substring(HEADER_PREFIX.length()), value);
            }
        });
        return new ObjectInfo(key, size(response), lastModifiedMillis(response), metadata);
    }

    private static long size(S3Response response) {
        String length = response.header("content-length");
        try {
            return length == null ? 0 : Long.parseLong(length.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static long lastModifiedMillis(S3Response response) {
        String modified = response.header("last-modified");
        if (modified == null) {
            return 0;
        }
        try {
            return ZonedDateTime.parse(modified, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            return 0;
        }
    }
}
