package io.nodusdb.objectstore.s3;

import java.nio.charset.StandardCharsets;
import java.util.Map;

record S3Response(int status, Map<String, String> headers, byte[] body) {

    S3Response {
        headers = Map.copyOf(headers);
    }

    boolean successful() {
        return status >= 200 && status < 300;
    }

    String header(String lowerCaseName) {
        return headers.get(lowerCaseName);
    }

    String text() {
        return new String(body, StandardCharsets.UTF_8);
    }
}
