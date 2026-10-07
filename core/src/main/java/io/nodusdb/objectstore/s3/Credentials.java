package io.nodusdb.objectstore.s3;

import java.util.Objects;

public record Credentials(String accessKeyId, String secretAccessKey, String sessionToken) {

    public Credentials {
        Objects.requireNonNull(accessKeyId, "accessKeyId");
        Objects.requireNonNull(secretAccessKey, "secretAccessKey");
        if (accessKeyId.isEmpty() || secretAccessKey.isEmpty()) {
            throw new IllegalArgumentException("credentials need an access key id and a secret access key");
        }
        if (sessionToken != null && sessionToken.isEmpty()) {
            throw new IllegalArgumentException("a session token must not be empty");
        }
    }

    public Credentials(String accessKeyId, String secretAccessKey) {
        this(accessKeyId, secretAccessKey, null);
    }

    public boolean hasSessionToken() {
        return sessionToken != null;
    }

    @Override
    public String toString() {
        return "Credentials[accessKeyId=" + accessKeyId + ", secretAccessKey=<redacted>, sessionToken="
                + (sessionToken == null ? "none" : "<redacted>") + "]";
    }
}
