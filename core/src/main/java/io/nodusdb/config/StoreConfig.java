package io.nodusdb.config;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

public record StoreConfig(Store store, CredentialSource credentials, Duration requestTimeout) {

    public enum StoreType {
        DIRECTORY, S3
    }

    public enum CredentialKind {
        STATIC, ENVIRONMENT, FILE
    }

    public record Store(StoreType type, Path directory, String bucket, String prefix, String region, URI endpoint,
                        Boolean pathStyle, Path caBundle) {
    }

    public record CredentialSource(CredentialKind kind, String accessKeyId, String secretAccessKey,
                                   String sessionToken, Path file, String profile) {

        @Override
        public String toString() {
            return "CredentialSource[kind=" + kind + "]";
        }
    }

    public StoreConfig withRequestTimeout(Duration timeout) {
        return new StoreConfig(store, credentials, timeout);
    }
}
