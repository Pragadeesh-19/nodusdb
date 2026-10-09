package io.nodusdb.config;

import io.nodusdb.config.StoreConfig.CredentialSource;
import io.nodusdb.config.StoreConfig.Store;
import io.nodusdb.objectstore.DirectoryObjectStore;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.s3.Credentials;
import io.nodusdb.objectstore.s3.CredentialsProvider;
import io.nodusdb.objectstore.s3.EnvironmentCredentialsProvider;
import io.nodusdb.objectstore.s3.FileCredentialsProvider;
import io.nodusdb.objectstore.s3.S3Config;
import io.nodusdb.objectstore.s3.S3ObjectStore;
import io.nodusdb.objectstore.s3.StaticCredentialsProvider;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

public final class StoreFactory {

    public record Opened(ObjectStore store, String icebergLocation) {
    }

    private static final String ICEBERG_FOLDER = "iceberg";

    private StoreFactory() {
    }

    public static Opened open(StoreConfig config) {
        Store spec = config.store();
        return switch (spec.type()) {
            case DIRECTORY -> directory(spec);
            case S3 -> s3(config);
        };
    }

    private static Opened directory(Store spec) {
        String root = spec.directory().toAbsolutePath().toString().replace('\\', '/');
        return new Opened(new DirectoryObjectStore(spec.directory()), trimTrailingSlash(root) + "/" + ICEBERG_FOLDER);
    }

    private static Opened s3(StoreConfig config) {
        S3Config s3 = s3Config(config.store(), config.requestTimeout());
        return new Opened(new S3ObjectStore(s3, provider(config.credentials(), System::getenv)),
                "s3://" + s3.bucket() + "/" + s3.prefix() + ICEBERG_FOLDER);
    }

    static S3Config s3Config(Store spec, Duration requestTimeout) {
        String region = S3Config.requireRegion(spec.region());
        URI endpoint = spec.endpoint() != null ? spec.endpoint()
                : URI.create("https://s3." + region + ".amazonaws.com");
        S3Config s3 = S3Config.of(endpoint, region, spec.bucket()).withPrefix(spec.prefix());
        if (spec.pathStyle() != null) {
            s3 = s3.withPathStyle(spec.pathStyle());
        }
        if (spec.caBundle() != null) {
            s3 = s3.withCaBundle(spec.caBundle());
        }
        return s3.withTimeouts(S3Config.DEFAULT_CONNECT_TIMEOUT, requestTimeout, S3Config.DEFAULT_TRANSFER_TIMEOUT);
    }

    static CredentialsProvider provider(CredentialSource source, Supplier<Map<String, String>> environment) {
        return switch (source.kind()) {
            case STATIC -> new StaticCredentialsProvider(new Credentials(source.accessKeyId(),
                    source.secretAccessKey(), source.sessionToken()));
            case ENVIRONMENT -> new EnvironmentCredentialsProvider(environment);
            case FILE -> new FileCredentialsProvider(source.file(), source.profile());
        };
    }

    private static String trimTrailingSlash(String path) {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }
}
