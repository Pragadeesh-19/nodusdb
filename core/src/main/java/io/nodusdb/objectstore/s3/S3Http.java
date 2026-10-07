package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.TransientStoreException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class S3Http implements AutoCloseable {

    record Request(String method, String path, String query, Map<String, String> headers, S3Payload payload,
                   Duration timeout) {

        Request {
            headers = Map.copyOf(headers);
        }
    }

    private final S3Config config;
    private final CredentialsProvider credentials;
    private final SigV4Signer signer;
    private final Clock clock;
    private final HttpClient client;

    S3Http(S3Config config, CredentialsProvider credentials, Clock clock) {
        this.config = config;
        this.credentials = credentials;
        this.clock = clock;
        this.signer = new SigV4Signer(config.region(), "s3");
        HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(config.connectTimeout());
        if (config.caBundle() != null) {
            builder.sslContext(TrustStores.fromBundle(config.caBundle()));
        }
        this.client = builder.build();
    }

    S3Response send(Request request) {
        String payloadHash = request.payload().sha256Hex();
        Credentials used = credentials.current();
        S3Response response = exchange(request, used, payloadHash);
        if (S3Errors.refreshable(response)) {
            Credentials refreshed = credentials.refresh();
            if (!refreshed.equals(used)) {
                response = exchange(request, refreshed, payloadHash);
            }
        }
        return response;
    }

    @Override
    public void close() {
        client.close();
    }

    private S3Response exchange(Request request, Credentials used, String payloadHash) {
        SigV4Signer.Signed signed = signer.sign(request.method(), config.hostHeader(), request.path(),
                request.query(), request.headers(), payloadHash, used, clock.instant());
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(config.url(request.path(), request.query())))
                .timeout(request.timeout())
                .method(request.method(), request.payload().publisher());
        request.headers().forEach(builder::header);
        signed.headers().forEach(builder::header);
        try {
            HttpResponse<byte[]> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            return new S3Response(response.statusCode(), lowerCased(response), response.body());
        } catch (IOException e) {
            throw new TransientStoreException(request.method() + " " + request.path() + " did not complete: "
                    + e.getClass().getSimpleName(), 0, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStoreException(request.method() + " " + request.path() + " was interrupted", 0, e);
        }
    }

    private static Map<String, String> lowerCased(HttpResponse<byte[]> response) {
        Map<String, String> headers = new LinkedHashMap<>();
        response.headers().map().forEach((name, values) -> {
            if (!values.isEmpty()) {
                headers.put(name.toLowerCase(Locale.ROOT), values.get(0));
            }
        });
        return headers;
    }
}
