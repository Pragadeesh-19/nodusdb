package io.nodusdb.objectstore.s3;

import java.util.Objects;

public final class StaticCredentialsProvider implements CredentialsProvider {

    private final Credentials credentials;

    public StaticCredentialsProvider(Credentials credentials) {
        this.credentials = Objects.requireNonNull(credentials, "credentials");
    }

    @Override
    public Credentials current() {
        return credentials;
    }

    @Override
    public Credentials refresh() {
        return credentials;
    }
}
