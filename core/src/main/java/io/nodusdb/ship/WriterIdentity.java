package io.nodusdb.ship;

import io.nodusdb.chain.SigningKey;

import java.util.Objects;

public record WriterIdentity(SigningKey key, long nonce) {

    public WriterIdentity {
        Objects.requireNonNull(key, "key");
    }
}
