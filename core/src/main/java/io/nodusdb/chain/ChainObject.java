package io.nodusdb.chain;

import java.util.Objects;

public record ChainObject(ChainHeader header, ChainBody body, ChainHash digest, byte[] signature, byte[] encoded) {

    public ChainObject {
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(digest, "digest");
        Objects.requireNonNull(signature, "signature");
        Objects.requireNonNull(encoded, "encoded");
        if (header.kind() != body.kind()) {
            throw new IllegalArgumentException("the header says " + header.kind() + " but the body is "
                    + body.kind());
        }
    }

    public long seq() {
        return header.seq();
    }

    public long epoch() {
        return header.epoch();
    }
}
