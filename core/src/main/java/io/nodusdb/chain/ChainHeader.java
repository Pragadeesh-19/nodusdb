package io.nodusdb.chain;

import java.util.Objects;

public record ChainHeader(ChainKind kind, long seq, long epoch, long writerNonce, ChainHash prev, int keyId) {

    public ChainHeader {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(prev, "prev");
        if (seq < 1) {
            throw new IllegalArgumentException("a sequence number starts at 1: " + seq);
        }
        if (epoch < 0) {
            throw new IllegalArgumentException("an epoch must not be negative: " + epoch);
        }
    }
}
