package io.nodusdb.replica;

import io.nodusdb.chain.ChainHash;

import java.util.Objects;

public record MarkerPosition(long seq, ChainHash digest, long epoch, long lsn) {

    public MarkerPosition {
        Objects.requireNonNull(digest, "digest");
        if (seq < 1 || epoch < 0 || lsn < 0) {
            throw new IllegalArgumentException("a marker position needs seq >= 1, epoch >= 0 and lsn >= 0: "
                    + seq + ", " + epoch + ", " + lsn);
        }
    }
}
