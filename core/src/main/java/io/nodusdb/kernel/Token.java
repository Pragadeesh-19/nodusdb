package io.nodusdb.kernel;

public record Token(long epoch, long lsn) {

    public Token {
        if (epoch < 0 || lsn < 0) {
            throw new IllegalArgumentException("a token has a non-negative epoch and LSN: " + epoch + ", " + lsn);
        }
    }
}
