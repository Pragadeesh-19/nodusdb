package io.nodusdb.log.record;

public final class MalformedTransactionException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public MalformedTransactionException(String message) {
        super(message);
    }
}
