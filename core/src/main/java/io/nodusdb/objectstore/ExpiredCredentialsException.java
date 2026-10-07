package io.nodusdb.objectstore;

public final class ExpiredCredentialsException extends ObjectStoreException {

    private static final long serialVersionUID = 1L;

    public ExpiredCredentialsException(String message, int status) {
        super(message, status);
    }
}
