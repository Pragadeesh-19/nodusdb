package io.nodusdb.objectstore;

public final class TransientStoreException extends ObjectStoreException {

    private static final long serialVersionUID = 1L;

    public TransientStoreException(String message, int status) {
        super(message, status);
    }

    public TransientStoreException(String message, int status, Throwable cause) {
        super(message, status, cause);
    }
}
