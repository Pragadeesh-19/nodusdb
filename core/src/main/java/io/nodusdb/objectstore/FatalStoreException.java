package io.nodusdb.objectstore;

public final class FatalStoreException extends ObjectStoreException {

    private static final long serialVersionUID = 1L;

    public FatalStoreException(String message, int status) {
        super(message, status);
    }

    public FatalStoreException(String message, int status, Throwable cause) {
        super(message, status, cause);
    }
}
