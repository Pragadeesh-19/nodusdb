package io.nodusdb.objectstore;

public final class UnsupportedStoreException extends ObjectStoreException {

    private static final long serialVersionUID = 1L;

    public UnsupportedStoreException(String message) {
        super(message, 0);
    }
}
