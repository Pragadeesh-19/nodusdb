package io.nodusdb.objectstore;

public abstract class ObjectStoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int status;

    protected ObjectStoreException(String message, int status) {
        super(message);
        this.status = status;
    }

    protected ObjectStoreException(String message, int status, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public int status() {
        return status;
    }
}
