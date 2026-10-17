package io.nodusdb.log;

public final class RecordsGoneException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RecordsGoneException(String message) {
        super(message);
    }

    public RecordsGoneException(String message, Throwable cause) {
        super(message, cause);
    }
}
