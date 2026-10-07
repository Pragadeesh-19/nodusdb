package io.nodusdb.error;

public final class StaleReadException extends NodusException {

    private static final long serialVersionUID = 1L;

    public StaleReadException(String message) {
        super(ErrorCode.STALE_READ, message);
    }
}
