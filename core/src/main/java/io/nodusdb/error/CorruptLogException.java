package io.nodusdb.error;

public final class CorruptLogException extends NodusException {

    private static final long serialVersionUID = 1L;

    public CorruptLogException(String message) {
        super(ErrorCode.CORRUPT_LOG, message);
    }

    public CorruptLogException(String message, Throwable cause) {
        super(ErrorCode.CORRUPT_LOG, message, cause);
    }
}
