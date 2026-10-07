package io.nodusdb.error;

public abstract class NodusException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final ErrorCode code;

    protected NodusException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    protected NodusException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
