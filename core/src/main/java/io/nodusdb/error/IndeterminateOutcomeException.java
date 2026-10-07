package io.nodusdb.error;

public final class IndeterminateOutcomeException extends NodusException {

    private static final long serialVersionUID = 1L;

    public IndeterminateOutcomeException(String message) {
        super(ErrorCode.INDETERMINATE, message);
    }

    public IndeterminateOutcomeException(String message, Throwable cause) {
        super(ErrorCode.INDETERMINATE, message, cause);
    }
}
