package io.nodusdb.error;

public final class UpgradeFailedException extends NodusException {

    private static final long serialVersionUID = 1L;

    public UpgradeFailedException(String message) {
        super(ErrorCode.FAILURE, message);
    }

    public UpgradeFailedException(String message, Throwable cause) {
        super(ErrorCode.FAILURE, message, cause);
    }
}
