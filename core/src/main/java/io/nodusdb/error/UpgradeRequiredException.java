package io.nodusdb.error;

public final class UpgradeRequiredException extends NodusException {

    private static final long serialVersionUID = 1L;

    public UpgradeRequiredException(String message) {
        super(ErrorCode.UPGRADE_REQUIRED, message);
    }
}
