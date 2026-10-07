package io.nodusdb.error;

public final class CheckDepthException extends NodusException {

    private static final long serialVersionUID = 1L;

    public CheckDepthException(String message) {
        super(ErrorCode.CHECK_DEPTH, message);
    }
}
