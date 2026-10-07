package io.nodusdb.error;

public final class LogBacklogException extends NodusException {

    private static final long serialVersionUID = 1L;

    public LogBacklogException(String message) {
        super(ErrorCode.LOG_BACKLOG, message);
    }
}
