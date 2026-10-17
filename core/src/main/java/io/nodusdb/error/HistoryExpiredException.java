package io.nodusdb.error;

public final class HistoryExpiredException extends NodusException {

    private static final long serialVersionUID = 1L;

    public HistoryExpiredException(String message) {
        super(ErrorCode.HISTORY_EXPIRED, message);
    }
}
