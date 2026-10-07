package io.nodusdb.error;

public final class WriterFencedException extends NodusException {

    private static final long serialVersionUID = 1L;

    public WriterFencedException(String message) {
        super(ErrorCode.WRITER_FENCED, message);
    }

    public WriterFencedException(String message, Throwable cause) {
        super(ErrorCode.WRITER_FENCED, message, cause);
    }
}
