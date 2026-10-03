package io.nodusdb.kernel;

public final class OutputBufferTooSmallException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public OutputBufferTooSmallException(String message) {
        super(message);
    }
}
