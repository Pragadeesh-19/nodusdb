package io.nodusdb.error;

public final class UnsupportedFeatureException extends NodusException {

    private static final long serialVersionUID = 1L;

    public UnsupportedFeatureException(String message) {
        super(ErrorCode.UNSUPPORTED, message);
    }
}
