package io.nodusdb.error;

public final class TokenLostException extends NodusException {

    private static final long serialVersionUID = 1L;

    public TokenLostException(String message) {
        super(ErrorCode.TOKEN_LOST, message);
    }
}
