package io.nodusdb.chain;

import io.nodusdb.error.ErrorCode;
import io.nodusdb.error.NodusException;

public final class ChainFormatException extends NodusException {

    private static final long serialVersionUID = 1L;

    public ChainFormatException(String message) {
        super(ErrorCode.CHAIN_TRUST, message);
    }
}
