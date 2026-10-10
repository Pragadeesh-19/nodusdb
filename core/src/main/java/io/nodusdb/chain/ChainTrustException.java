package io.nodusdb.chain;

import io.nodusdb.error.ErrorCode;
import io.nodusdb.error.NodusException;

public final class ChainTrustException extends NodusException {

    private static final long serialVersionUID = 1L;

    public ChainTrustException(String message) {
        super(ErrorCode.CHAIN_TRUST, message);
    }
}
