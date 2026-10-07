package io.nodusdb.chain;

import java.nio.charset.StandardCharsets;

final class ChainSignature {

    static final int BYTES = 64;
    static final String ALGORITHM = "Ed25519";

    private static final byte[] DOMAIN = "nodus.chain.v1\0".getBytes(StandardCharsets.US_ASCII);

    private ChainSignature() {
    }

    static byte[] message(ChainHash digest) {
        byte[] message = new byte[DOMAIN.length + ChainHash.BYTES];
        System.arraycopy(DOMAIN, 0, message, 0, DOMAIN.length);
        System.arraycopy(digest.toBytes(), 0, message, DOMAIN.length, ChainHash.BYTES);
        return message;
    }
}
