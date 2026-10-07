package io.nodusdb.chain;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Objects;

public final class SigningKey {

    private final int keyId;
    private final PrivateKey key;

    public SigningKey(int keyId, PrivateKey key) {
        if (keyId < 0) {
            throw new IllegalArgumentException("a key id must not be negative: " + keyId);
        }
        this.keyId = keyId;
        this.key = Objects.requireNonNull(key, "key");
    }

    public int keyId() {
        return keyId;
    }

    byte[] sign(ChainHash digest) {
        try {
            Signature signer = Signature.getInstance(ChainSignature.ALGORITHM);
            signer.initSign(key);
            signer.update(ChainSignature.message(digest));
            return signer.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("the signing key cannot sign", e);
        }
    }

    @Override
    public String toString() {
        return "SigningKey[keyId=" + keyId + "]";
    }
}
