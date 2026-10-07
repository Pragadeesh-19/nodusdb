package io.nodusdb.chain;

import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Objects;

public final class ChainVerifier {

    private final Keyring keyring;

    public ChainVerifier(Keyring keyring) {
        this.keyring = Objects.requireNonNull(keyring, "keyring");
    }

    public void verify(ChainObject object) {
        verifySignature(object);
        if (object.body() instanceof ChainBody.Records records) {
            ChainRecords.verify(records);
        }
    }

    public void verifySignature(ChainObject object) {
        PublicKey key = keyring.find(object.header().keyId()).orElseThrow(() -> new ChainTrustException(
                "no trusted key has the id " + object.header().keyId()));
        try {
            Signature verifier = Signature.getInstance(ChainSignature.ALGORITHM);
            verifier.initVerify(key);
            verifier.update(ChainSignature.message(object.digest()));
            if (!verifier.verify(object.signature())) {
                throw new ChainTrustException("the signature of object " + object.seq() + " does not verify");
            }
        } catch (GeneralSecurityException e) {
            throw new ChainTrustException("the signature of object " + object.seq() + " does not verify");
        }
    }
}
