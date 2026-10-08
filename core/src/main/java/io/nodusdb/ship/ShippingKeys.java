package io.nodusdb.ship;

import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;
import io.nodusdb.chain.SigningKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;

public final class ShippingKeys {

    private static final String ALGORITHM = "Ed25519";
    private static final byte[] PROBE = "nodusdb signing key check".getBytes(StandardCharsets.US_ASCII);

    public record Loaded(SigningKey signing, Keyring keyring) {
    }

    private ShippingKeys() {
    }

    public static Loaded load(ShippingConfig.Signing config) {
        try {
            PrivateKey privateKey = KeyFiles.readPrivate(config.keyFile());
            SigningKey signing = new SigningKey(config.keyId(), privateKey);
            if (config.publicKeyFile() == null) {
                return new Loaded(signing, Keyring.empty());
            }
            PublicKey publicKey = KeyFiles.readPublic(config.publicKeyFile());
            requireMatching(privateKey, publicKey);
            return new Loaded(signing, Keyring.single(config.keyId(), publicKey));
        } catch (NoSuchFileException missing) {
            throw new IllegalArgumentException("key file not found: " + missing.getFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void requireMatching(PrivateKey privateKey, PublicKey publicKey) {
        try {
            Signature signer = Signature.getInstance(ALGORITHM);
            signer.initSign(privateKey);
            signer.update(PROBE);
            byte[] signature = signer.sign();
            Signature verifier = Signature.getInstance(ALGORITHM);
            verifier.initVerify(publicKey);
            verifier.update(PROBE);
            if (!verifier.verify(signature)) {
                throw new IllegalArgumentException("the public key does not belong to the signing key");
            }
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("the signing and public keys cannot be used together");
        }
    }
}
