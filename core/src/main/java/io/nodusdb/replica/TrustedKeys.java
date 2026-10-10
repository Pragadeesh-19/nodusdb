package io.nodusdb.replica;

import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;

final class TrustedKeys {

    private TrustedKeys() {
    }

    static Keyring load(FollowerConfig.Trust trust) {
        try {
            return Keyring.single(trust.keyId(), KeyFiles.readPublic(trust.publicKeyFile()));
        } catch (NoSuchFileException missing) {
            throw new IllegalArgumentException("key file not found: " + missing.getFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
