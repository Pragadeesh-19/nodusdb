package io.nodusdb.chain;

import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;

public final class Keyring {

    private static final Keyring EMPTY = new Keyring(Map.of());

    private final Map<Integer, PublicKey> keys;

    private Keyring(Map<Integer, PublicKey> keys) {
        this.keys = keys;
    }

    public static Keyring empty() {
        return EMPTY;
    }

    public static Keyring of(Map<Integer, PublicKey> keys) {
        for (Integer keyId : keys.keySet()) {
            if (keyId < 0) {
                throw new IllegalArgumentException("a key id must not be negative: " + keyId);
            }
        }
        return new Keyring(Map.copyOf(keys));
    }

    public static Keyring single(int keyId, PublicKey key) {
        return of(Map.of(keyId, key));
    }

    public Optional<PublicKey> find(int keyId) {
        return Optional.ofNullable(keys.get(keyId));
    }

    public int size() {
        return keys.size();
    }
}
