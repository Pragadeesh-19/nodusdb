package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainRecords;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.ChainVerifier;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.ObjectStore;

import java.util.Objects;
import java.util.Optional;

public final class ChainFetch {

    public enum Trust {
        WHEN_KEY_KNOWN,
        REQUIRED
    }

    private final ObjectStore store;
    private final Keyring keyring;
    private final Trust trust;
    private final ChainVerifier verifier;

    public ChainFetch(ObjectStore store, Keyring keyring, Trust trust) {
        this.store = Objects.requireNonNull(store, "store");
        this.keyring = Objects.requireNonNull(keyring, "keyring");
        this.trust = Objects.requireNonNull(trust, "trust");
        this.verifier = new ChainVerifier(keyring);
    }

    public Optional<ChainObject> fetch(long seq) {
        Optional<byte[]> bytes = store.get(ChainLayout.chainKey(seq));
        if (bytes.isEmpty()) {
            return Optional.empty();
        }
        ChainObject object = ChainCodec.decode(bytes.get());
        if (object.seq() != seq) {
            throw new ChainTrustException("object " + seq + " carries sequence number " + object.seq());
        }
        if (trust == Trust.REQUIRED || keyring.find(object.header().keyId()).isPresent()) {
            verifier.verifySignature(object);
        }
        if (object.body() instanceof ChainBody.Records records) {
            ChainRecords.verify(records);
        }
        return Optional.of(object);
    }

    public ChainObject require(long seq) {
        return fetch(seq).orElseThrow(() -> new ChainTrustException(
                "object " + seq + " was listed but cannot be read"));
    }
}
