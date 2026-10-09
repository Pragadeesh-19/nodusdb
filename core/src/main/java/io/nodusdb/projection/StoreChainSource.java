package io.nodusdb.projection;

import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainRing;

import java.util.Optional;
import java.util.OptionalLong;

public final class StoreChainSource implements ChainSource {

    private final ChainRing ring;
    private final ChainFetch chainFetch;

    public StoreChainSource(ChainRing ring, ObjectStore store, Keyring keyring) {
        this.ring = ring;
        this.chainFetch = new ChainFetch(store, keyring, ChainFetch.Trust.WHEN_KEY_KNOWN);
    }

    @Override
    public Optional<ChainObject> fetch(long seq) {
        Optional<ChainObject> cached = ring.get(seq);
        if (cached.isPresent()) {
            return cached;
        }
        return chainFetch.fetch(seq);
    }

    @Override
    public OptionalLong oldestSeq() {
        return chainFetch.oldestSeq();
    }
}
