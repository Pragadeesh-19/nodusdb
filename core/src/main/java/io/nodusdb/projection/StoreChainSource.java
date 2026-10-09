package io.nodusdb.projection;

import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainRing;

import java.util.Optional;
import java.util.OptionalLong;

public final class StoreChainSource implements ChainSource {

    private static final int PAGE_KEYS = 100;

    private final ChainRing ring;
    private final ObjectStore store;
    private final ChainFetch chainFetch;

    public StoreChainSource(ChainRing ring, ObjectStore store, Keyring keyring) {
        this.ring = ring;
        this.store = store;
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
        ListPage page = store.list(ChainLayout.CHAIN_PREFIX, "", PAGE_KEYS);
        for (ObjectInfo entry : page.entries()) {
            OptionalLong seq = ChainLayout.chainSeq(entry.key());
            if (seq.isPresent()) {
                return seq;
            }
        }
        return OptionalLong.empty();
    }
}
