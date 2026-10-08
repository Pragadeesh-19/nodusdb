package io.nodusdb.projection;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainRecords;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.ChainVerifier;
import io.nodusdb.chain.Keyring;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectInfo;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainRing;

import java.util.Optional;
import java.util.OptionalLong;

public final class StoreChainSource implements ChainSource {

    private static final int PAGE_KEYS = 100;

    private final ChainRing ring;
    private final ObjectStore store;
    private final Keyring keyring;

    public StoreChainSource(ChainRing ring, ObjectStore store, Keyring keyring) {
        this.ring = ring;
        this.store = store;
        this.keyring = keyring;
    }

    @Override
    public Optional<ChainObject> fetch(long seq) {
        Optional<ChainObject> cached = ring.get(seq);
        if (cached.isPresent()) {
            return cached;
        }
        Optional<byte[]> bytes = store.get(ChainLayout.chainKey(seq));
        if (bytes.isEmpty()) {
            return Optional.empty();
        }
        ChainObject object = ChainCodec.decode(bytes.get());
        if (object.seq() != seq) {
            throw new ChainTrustException("object " + seq + " carries sequence number " + object.seq());
        }
        if (keyring.find(object.header().keyId()).isPresent()) {
            new ChainVerifier(keyring).verifySignature(object);
        }
        if (object.body() instanceof ChainBody.Records records) {
            ChainRecords.verify(records);
        }
        return Optional.of(object);
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
