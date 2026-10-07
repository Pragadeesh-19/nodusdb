package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.Keyring;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.objectstore.ConditionalWriteProbe;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.PutResult;
import io.nodusdb.objectstore.UnsupportedStoreException;

import java.util.Optional;
import java.util.function.LongSupplier;

public final class OpenReconciler {

    public record Local(long lastLsn, long latestEpoch) {
    }

    public record Start(long epoch, ChainCursor cursor, long historyGapBeforeLsn) {
    }

    private static final long NO_GAP = 0;

    private final ObjectStore store;
    private final SigningKey key;
    private final Keyring keyring;
    private final long writerNonce;
    private final ShipSettings settings;
    private final SnapshotPublisher publisher;
    private final LongSupplier clockMicros;

    public OpenReconciler(ObjectStore store, SigningKey key, Keyring keyring, long writerNonce,
                          ShipSettings settings, SnapshotPublisher publisher, LongSupplier clockMicros) {
        this.store = store;
        this.key = key;
        this.keyring = keyring;
        this.writerNonce = writerNonce;
        this.settings = settings;
        this.publisher = publisher;
        this.clockMicros = clockMicros;
    }

    public Start reconcile(Local local) {
        requireConditionalWrites();
        Optional<ChainHead.Found> head = new ChainHead(store, keyring).find();
        EpochClaims claims = new EpochClaims(store, writerNonce, key.keyId(), clockMicros);
        if (head.isEmpty()) {
            return startNewChain(local, claims);
        }
        return continueChain(local, claims, head.get().cursor());
    }

    private void requireConditionalWrites() {
        try {
            ConditionalWriteProbe.verify(store);
        } catch (UnsupportedStoreException unsupported) {
            throw new UnsupportedFeatureException("the object store cannot be used for shipping: "
                    + unsupported.getMessage());
        }
    }

    private Start startNewChain(Local local, EpochClaims claims) {
        ChainBody.SnapshotRef reference = publisher.publish(1);
        long epoch = claims.claim(local.latestEpoch() + 1, settings.claimAttempts());
        ChainObject first = ChainCodec.seal(new ChainHeader(ChainKind.SNAPSHOT_REF, 1, epoch, writerNonce,
                ChainHash.ZERO, key.keyId()), reference, key);
        PutResult result = store.putIfAbsent(ChainLayout.chainKey(1), first.encoded());
        if (result != PutResult.CREATED) {
            throw new WriterFencedException("another writer started the chain while this one was opening it");
        }
        return new Start(epoch, ChainCursor.after(1, first.digest(), epoch, reference.lsn()), reference.lsn());
    }

    private Start continueChain(Local local, EpochClaims claims, ChainCursor head) {
        if (local.lastLsn() < head.lastLsn()) {
            throw new WriterFencedException("the shipped chain reaches LSN " + head.lastLsn()
                    + " but this directory's log ends at LSN " + local.lastLsn()
                    + "; it is older than the chain and cannot continue it");
        }
        if (head.epoch() > local.latestEpoch()) {
            throw new WriterFencedException("the chain was last written in epoch " + head.epoch()
                    + " but this directory has only seen epoch " + local.latestEpoch()
                    + "; another writer has continued the chain");
        }
        long epoch = claims.claim(local.latestEpoch() + 1, settings.claimAttempts());
        return new Start(epoch, head, NO_GAP);
    }
}
