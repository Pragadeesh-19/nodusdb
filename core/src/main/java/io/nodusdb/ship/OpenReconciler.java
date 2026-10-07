package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.Keyring;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.objectstore.ConditionalWriteProbe;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.UnsupportedStoreException;

import java.util.Optional;
import java.util.function.LongSupplier;

public final class OpenReconciler {

    public record Local(long lastLsn, long latestEpoch) {
    }

    public record Start(long epoch, ChainCursor cursor, ChainBody.SnapshotRef firstReference) {

        public boolean startsChain() {
            return firstReference != null;
        }

        public long shippedLsn() {
            return startsChain() ? firstReference.lsn() : cursor.lastLsn();
        }
    }

    private final ObjectStore store;
    private final WriterIdentity identity;
    private final Keyring keyring;
    private final ShipSettings settings;
    private final SnapshotPublisher publisher;
    private final LongSupplier clockMicros;

    public OpenReconciler(ObjectStore store, WriterIdentity identity, Keyring keyring, ShipSettings settings,
                          SnapshotPublisher publisher, LongSupplier clockMicros) {
        this.store = store;
        this.identity = identity;
        this.keyring = keyring;
        this.settings = settings;
        this.publisher = publisher;
        this.clockMicros = clockMicros;
    }

    public Start reconcile(Local local) {
        requireConditionalWrites();
        Optional<ChainHead.Found> head = new ChainHead(store, keyring).find();
        EpochClaims claims = new EpochClaims(store, identity.nonce(), identity.key().keyId(), clockMicros);
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
        return new Start(epoch, ChainCursor.beforeFirst(), reference);
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
        return new Start(epoch, head, null);
    }
}
