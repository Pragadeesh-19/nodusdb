package io.nodusdb.ship;

import io.nodusdb.objectstore.ObjectStore;

import java.security.SecureRandom;
import java.util.function.LongSupplier;

public final class ShippingBootstrap {

    public record Reconciled(ShippingConfig config, ObjectStore store, String icebergLocation,
                             ShippingKeys.Loaded keys, WriterIdentity identity, OpenReconciler.Start start,
                             long newestSnapshotLsn) {
    }

    private static final SecureRandom RANDOM = new SecureRandom();

    private ShippingBootstrap() {
    }

    public static Reconciled reconcile(ShippingConfig config, OpenReconciler.Local local, SnapshotSource source,
                                       LongSupplier clockMicros) {
        return reconcile(config, local, source, clockMicros, RANDOM::nextLong);
    }

    static Reconciled reconcile(ShippingConfig config, OpenReconciler.Local local, SnapshotSource source,
                                LongSupplier clockMicros, LongSupplier nonce) {
        ShippingKeys.Loaded keys = ShippingKeys.load(config.signing());
        ShippingStores.Opened opened = ShippingStores.open(config);
        ObjectStore store = opened.store();
        try {
            WriterIdentity identity = new WriterIdentity(keys.signing(), nonce.getAsLong());
            SnapshotPublisher publisher = new SnapshotUploadPublisher(source, new SnapshotUploader(store));
            OpenReconciler reconciler = new OpenReconciler(store, identity, keys.keyring(), config.ship(), publisher,
                    clockMicros);
            OpenReconciler.Start start = reconciler.reconcile(local);
            long newest = new ChainHead(store, keys.keyring()).newestSnapshotLsn();
            return new Reconciled(config, store, opened.icebergLocation(), keys, identity, start, newest);
        } catch (RuntimeException failure) {
            closeQuietly(store, failure);
            throw failure;
        }
    }

    private static void closeQuietly(ObjectStore store, RuntimeException cause) {
        try {
            store.close();
        } catch (RuntimeException closeFailure) {
            cause.addSuppressed(closeFailure);
        }
    }
}
