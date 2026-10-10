package io.nodusdb.replica;

import io.nodusdb.chain.Keyring;
import io.nodusdb.config.StoreFactory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.ship.ChainHead;
import io.nodusdb.ship.EpochClaims;
import io.nodusdb.ship.ShippingConfig;
import io.nodusdb.ship.ShippingKeys;
import io.nodusdb.storage.DurableGraph;

import java.io.IOException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;

public final class WriterTakeover {

    public record Taken(GraphKernel kernel, long claimedEpoch, long epoch, long handoffLsn, int attempts) {
    }

    private static final Duration SHIP_WAIT = Duration.ofSeconds(30);
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;
    private static final SecureRandom RANDOM = new SecureRandom();

    private WriterTakeover() {
    }

    public static Taken takeOver(ShippingConfig shipping, Path directory, LogConfig logConfig, long maxMemoryBytes)
            throws IOException {
        if (shipping.signing().publicKeyFile() == null) {
            throw new IllegalArgumentException("a takeover needs signing.public_key_file to verify the chain it "
                    + "restores from");
        }
        Keyring keyring = ShippingKeys.load(shipping.signing()).keyring();
        ObjectStore store = StoreFactory.open(shipping.storage()).store();
        try {
            return perform(store, keyring, shipping, directory, logConfig, maxMemoryBytes);
        } finally {
            store.close();
        }
    }

    private static Taken perform(ObjectStore store, Keyring keyring, ShippingConfig shipping, Path directory,
                                 LogConfig logConfig, long maxMemoryBytes) throws IOException {
        ChainHead head = new ChainHead(store, keyring, Trust.REQUIRED);
        EpochClaims claims = new EpochClaims(store, RANDOM.nextLong(), shipping.signing().keyId(),
                WriterTakeover::wallClockMicros);
        Restore restore = new Restore(store, keyring, FollowerConfig.DEFAULT_DOWNLOAD_PARALLELISM);
        Takeover<GraphKernel> takeover = new Takeover<>(
                () -> requireChain(head),
                () -> claims.claim(claims.highestClaimed() + 1, shipping.ship().claimAttempts()),
                restore::restoreTo,
                () -> head.find().map(found -> found.cursor().lastLsn()).orElse(0L),
                (opened, claimed, restored) -> openWriter(opened, shipping, logConfig, maxMemoryBytes));
        Takeover.Result<GraphKernel> result = takeover.run(directory);
        GraphKernel kernel = result.writer();
        return new Taken(kernel, result.claimedEpoch(), kernel.epoch(), result.restored().appliedLsn(),
                result.attempts());
    }

    private static void requireChain(ChainHead head) {
        if (head.find().isEmpty()) {
            throw new IllegalStateException("the object store holds no chain to take over");
        }
    }

    private static GraphKernel openWriter(Path directory, ShippingConfig shipping, LogConfig logConfig,
                                          long maxMemoryBytes) throws IOException {
        GraphKernel kernel = DurableGraph.open(directory, logConfig, maxMemoryBytes, shipping).kernel();
        try {
            kernel.shipWatermark().awaitShipped(kernel.epoch(), kernel.appliedLsn(), SHIP_WAIT.toNanos());
        } catch (RuntimeException failure) {
            closeQuietly(kernel, failure);
            throw failure;
        }
        return kernel;
    }

    private static void closeQuietly(GraphKernel kernel, RuntimeException cause) {
        try {
            kernel.close();
        } catch (RuntimeException closeFailure) {
            cause.addSuppressed(closeFailure);
        }
    }

    private static long wallClockMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * MICROS_PER_SECOND + now.getNano() / NANOS_PER_MICRO;
    }
}
