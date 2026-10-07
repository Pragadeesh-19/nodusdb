package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainFormatException;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.error.CorruptLogException;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.PutResult;
import io.nodusdb.objectstore.TransientStoreException;
import io.nodusdb.ship.OpenReconciler.Start;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.LongSupplier;

public final class ShipperCore {

    public enum Cadence {
        CONTINUE, IDLE, BACKOFF, STOP
    }

    public record Next(Cadence cadence, long nanos) {

        private static final Next CONTINUE = new Next(Cadence.CONTINUE, 0);
        private static final Next STOP = new Next(Cadence.STOP, 0);

        static Next immediately() {
            return CONTINUE;
        }

        static Next stop() {
            return STOP;
        }

        static Next idle(long nanos) {
            return new Next(Cadence.IDLE, nanos);
        }

        static Next backoff(long nanos) {
            return new Next(Cadence.BACKOFF, nanos);
        }
    }

    private final ObjectStore store;
    private final EpochClaims claims;
    private final WriterIdentity identity;
    private final Start start;
    private final LogFeed feed;
    private final ShipState state;
    private final ShipSettings settings;
    private final LongSupplier nanoClock;
    private final ChainCursor cursor;
    private ChainObject pending;
    private long pendingLastLsn;
    private int transientFailures;
    private int conflicts;

    public ShipperCore(ObjectStore store, EpochClaims claims, WriterIdentity identity, Start start, LogFeed feed,
                       ShipState state, ShipSettings settings, LongSupplier nanoClock) {
        this.store = store;
        this.claims = claims;
        this.identity = identity;
        this.start = start;
        this.feed = feed;
        this.state = state;
        this.settings = settings;
        this.nanoClock = nanoClock;
        this.cursor = start.cursor();
    }

    public Next step() {
        if (state.terminal()) {
            return Next.stop();
        }
        try {
            return pending == null ? prepare() : commit();
        } catch (ObjectStoreException failure) {
            return storeFailed(failure);
        } catch (IOException | CorruptLogException failure) {
            return logFailed(failure);
        }
    }

    public boolean drain() {
        Next next;
        do {
            next = step();
        } while (next.cadence() == Cadence.CONTINUE);
        return next.cadence() == Cadence.IDLE;
    }

    private Next prepare() throws IOException {
        if (cursor.atStart()) {
            pending = sealReference();
            pendingLastLsn = start.firstReference().lsn();
            return commit();
        }
        LogTailReader.Batch batch = feed.next(settings.maxObjectBytes());
        if (batch == null) {
            state.backlog(feed.unreadBytes());
            state.active();
            transientFailures = 0;
            return Next.idle(settings.interval().toNanos());
        }
        if (batch.lsnFirst() != cursor.lastLsn() + 1) {
            state.failed("the log batch starts at LSN " + batch.lsnFirst() + " but the chain ends at LSN "
                    + cursor.lastLsn());
            return Next.backoff(settings.failedRetry().toNanos());
        }
        pending = ChainCodec.seal(header(ChainKind.RECORDS),
                new ChainBody.Records(batch.lsnFirst(), batch.lsnLast(), batch.records()), identity.key());
        pendingLastLsn = batch.lsnLast();
        state.backlog(feed.unreadBytes() + batch.records().length);
        return commit();
    }

    private ChainObject sealReference() {
        return ChainCodec.seal(header(ChainKind.SNAPSHOT_REF), start.firstReference(), identity.key());
    }

    private ChainHeader header(ChainKind kind) {
        return new ChainHeader(kind, cursor.seq() + 1, start.epoch(), identity.nonce(), cursor.digest(),
                identity.key().keyId());
    }

    private Next commit() throws IOException {
        if (claims.supersededBy(start.epoch())) {
            return fenced("epoch " + start.epoch() + " was superseded: a writer with a newer epoch has claimed "
                    + "this bucket");
        }
        String key = ChainLayout.chainKey(pending.seq());
        PutResult result = store.putIfAbsent(key, pending.encoded());
        return switch (result) {
            case CREATED -> committed();
            case ALREADY_EXISTS -> settle(key);
            case CONFLICT -> conflicted(key);
        };
    }

    private Next committed() throws IOException {
        cursor.accept(pending);
        state.shipped(pendingLastLsn, pending.seq(), pending.encoded().length, nanoClock.getAsLong());
        state.active();
        pending = null;
        transientFailures = 0;
        conflicts = 0;
        state.backlog(feed.unreadBytes());
        return Next.immediately();
    }

    private Next conflicted(String key) throws IOException {
        conflicts++;
        if (conflicts <= settings.conflictRetries()) {
            state.retrying("a concurrent write to " + key + " is in progress");
            return Next.backoff(settings.backoffNanos(conflicts));
        }
        conflicts = 0;
        return settle(key);
    }

    private Next settle(String key) throws IOException {
        Optional<byte[]> existing = store.get(key);
        if (existing.isEmpty()) {
            return retryAfter("the store reports " + key + " as existing but cannot return it");
        }
        if (Arrays.equals(existing.get(), pending.encoded())) {
            return committed();
        }
        return fenced("chain object " + pending.seq() + " was written by another writer" + describe(existing.get())
                + "; this writer is in epoch " + start.epoch());
    }

    private static String describe(byte[] foreign) {
        try {
            ChainHeader header = ChainCodec.decode(foreign).header();
            return " in epoch " + header.epoch();
        } catch (ChainFormatException unreadable) {
            return "";
        }
    }

    private Next fenced(String reason) {
        state.fenced(reason);
        return Next.stop();
    }

    private Next retryAfter(String reason) {
        transientFailures++;
        state.retrying(reason);
        return Next.backoff(settings.backoffNanos(transientFailures));
    }

    private Next storeFailed(ObjectStoreException failure) {
        if (failure instanceof TransientStoreException) {
            return retryAfter(failure.getMessage());
        }
        state.failed(failure.getMessage());
        return Next.backoff(settings.failedRetry().toNanos());
    }

    private Next logFailed(Exception failure) {
        state.failed("reading the local log failed: " + failure.getMessage());
        return Next.backoff(settings.failedRetry().toNanos());
    }
}
