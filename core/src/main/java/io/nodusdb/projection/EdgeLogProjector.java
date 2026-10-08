package io.nodusdb.projection;

import io.nodusdb.avro.AvroFormatException;
import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainFormatException;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.iceberg.DataFile;
import io.nodusdb.iceberg.DataFileWriter;
import io.nodusdb.iceberg.EdgeLogRows;
import io.nodusdb.iceberg.IcebergTable;
import io.nodusdb.json.JsonException;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.TransientStoreException;
import io.nodusdb.ship.ShipState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

public final class EdgeLogProjector {

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

    public record Clocks(LongSupplier nanos, LongSupplier millis) {
    }

    public static final String PROJECTED_LSN = "nodus.projected.lsn";
    public static final String PROJECTED_EPOCH = "nodus.projected.epoch";
    public static final String PROJECTED_SCHEMA_VERSION = "nodus.projected.schema_version";
    public static final String PROJECTED_CHAIN_SEQ = "nodus.projected.chain_seq";

    private static final long MAX_IDLE_NANOS = 250_000_000L;

    private final ChainSource source;
    private final ShipState state;
    private final IcebergTable table;
    private final DataFileWriter writer;
    private final NameResolver resolver;
    private final SigningKey key;
    private final ProjectionSettings settings;
    private final Clocks clocks;
    private final DayBuffers buffers = new DayBuffers();
    private final EdgeLogDecoder decoder;
    private final List<DataFile> pendingFiles = new ArrayList<>();
    private boolean started;
    private boolean contextKnown;
    private boolean drainRequested;
    private Optional<IcebergTable.Head> head = Optional.empty();
    private ChainCursor cursor;
    private long nextSeq;
    private long lastAppliedSeq;
    private long projectedLsn;
    private long committedLsn;
    private long batchStartLsn = -1;
    private long pendingRows;
    private ChainHash previousCommit;
    private long lastCommitNanos;
    private long lastSweepNanos;
    private int failures;
    private IcebergTable.Prepared inFlight;
    private CommitRecord.Sealed inFlightRecord;
    private IcebergTable.Expiry expiry;
    private boolean expiryPublished;

    public EdgeLogProjector(ChainSource source, ShipState state, IcebergTable table, DataFileWriter writer,
                            NameResolver resolver, SigningKey key, ProjectionSettings settings, Clocks clocks) {
        this.source = source;
        this.state = state;
        this.table = table;
        this.writer = writer;
        this.resolver = resolver;
        this.key = key;
        this.settings = settings;
        this.clocks = clocks;
        this.decoder = new EdgeLogDecoder(new StreamNames(resolver), buffers);
    }

    public Next step() {
        if (state.projectionTerminal()) {
            return Next.stop();
        }
        try {
            if (!started) {
                start();
            }
            return advance();
        } catch (WriterFencedException fenced) {
            state.projectionFenced(fenced.getMessage());
            return Next.stop();
        } catch (TransientStoreException transientFailure) {
            return retryAfter(transientFailure.getMessage());
        } catch (ObjectStoreException | ChainFormatException | ChainTrustException | AvroFormatException
                 | JsonException | IllegalStateException | IOException | UncheckedIOException failure) {
            state.projectionFailed(String.valueOf(failure.getMessage()));
            return Next.backoff(settings.failedRetry().toNanos());
        }
    }

    public boolean drain() {
        drainRequested = true;
        try {
            Next next;
            do {
                next = step();
            } while (next.cadence() == Cadence.CONTINUE);
            return next.cadence() == Cadence.IDLE && !hasWork();
        } finally {
            drainRequested = false;
        }
    }

    private Next advance() throws IOException {
        if (expiry != null) {
            finishExpiry();
            return Next.immediately();
        }
        if (inFlight != null) {
            publishInFlight();
            return Next.immediately();
        }
        if (sweepDue()) {
            sweepOrphans();
        }
        long shipped = state.snapshot().chainSeq();
        int applied = 0;
        while (nextSeq <= shipped && applied < settings.objectsPerStep()) {
            Optional<ChainObject> object = source.fetch(nextSeq);
            if (object.isEmpty()) {
                return retryAfter("chain object " + nextSeq + " is shipped but cannot be read");
            }
            apply(object.get());
            nextSeq++;
            applied++;
            writeFullBuffers();
        }
        if (hasWork() && (drainRequested || commitDue())) {
            commit();
            return Next.immediately();
        }
        if (nextSeq <= shipped) {
            return Next.immediately();
        }
        failures = 0;
        state.projectionActive();
        return Next.idle(Math.min(MAX_IDLE_NANOS, settings.commitInterval().toNanos()));
    }

    private void start() {
        state.projectionStarting();
        head = table.load();
        if (head.isPresent()) {
            resume(head.get());
        } else {
            begin();
        }
        long now = clocks.nanos().getAsLong();
        lastCommitNanos = now;
        lastSweepNanos = now;
        started = true;
    }

    private void begin() {
        OptionalLong oldest = source.oldestSeq();
        nextSeq = oldest.orElse(1);
        cursor = null;
        contextKnown = false;
        projectedLsn = -1;
        committedLsn = -1;
        batchStartLsn = -1;
        previousCommit = null;
        table.removeOrphanData(committedLsn);
    }

    private void resume(IcebergTable.Head existing) {
        Map<String, String> summary = existing.state().current().orElseThrow(
                () -> new IllegalStateException("the table has no snapshot")).summary();
        long lsn = requiredNumber(summary, PROJECTED_LSN);
        long seq = requiredNumber(summary, PROJECTED_CHAIN_SEQ);
        long epoch = requiredNumber(summary, PROJECTED_EPOCH);
        long version = requiredNumber(summary, PROJECTED_SCHEMA_VERSION);
        String sealed = summary.get(CommitRecord.PROPERTY);
        previousCommit = sealed == null ? null : CommitRecord.digestOf(sealed);
        ChainObject last = source.fetch(seq).orElseThrow(() -> new IllegalStateException(
                "chain object " + seq + ", the last one projected, cannot be read"));
        cursor = ChainCursor.after(seq, last.digest(), last.epoch(), lsn);
        nextSeq = seq + 1;
        lastAppliedSeq = seq;
        projectedLsn = lsn;
        committedLsn = lsn;
        batchStartLsn = lsn + 1;
        decoder.context(epoch, Math.toIntExact(version));
        contextKnown = true;
        state.projectionResumed(lsn, seq, existing.state().currentSnapshotId());
        table.removeOrphans(existing, lsn, clocks.millis().getAsLong() - settings.orphanGrace().toMillis());
    }

    private static long requiredNumber(Map<String, String> summary, String property) {
        String value = summary.get(property);
        if (value == null) {
            throw new IllegalStateException("the table was not written by nodusdb: its snapshot lacks " + property);
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("the table's " + property + " is not a number");
        }
    }

    private void apply(ChainObject object) {
        if (cursor == null) {
            cursor = precursorOf(object);
        }
        cursor.accept(object);
        if (!contextKnown) {
            learnContext(object);
        }
        if (object.body() instanceof ChainBody.Records records) {
            if (batchStartLsn < 0) {
                batchStartLsn = records.lsnFirst();
            }
            decoder.decode(records);
        }
        projectedLsn = cursor.lastLsn();
        lastAppliedSeq = object.seq();
    }

    private static ChainCursor precursorOf(ChainObject object) {
        if (object.seq() == 1) {
            return ChainCursor.beforeFirst();
        }
        long lastLsn = switch (object.body()) {
            case ChainBody.Records records -> records.lsnFirst() - 1;
            case ChainBody.SnapshotRef reference -> reference.lsn();
        };
        return ChainCursor.after(object.seq() - 1, object.header().prev(), object.epoch(), lastLsn);
    }

    private void learnContext(ChainObject object) {
        long lsn = switch (object.body()) {
            case ChainBody.Records records -> records.lsnFirst();
            case ChainBody.SnapshotRef reference -> reference.lsn();
        };
        decoder.context(resolver.tenureEpoch(lsn), resolver.schemaVersion());
        contextKnown = true;
    }

    private boolean hasWork() {
        return !pendingFiles.isEmpty() || buffers.any();
    }

    private boolean commitDue() {
        return clocks.nanos().getAsLong() - lastCommitNanos >= settings.commitInterval().toNanos();
    }

    private void writeFullBuffers() throws IOException {
        for (int day : buffers.daysWithAtLeast(settings.flushRows())) {
            writeBuffer(day);
        }
    }

    private void writeAllBuffers() throws IOException {
        for (int day : buffers.nonEmptyDays()) {
            writeBuffer(day);
        }
    }

    private void writeBuffer(int day) throws IOException {
        EdgeLogRows rows = buffers.buffer(day);
        DataFile file = writer.write(rows, day);
        pendingFiles.add(file);
        pendingRows += file.recordCount();
        buffers.written(day);
    }

    private void commit() throws IOException {
        writeAllBuffers();
        inFlightRecord = CommitRecord.seal(key, previousCommit, batchStartLsn, projectedLsn, lastAppliedSeq,
                pendingFiles);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(PROJECTED_LSN, Long.toString(projectedLsn));
        properties.put(PROJECTED_EPOCH, Long.toString(decoder.epoch()));
        properties.put(PROJECTED_SCHEMA_VERSION, Integer.toString(decoder.version()));
        properties.put(PROJECTED_CHAIN_SEQ, Long.toString(lastAppliedSeq));
        properties.put(CommitRecord.PROPERTY, inFlightRecord.text());
        inFlight = table.prepareAppend(head, List.copyOf(pendingFiles), properties);
        publishInFlight();
    }

    private void publishInFlight() {
        IcebergTable.Head published = table.publish(inFlight);
        head = Optional.of(published);
        previousCommit = inFlightRecord.digest();
        long now = clocks.nanos().getAsLong();
        state.projectionCommitted(projectedLsn, lastAppliedSeq, published.state().currentSnapshotId(), pendingRows,
                now);
        committedLsn = projectedLsn;
        batchStartLsn = projectedLsn + 1;
        lastCommitNanos = now;
        pendingFiles.clear();
        pendingRows = 0;
        inFlight = null;
        inFlightRecord = null;
        failures = 0;
        decoder.commitBoundary();
        IcebergTable.Expiry due = table.prepareExpiry(published,
                clocks.millis().getAsLong() - settings.snapshotRetention().toMillis());
        if (due.prepared().isPresent()) {
            expiry = due;
            expiryPublished = false;
        }
    }

    private void finishExpiry() {
        if (!expiryPublished) {
            head = Optional.of(table.publish(expiry.prepared().orElseThrow()));
            expiryPublished = true;
        }
        table.deleteAll(expiry.deletable());
        expiry = null;
        expiryPublished = false;
    }

    private boolean sweepDue() {
        return head.isPresent() && pendingFiles.isEmpty() && inFlight == null
                && clocks.nanos().getAsLong() - lastSweepNanos >= settings.orphanSweepInterval().toNanos();
    }

    private void sweepOrphans() {
        table.removeOrphans(head.orElseThrow(), committedLsn,
                clocks.millis().getAsLong() - settings.orphanGrace().toMillis());
        lastSweepNanos = clocks.nanos().getAsLong();
    }

    private Next retryAfter(String reason) {
        failures++;
        state.projectionRetrying(reason);
        return Next.backoff(settings.backoffNanos(failures));
    }
}
