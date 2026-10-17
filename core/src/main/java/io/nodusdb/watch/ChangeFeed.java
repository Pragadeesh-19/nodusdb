package io.nodusdb.watch;

import io.nodusdb.error.HistoryExpiredException;
import io.nodusdb.error.TokenLostException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.changes.ChangeBuffer;
import io.nodusdb.kernel.changes.ChangeRing;
import io.nodusdb.log.RecordSource;
import io.nodusdb.log.RecordStream;
import io.nodusdb.log.RecordsGoneException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public final class ChangeFeed implements AutoCloseable {

    public static final int MAX_LIMIT = 1_000_000;
    public static final long NO_BOUND = Long.MAX_VALUE;

    private static final long DURABILITY_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long MAX_WAIT_NANOS = TimeUnit.DAYS.toNanos(3_650);

    private enum Outcome { DELIVERED, NOTHING_NEW, NOT_YET_READABLE }

    private record Attempt(Outcome outcome, ChangeBatch batch) {

        static final Attempt NOTHING_NEW = new Attempt(Outcome.NOTHING_NEW, null);
        static final Attempt NOT_YET_READABLE = new Attempt(Outcome.NOT_YET_READABLE, null);
    }

    private final GraphKernel kernel;
    private final List<RecordSource> sources;
    private final int ringCapacity;
    private final ChangeDecoder decoder;
    private final FallbackReaders readers;
    private final AtomicLong expiredReads = new AtomicLong();
    private volatile boolean closed;

    public ChangeFeed(GraphKernel kernel, List<RecordSource> sources, int ringCapacity) {
        this(kernel, sources, ringCapacity, System::nanoTime, FallbackReaders.IDLE_LIMIT);
    }

    ChangeFeed(GraphKernel kernel, List<RecordSource> sources, int ringCapacity, LongSupplier readerClock,
               Duration readerIdleLimit) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.sources = List.copyOf(sources);
        this.ringCapacity = ChangeRing.requireCapacity(ringCapacity);
        this.decoder = new ChangeDecoder(kernel);
        this.readers = new FallbackReaders(readerClock, readerIdleLimit);
    }

    public int ringCapacity() {
        return ringCapacity;
    }

    public int openReaders() {
        return readers.size();
    }

    public long expiredReads() {
        return expiredReads.get();
    }

    public ChangeBatch read(Token after, long throughLsn, int limit, Duration wait) {
        Objects.requireNonNull(after, "after");
        Objects.requireNonNull(wait, "wait");
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("a read returns between 1 and " + MAX_LIMIT + " changes: " + limit);
        }
        if (throughLsn < 0) {
            throw new IllegalArgumentException("the LSN to read through must not be negative: " + throughLsn);
        }
        if (wait.isNegative()) {
            throw new IllegalArgumentException("the wait must not be negative: " + wait);
        }
        requireOpen();
        ChangeRing ring = kernel.enableChanges(ringCapacity);
        long deadline = System.nanoTime() + waitNanos(wait);
        while (true) {
            requireOpen();
            long seen = Math.max(after.lsn(), ring.publishedLsn());
            Attempt attempt = attempt(after, throughLsn, limit, ring);
            if (attempt.outcome() == Outcome.DELIVERED) {
                return attempt.batch();
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return ChangeBatch.nothingAfter(after);
            }
            if (attempt.outcome() == Outcome.NOT_YET_READABLE) {
                ring.awaitBeyond(ring.publishedLsn(), Math.min(remaining, DURABILITY_POLL_NANOS));
            } else {
                ring.awaitBeyond(seen, remaining);
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        readers.close();
    }

    private Attempt attempt(Token after, long throughLsn, int limit, ChangeRing ring) {
        long epoch = kernel.epoch();
        if (after.epoch() < epoch && kernel.epochHistory().isLost(after.epoch(), after.lsn())) {
            throw new TokenLostException("the change at " + after + " was acknowledged but never became durable; "
                    + "read the current state and watch from token()");
        }
        if (after.epoch() > epoch) {
            return Attempt.NOTHING_NEW;
        }
        long bound = Math.min(kernel.appliedLsn(), throughLsn);
        if (after.lsn() >= bound) {
            return Attempt.NOTHING_NEW;
        }
        ChangeBuffer buffer = new ChangeBuffer();
        if (ring.read(after.lsn(), bound, limit, buffer)) {
            return buffer.size() == 0 ? Attempt.NOTHING_NEW : delivered(buffer);
        }
        fallback(after.lsn(), bound, limit, buffer);
        return buffer.size() == 0 ? Attempt.NOT_YET_READABLE : delivered(buffer);
    }

    private Attempt delivered(ChangeBuffer buffer) {
        List<Change> changes = decoder.decode(buffer);
        return new Attempt(Outcome.DELIVERED, new ChangeBatch(changes, decoder.tokenOf(buffer.lastCommitLsn())));
    }

    private void fallback(long afterLsn, long boundLsn, int limit, ChangeBuffer buffer) {
        FallbackCursor cursor = readers.take(afterLsn);
        int nextSource = 0;
        while (true) {
            if (cursor == null) {
                cursor = open(afterLsn, nextSource);
                if (cursor == null) {
                    throw expired(afterLsn);
                }
            }
            try {
                cursor.fill(boundLsn, limit, buffer);
                readers.give(cursor);
                return;
            } catch (RecordsGoneException gone) {
                FallbackReaders.closeAll(List.of(cursor));
                if (buffer.size() > 0) {
                    return;
                }
                nextSource = cursor.sourceIndex() + 1;
                cursor = null;
            } catch (IOException failure) {
                closeAfterFailure(cursor, failure);
                throw new UncheckedIOException("could not read the changes after LSN " + afterLsn + " from the "
                        + cursor.sourceName(), failure);
            } catch (RuntimeException failure) {
                closeAfterFailure(cursor, failure);
                throw failure;
            }
        }
    }

    private FallbackCursor open(long afterLsn, int fromSource) {
        for (int index = fromSource; index < sources.size(); index++) {
            RecordSource source = sources.get(index);
            Optional<RecordStream> stream;
            try {
                stream = source.open(afterLsn);
            } catch (IOException failure) {
                throw new UncheckedIOException("could not open the " + source.name() + " after LSN " + afterLsn,
                        failure);
            }
            if (stream.isPresent()) {
                return new FallbackCursor(index, source.name(), stream.get(), afterLsn);
            }
        }
        return null;
    }

    private HistoryExpiredException expired(long afterLsn) {
        expiredReads.incrementAndGet();
        ChangeRing ring = kernel.changes();
        String held = ring == null ? "" : "the change buffer holds changes after LSN " + ring.servesFromLsn();
        String others;
        if (sources.isEmpty()) {
            others = "and the graph keeps no other history";
        } else if (sources.size() == 1) {
            others = "and the " + sources.get(0).name() + " does not hold them";
        } else {
            others = "and neither the " + sources.stream().map(RecordSource::name)
                    .collect(Collectors.joining(" nor the ")) + " holds them";
        }
        return new HistoryExpiredException("the changes after LSN " + afterLsn + " are no longer kept: " + held
                + (held.isEmpty() ? "" : ", ") + others + "; read the current state and watch from token()");
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("the change feed is closed");
        }
        if (kernel.isClosed()) {
            throw new IllegalStateException("graph is closed");
        }
    }

    private static void closeAfterFailure(FallbackCursor cursor, Exception failure) {
        try {
            cursor.close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static long waitNanos(Duration wait) {
        try {
            return Math.min(wait.toNanos(), MAX_WAIT_NANOS);
        } catch (ArithmeticException tooLong) {
            return MAX_WAIT_NANOS;
        }
    }
}
