package io.nodusdb.history;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.error.HistoryExpiredException;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.kernel.ErasedSymbols;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.past.PastReport;
import io.nodusdb.kernel.past.PastTarget;
import io.nodusdb.kernel.past.PastView;
import io.nodusdb.replica.ChainReplay;
import io.nodusdb.replica.KernelReplicaSink;
import io.nodusdb.replica.ReplayPosition;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

final class BucketView implements PastView {

    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

    private final BucketPast owner;
    private final GraphKernel live;
    private final KernelReplicaSink sink;
    private final MemoryBudget budget;
    private final ChainReplay replay;
    private final ReplayPosition position;
    private final long snapshotLsn;
    private long buildMillis;
    private ChainObject held;
    private long replayedObjects;
    private long replayedRecords;
    private int seenErasures;
    private volatile boolean closed;

    BucketView(BucketPast owner, GraphKernel live, KernelReplicaSink sink, MemoryBudget budget, ChainReplay replay,
               ReplayPosition position, int erasuresCovered) {
        this.owner = owner;
        this.live = live;
        this.sink = sink;
        this.budget = budget;
        this.replay = replay;
        this.position = position;
        this.snapshotLsn = position.snapshotLsn();
        this.seenErasures = erasuresCovered;
    }

    synchronized void build(PastLimit limit, long deadlineNanos, long startedNanos) {
        roll(limit, deadlineNanos);
        sink.kernel().completeClosure();
        buildMillis = (System.nanoTime() - startedNanos) / NANOS_PER_MILLI;
    }

    @Override
    public GraphKernel kernel() {
        return sink.kernel();
    }

    @Override
    public synchronized PastReport report() {
        GraphKernel kernel = sink.kernel();
        return new PastReport(kernel.token(), kernel.lastCommitMicros(), snapshotLsn, replayedObjects,
                replayedRecords, buildMillis, budget.used());
    }

    @Override
    public synchronized void refreshErasures() {
        int erased = live.erasedSymbols();
        if (erased == seenErasures || closed) {
            return;
        }
        ErasedSymbols log = live.erasedSymbolLog();
        GraphKernel kernel = sink.kernel();
        for (int i = 0; i < log.count(); i++) {
            int symbol = log.symbolId(i);
            if (symbol < kernel.symbols().size()) {
                kernel.scrubSymbol(symbol, live.symbols().resolve(symbol));
            }
        }
        seenErasures = erased;
    }

    @Override
    public synchronized void advance(PastTarget target, Duration wait) {
        requireOpen();
        GraphKernel kernel = sink.kernel();
        if (target instanceof PastTarget.AtToken token && token.token().lsn() < kernel.appliedLsn()
                || target instanceof PastTarget.AtTime time && time.micros() < kernel.lastCommitMicros()) {
            throw new IllegalArgumentException("a past view moves forward only; it stands at " + kernel.token());
        }
        long newestErase = live.newestEraseLsn();
        if (newestErase > kernel.appliedLsn()) {
            throw new HistoryExpiredException("a name was erased at LSN " + newestErase + ", after this view; "
                    + "open a new view instead of moving this one");
        }
        long deadline = owner.prepare(target, wait);
        roll(new PastLimit(target), deadline);
        kernel.completeClosure();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            sink.kernel().close();
        } finally {
            budget.releaseAll();
            owner.closed(this);
        }
    }

    private void roll(PastLimit limit, long deadlineNanos) {
        limit.reachedAt(position.appliedLsn());
        if (held != null && !limit.reached()) {
            apply(held, limit);
        }
        while (!limit.reached()) {
            long before = position.appliedLsn();
            Optional<ChainObject> next = replay.advance(position, sink, object -> { }, limit);
            if (next.isPresent()) {
                replayedObjects++;
                replayedRecords += position.appliedLsn() - before;
                keepIfPartial(next.get());
                continue;
            }
            if (limit.target() instanceof PastTarget.AtTime) {
                return;
            }
            if (System.nanoTime() >= deadlineNanos) {
                throw new StaleReadException("the bucket holds the graph up to LSN " + position.appliedLsn()
                        + ", not yet " + ((PastTarget.AtToken) limit.target()).token());
            }
            owner.pauseForChain();
        }
    }

    private void apply(ChainObject object, PastLimit limit) {
        long before = position.appliedLsn();
        ChainReplay.applyRecords(object, position, sink, limit);
        replayedRecords += position.appliedLsn() - before;
        keepIfPartial(object);
    }

    private void keepIfPartial(ChainObject object) {
        held = object.body() instanceof ChainBody.Records records && position.appliedLsn() < records.lsnLast()
                ? object : null;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("the past view is closed");
        }
    }
}
