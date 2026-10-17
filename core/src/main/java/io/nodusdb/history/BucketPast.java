package io.nodusdb.history;

import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.error.HistoryExpiredException;
import io.nodusdb.error.TokenLostException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.past.PastSource;
import io.nodusdb.kernel.past.PastTarget;
import io.nodusdb.kernel.past.PastView;
import io.nodusdb.log.ShipWatermark;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.replica.AnchorBounds;
import io.nodusdb.replica.ChainReplay;
import io.nodusdb.replica.KernelReplicaSink;
import io.nodusdb.replica.ReplayPosition;
import io.nodusdb.replica.SnapshotDownloader;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.storage.snapshot.SnapshotMeta;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class BucketPast implements PastSource {

    public static final int DEFAULT_DOWNLOAD_PARALLELISM = 4;

    private static final long CHAIN_POLL_MILLIS = 20;
    private static final long MAX_WAIT_NANOS = TimeUnit.DAYS.toNanos(3_650);
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;

    private final GraphKernel live;
    private final ObjectStore store;
    private final ChainFetch chain;
    private final Path scratch;
    private final int downloadParallelism;
    private final MemoryBudget pool;
    private final Set<BucketView> views = new HashSet<>();

    public BucketPast(GraphKernel live, ObjectStore store, ChainFetch chain, Path scratch, int downloadParallelism,
                      long poolLimitBytes) {
        this.live = Objects.requireNonNull(live, "live");
        this.store = Objects.requireNonNull(store, "store");
        this.chain = Objects.requireNonNull(chain, "chain");
        this.scratch = Objects.requireNonNull(scratch, "scratch");
        if (downloadParallelism < 1 || downloadParallelism > SnapshotDownloader.MAX_PARALLELISM) {
            throw new IllegalArgumentException("the download parallelism must be between 1 and "
                    + SnapshotDownloader.MAX_PARALLELISM + ": " + downloadParallelism);
        }
        if (poolLimitBytes < 0) {
            throw new IllegalArgumentException("the history memory limit must not be negative: " + poolLimitBytes);
        }
        this.downloadParallelism = downloadParallelism;
        this.pool = poolLimitBytes == GraphKernel.NO_MEMORY_LIMIT ? MemoryBudget.unlimited()
                : MemoryBudget.limitedTo(poolLimitBytes);
    }

    @Override
    public PastView open(PastTarget target, long maxMemoryBytes, Duration wait) {
        Objects.requireNonNull(target, "target");
        if (maxMemoryBytes < 0) {
            throw new IllegalArgumentException("the view memory limit must not be negative: " + maxMemoryBytes);
        }
        long started = System.nanoTime();
        int erasuresCovered = live.erasedSymbols();
        long deadline = prepare(target, wait);
        long floor = live.newestEraseLsn();
        if (target instanceof PastTarget.AtToken token && token.token().lsn() < floor) {
            throw new HistoryExpiredException("a name was erased at LSN " + floor + ", so the bucket keeps no history "
                    + "before it; " + token.token() + " is earlier");
        }
        MemoryBudget budget = MemoryBudget.drawingOn(pool,
                maxMemoryBytes == GraphKernel.NO_MEMORY_LIMIT ? Long.MAX_VALUE : maxMemoryBytes);
        GraphKernel kernel = GraphKernel.openReplica(budget);
        try {
            BucketView view = start(kernel, budget, target, floor, erasuresCovered, deadline, started);
            synchronized (views) {
                views.add(view);
            }
            return view;
        } catch (RuntimeException | Error failure) {
            kernel.close();
            budget.releaseAll();
            throw failure;
        }
    }

    @Override
    public PoolReport pool() {
        synchronized (views) {
            return new PoolReport(true, views.size(), pool.used(),
                    pool.isLimited() ? pool.limit() : GraphKernel.NO_MEMORY_LIMIT);
        }
    }

    long prepare(PastTarget target, Duration wait) {
        Objects.requireNonNull(wait, "wait");
        if (wait.isNegative()) {
            throw new IllegalArgumentException("the wait must not be negative: " + wait);
        }
        if (live.isClosed()) {
            throw new IllegalStateException("graph is closed");
        }
        long deadline = System.nanoTime() + waitNanos(wait);
        if (target instanceof PastTarget.AtTime time && time.micros() > nowMicros()) {
            throw new IllegalArgumentException("a past view cannot stand at a point in the future: " + time.micros());
        }
        if (target instanceof PastTarget.AtToken token) {
            requireNotLost(token.token());
        }
        ShipWatermark shipping = live.shipWatermark();
        if (shipping.configured()) {
            Token now = live.token();
            shipping.awaitShipped(now.epoch(), now.lsn(), Math.max(1, deadline - System.nanoTime()));
        }
        return deadline;
    }

    void pauseForChain() {
        try {
            Thread.sleep(CHAIN_POLL_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new InterruptedIOException("the wait for the bucket was interrupted"));
        }
    }

    void closed(BucketView view) {
        synchronized (views) {
            views.remove(view);
        }
    }

    private BucketView start(GraphKernel kernel, MemoryBudget budget, PastTarget target, long floor,
                             int erasuresCovered, long deadline, long started) {
        KernelReplicaSink sink = new KernelReplicaSink(kernel);
        ChainReplay replay = new ChainReplay(store, chain, new SnapshotDownloader(store, downloadParallelism),
                scratchDirectory());
        PastLimit limit = new PastLimit(target);
        Optional<ReplayPosition> position;
        try {
            position = replay.bootstrap(sink, bounds(target, floor), limit);
        } catch (IOException e) {
            throw new UncheckedIOException("could not load a snapshot from the bucket", e);
        }
        if (position.isEmpty()) {
            throw new HistoryExpiredException("the bucket holds no snapshot at or before " + describe(target)
                    + (floor > 0 ? " and at or after the newest erasure, LSN " + floor : "")
                    + "; it keeps history only for its retention window");
        }
        requireLoadedWithin(sink.loaded(), target);
        BucketView view = new BucketView(this, live, sink, budget, replay, position.get(), erasuresCovered);
        view.build(limit, deadline, started);
        return view;
    }

    private Path scratchDirectory() {
        try {
            return Files.createDirectories(scratch);
        } catch (IOException e) {
            throw new UncheckedIOException("could not create the history scratch directory " + scratch, e);
        }
    }

    private void requireNotLost(Token token) {
        if (token.epoch() < live.epoch() && live.epochHistory().isLost(token.epoch(), token.lsn())) {
            throw new TokenLostException("the write at " + token + " was acknowledged but never became durable");
        }
    }

    private static AnchorBounds bounds(PastTarget target, long floor) {
        return switch (target) {
            case PastTarget.AtToken token -> new AnchorBounds(floor, token.token().lsn(),
                    Long.MAX_VALUE);
            case PastTarget.AtTime time -> new AnchorBounds(floor, Long.MAX_VALUE, time.micros());
        };
    }

    private static void requireLoadedWithin(SnapshotMeta loaded, PastTarget target) {
        if (target instanceof PastTarget.AtTime time && loaded.lastCommitMicros() > time.micros()) {
            throw new ChainTrustException("the snapshot at LSN " + loaded.lsn() + " was committed at "
                    + loaded.lastCommitMicros() + ", after the time its header gave");
        }
    }

    private static String describe(PastTarget target) {
        return switch (target) {
            case PastTarget.AtToken token -> token.token().toString();
            case PastTarget.AtTime time -> "the time " + time.micros() + " (microseconds since 1970)";
        };
    }

    private static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * MICROS_PER_SECOND + now.getNano() / NANOS_PER_MICRO;
    }

    private static long waitNanos(Duration wait) {
        try {
            return Math.min(wait.toNanos(), MAX_WAIT_NANOS);
        } catch (ArithmeticException tooLong) {
            return MAX_WAIT_NANOS;
        }
    }
}
