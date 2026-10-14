package io.nodusdb.erasure;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.symbols.SymbolTable;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.replica.Anchor;
import io.nodusdb.replica.AnchorFinder;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainRetention;
import io.nodusdb.ship.ShipState;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public final class BucketCut implements ErasureLayer {

    public record Bucket(AnchorFinder anchors, ChainFetch chain, ChainRetention retention, LongSupplier projectedSeq) {
    }

    static final String NAME = "bucket";
    static final long SEARCH_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

    private static final long NEVER_SEARCHED = -1;

    private final GraphKernel kernel;
    private final ShipState state;
    private final Bucket bucket;
    private final ErasureCoverage coverage;
    private final LongSupplier nanoClock;
    private long searchedNanos;
    private long searchedReferenceSeq = NEVER_SEARCHED;
    private Anchor newest;
    private ShipState.CutRequest requested;

    public BucketCut(GraphKernel kernel, ShipState state, Bucket bucket, ErasureCoverage coverage,
                     LongSupplier nanoClock) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.state = Objects.requireNonNull(state, "state");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.coverage = Objects.requireNonNull(coverage, "coverage");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public long coveredLsn() {
        return coverage.covered().eraseLsn();
    }

    @Override
    public boolean covers(long requiredLsn, int erasedSymbols) {
        ErasureCoverage.Covered covered = coverage.covered();
        return covered.erasedCount() >= erasedSymbols || covered.eraseLsn() >= requiredLsn;
    }

    @Override
    public void advance(long targetLsn, boolean due) {
        if (requested != null) {
            finishRequested();
            return;
        }
        int erased = kernel.erasedSymbols();
        long newestErase = kernel.newestEraseLsn();
        Anchor anchor = newestAnchor();
        if (anchor == null || anchor.snapshotLsn() < newestErase) {
            return;
        }
        long keep = anchor.firstSeq();
        requireClean(keep);
        if (bucket.projectedSeq().getAsLong() < keep) {
            return;
        }
        requested = new ShipState.CutRequest(newestErase, erased, keep, anchor.snapshotLsn());
        state.offerCut(requested);
    }

    private void finishRequested() {
        if (!state.committedCut().answers(requested)) {
            return;
        }
        bucket.retention().cut(requested.firstKeptSeq(), requested.snapshotLsn());
        coverage.record(new ErasureCoverage.Covered(requested.erasedCount(), requested.eraseLsn(),
                requested.firstKeptSeq(), requested.snapshotLsn()));
        requested = null;
    }

    private Anchor newestAnchor() {
        long now = nanoClock.getAsLong();
        long referenceSeq = state.snapshot().references().seq();
        boolean stale = searchedReferenceSeq == NEVER_SEARCHED || referenceSeq != searchedReferenceSeq
                || now - searchedNanos >= SEARCH_INTERVAL_NANOS;
        if (stale) {
            Iterator<Anchor> candidates = bucket.anchors().candidates();
            newest = candidates.hasNext() ? candidates.next() : null;
            searchedNanos = now;
            searchedReferenceSeq = referenceSeq;
        }
        return newest;
    }

    private void requireClean(long keep) {
        if (!(bucket.chain().require(keep).body() instanceof ChainBody.Records records)) {
            return;
        }
        SymbolTable symbols = kernel.symbols();
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(records.records()), 0,
                records.records().length);
        while (reader.hasRecord()) {
            if (reader.type() == RecordType.SYMBOL && symbols.isErased(reader.symbolId())) {
                throw new IllegalStateException("chain object " + keep + " is kept for followers and still holds "
                        + "an erased name; waiting for a later snapshot");
            }
            reader.advance();
        }
    }
}
