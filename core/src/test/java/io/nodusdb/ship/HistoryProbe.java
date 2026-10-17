package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.RecordStream;
import io.nodusdb.log.RecordsGoneException;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.replica.AnchorBounds;
import io.nodusdb.replica.ChainGapException;
import io.nodusdb.replica.ChainReplay;
import io.nodusdb.replica.ReplayLimit;
import io.nodusdb.replica.ReplayPosition;
import io.nodusdb.replica.SnapshotDownloader;
import io.nodusdb.watch.ChainRecordSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

final class HistoryProbe {

    interface Requirement {

        void require(boolean condition, String message);
    }

    private static final int BATCH_BYTES = 1 << 20;
    private static final int SETTLE_READS = 10_000;

    private static final class TargetLimit implements ReplayLimit {

        private final long target;
        private boolean reached;

        TargetLimit(long target) {
            this.target = target;
        }

        @Override
        public long lastLsn(ChainBody.Records records, long appliedLsn) {
            byte[] bytes = records.records();
            RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length);
            long last = appliedLsn;
            while (reader.hasRecord()) {
                if (reader.isCommitPoint()) {
                    if (reader.lsn() > target) {
                        reached = true;
                        return last;
                    }
                    last = reader.lsn();
                }
                reader.advance();
            }
            reached |= records.lsnLast() >= target;
            return last;
        }
    }

    private final MemoryObjectStore memory;
    private final FaultyObjectStore store;
    private final ChainFetch fetch;
    private final ChainRecordSource source;
    private final Path scratch;
    private final NavigableMap<Long, List<Long>> committed;
    private final LongFunction<Set<Long>> stateAt;
    private final LongSupplier lastLsn;
    private final Requirement checks;
    private final Random random;
    private long cursor;
    private RecordStream stream;
    private long batchesVerified;
    private long viewsVerified;

    HistoryProbe(MemoryObjectStore memory, Path scratch, NavigableMap<Long, List<Long>> committed,
                 LongFunction<Set<Long>> stateAt, LongSupplier lastLsn, Requirement checks, long seed) {
        this.memory = memory;
        this.store = new FaultyObjectStore(memory);
        this.fetch = new ChainFetch(store, ChainBuilder.keyring(), ChainFetch.Trust.REQUIRED);
        this.source = new ChainRecordSource(store, fetch);
        this.scratch = scratch;
        this.committed = committed;
        this.stateAt = stateAt;
        this.lastLsn = lastLsn;
        this.checks = checks;
        this.random = new Random(seed);
    }

    void faults(boolean on, long seed, double rate) {
        if (on) {
            store.randomFaults(seed, rate, rate, 0);
        } else {
            store.stopRandomFaults();
        }
    }

    void step() {
        if (random.nextInt(4) == 0) {
            buildView(1 + (long) (random.nextDouble() * Math.max(1, lastLsn.getAsLong())));
        } else {
            watch();
        }
    }

    void settle() {
        store.stopRandomFaults();
        for (int i = 0; i < SETTLE_READS && cursor < lastLsn.getAsLong(); i++) {
            watch();
        }
        checks.require(cursor == lastLsn.getAsLong(), "I7: the watcher stopped at LSN " + cursor
                + " but the chain ends at LSN " + lastLsn.getAsLong());
        buildView(lastLsn.getAsLong());
        checks.require(batchesVerified > 0, "I7: the watcher verified no batch");
    }

    long viewsVerified() {
        return viewsVerified;
    }

    private void watch() {
        try {
            if (stream == null) {
                Optional<RecordStream> opened = source.open(cursor);
                if (opened.isEmpty()) {
                    long resume = requireNotHeld(cursor);
                    cursor = Math.max(cursor, resume);
                    return;
                }
                stream = opened.get();
            }
            LogTailReader.Batch batch = stream.next(BATCH_BYTES);
            if (batch != null) {
                verify(batch);
            }
        } catch (RecordsGoneException gone) {
            stream = null;
        } catch (IOException | ObjectStoreException interruption) {
            return;
        }
    }

    private void verify(LogTailReader.Batch batch) {
        byte[] bytes = batch.records();
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length);
        List<Long> edges = new ArrayList<>();
        long first = -1;
        while (reader.hasRecord()) {
            if (first < 0) {
                first = reader.lsn();
            }
            if (reader.type() == RecordType.TUPLE_ADD) {
                edges.add(SimSink.edge(reader.object(), reader.subject()));
            }
            checks.require(reader.type() != RecordType.TUPLE_REMOVE, "I7: the simulation never removes a tuple");
            if (reader.isCommitPoint()) {
                long commit = reader.lsn();
                if (commit > cursor) {
                    checks.require(first == cursor + 1, "I7: the transaction after LSN " + cursor
                            + " starts at LSN " + first);
                    checks.require(edges.equals(committed.getOrDefault(commit, List.of())),
                            "I7: the transaction at LSN " + commit + " is not the one the log committed");
                    cursor = commit;
                }
                edges.clear();
                first = -1;
            }
            reader.advance();
        }
        batchesVerified++;
    }

    private long requireNotHeld(long after) {
        for (String key : ChainAudit.chainKeys(memory)) {
            ChainObject object = ChainCodec.decode(memory.get(key).orElseThrow());
            if (object.body() instanceof ChainBody.Records records) {
                checks.require(records.lsnFirst() > after + 1, "I7: the chain holds LSN " + (after + 1)
                        + " in " + key + " but the watch source said it does not");
                return records.lsnFirst() - 1;
            }
        }
        return after;
    }

    private void buildView(long target) {
        SimSink sink = new SimSink();
        ChainReplay replay = new ChainReplay(store, fetch, new SnapshotDownloader(store, 2), scratch);
        TargetLimit limit = new TargetLimit(target);
        try {
            Optional<ReplayPosition> position = replay.bootstrap(sink, new AnchorBounds(0, target, Long.MAX_VALUE),
                    limit);
            if (position.isEmpty()) {
                return;
            }
            while (!limit.reached && sink.appliedLsn() < target) {
                if (replay.advance(position.get(), sink, object -> { }, limit).isEmpty()) {
                    break;
                }
            }
        } catch (IOException | ObjectStoreException | ChainGapException | UncheckedIOException interruption) {
            return;
        }
        if (!limit.reached && sink.appliedLsn() < target) {
            return;
        }
        checks.require(sink.appliedLsn() <= target, "I8: a view for LSN " + target + " applied LSN "
                + sink.appliedLsn());
        checks.require(sink.edges().equals(stateAt.apply(target)), "I8: the view at LSN " + target
                + " differs from the log's state at that LSN");
        checks.require(committed.subMap(sink.appliedLsn(), false, target, true).isEmpty(), "I8: the view at LSN "
                + target + " stopped at LSN " + sink.appliedLsn() + " before a later commit it should hold");
        viewsVerified++;
    }
}
