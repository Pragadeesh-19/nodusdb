package io.nodusdb.log.simulation;

import io.nodusdb.log.LogConfig;
import io.nodusdb.log.LogRecovery;
import io.nodusdb.log.RecoveryResult;
import io.nodusdb.log.ReplaySink;
import io.nodusdb.log.SegmentedLog;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

public final class LogHarness {

    public record Unit(long commitLsn, long commitMicros, List<String> records) {
    }

    public static final class CollectingSink implements ReplaySink {

        private final List<Unit> units = new ArrayList<>();
        private List<String> current = new ArrayList<>();
        private long lastEpoch;

        @Override
        public void apply(RecordReader record) {
            current.add(describe(record));
            if (record.type() == RecordType.EPOCH) {
                lastEpoch = Math.max(lastEpoch, record.epochNumber());
            }
        }

        @Override
        public void committed(long commitLsn, long commitMicros) {
            units.add(new Unit(commitLsn, commitMicros, current));
            current = new ArrayList<>();
        }

        public List<Unit> units() {
            return units;
        }

        public long lastEpoch() {
            return lastEpoch;
        }
    }

    public record Opened(SegmentedLog log, CollectingSink recovered, RecoveryResult result) {
    }

    private LogHarness() {
    }

    public static Opened open(LogFileSystem files, LogConfig config, long afterLsn) throws IOException {
        return open(files, config, afterLsn, () -> System.currentTimeMillis() * 1000);
    }

    public static Opened open(LogFileSystem files, LogConfig config, long afterLsn, LongSupplier clock)
            throws IOException {
        CollectingSink sink = new CollectingSink();
        RecoveryResult result = LogRecovery.recover(files, afterLsn, sink);
        SegmentedLog log = SegmentedLog.open(files, config, result, sink.lastEpoch() + 1, 7, clock);
        return new Opened(log, sink, result);
    }

    public static CollectingSink recoverOnly(LogFileSystem files, long afterLsn) throws IOException {
        CollectingSink sink = new CollectingSink();
        LogRecovery.recover(files, afterLsn, sink);
        return sink;
    }

    public static String describe(RecordReader record) {
        return switch (record.type()) {
            case TUPLE_ADD, TUPLE_REMOVE -> record.type() + " " + record.object() + " " + record.relation() + " "
                    + record.subjectRelation() + " " + record.subject();
            case SYMBOL -> "SYMBOL " + record.symbolId() + " "
                    + new String(record.symbolBytes(), StandardCharsets.UTF_8);
            case GRAPH_CONFIG -> "GRAPH_CONFIG " + record.keyKindCode();
            case EPOCH -> "EPOCH " + record.epochNumber() + " " + record.epochHandoffLsn();
            case SCHEMA -> "SCHEMA " + record.schemaVersion();
            case ERASE -> "ERASE " + record.eraseSymbolId();
            case TXN_COMMIT -> "COMMIT";
        };
    }

    public static RecordBatch tuples(int first, int count) {
        RecordBatch batch = new RecordBatch();
        for (int i = 0; i < count; i++) {
            batch.tuple(RecordType.TUPLE_ADD, first + i, 1, 0, first + i + 1);
        }
        batch.commit();
        return batch;
    }

    public static RecordBatch single(int object, int subject) {
        RecordBatch batch = new RecordBatch();
        batch.autocommitTuple(RecordType.TUPLE_ADD, object, 1, 0, subject);
        return batch;
    }

    public static List<String> describeAll(RecordBatch sealedOrNot) {
        List<String> described = new ArrayList<>();
        RecordReader reader = sealedOrNot.reader();
        while (reader.hasRecord()) {
            if (reader.type() != RecordType.TXN_COMMIT) {
                described.add(describe(reader));
            }
            reader.advance();
        }
        return described;
    }
}
