package io.nodusdb.kernel.changes;

import io.nodusdb.log.record.RecordReader;

public final class ChangeExtractor {

    private ChangeExtractor() {
    }

    public static boolean extractSealed(RecordReader record, EntrySink sink) {
        boolean commitPoint = record.isCommitPoint();
        return extract(record, record.lsn(), commitPoint ? record.commitMicros() : 0, sink);
    }

    public static boolean extract(RecordReader record, long lsn, long commitMicros, EntrySink sink) {
        switch (record.type()) {
            case TUPLE_ADD -> tuple(record, lsn, ChangeEntry.ADD, sink);
            case TUPLE_REMOVE -> tuple(record, lsn, ChangeEntry.REMOVE, sink);
            case SCHEMA -> sink.entry(lsn, ChangeEntry.meta(ChangeEntry.SCHEMA, 0, 0), record.schemaVersion());
            case EPOCH -> sink.entry(lsn, ChangeEntry.meta(ChangeEntry.EPOCH, 0, 0), record.epochNumber());
            case ERASE -> sink.entry(lsn, ChangeEntry.meta(ChangeEntry.ERASE, 0, 0), record.eraseSymbolId());
            case SYMBOL, GRAPH_CONFIG, TXN_COMMIT -> {
            }
        }
        if (!record.isCommitPoint()) {
            return false;
        }
        sink.entry(lsn, ChangeEntry.meta(ChangeEntry.COMMIT, 0, 0), commitMicros);
        return true;
    }

    private static void tuple(RecordReader record, long lsn, int kind, EntrySink sink) {
        sink.entry(lsn, ChangeEntry.meta(kind, record.relation(), record.subjectRelation()),
                ChangeEntry.ids(record.object(), record.subject()));
    }
}
