package io.nodusdb.kernel;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;

final class RecordApplier {

    private final GraphState state;

    RecordApplier(GraphState state) {
        this.state = state;
    }

    void apply(RecordReader record) {
        RecordType type = record.type();
        switch (type) {
            case TUPLE_ADD -> applyTuple(record, true);
            case TUPLE_REMOVE -> applyTuple(record, false);
            case SYMBOL -> applySymbol(record);
            case GRAPH_CONFIG -> applyKeyKind(record);
            case SCHEMA -> applySchema(record);
            case EPOCH -> state.epochs().record(record.epochNumber(), record.lsn(), record.epochHandoffLsn());
            case TXN_COMMIT -> { }
            case ERASE -> throw new UnsupportedFeatureException(
                    "this log holds an erasure record, which needs a newer version of nodusdb");
        }
    }

    private void applyTuple(RecordReader record, boolean add) {
        applyTuple(add, record.object(), record.relation(), record.subjectRelation(), record.subject());
    }

    void applyTuple(boolean add, int object, int relation, int subjectRelation, int subject) {
        long objectKey = EdgeKey.pack(relation, subjectRelation, subject);
        long subjectKey = EdgeKey.pack(relation, subjectRelation, object);
        Partition partition = state.partitionOf(relation, subjectRelation);
        if (add) {
            state.ensureNodes(Math.max(object, subject) + 1L);
            EdgeTables tables = partition == Partition.DIRECT ? state.direct() : state.ensureIndirect();
            tables.add(object, objectKey, subject, subjectKey);
            return;
        }
        EdgeTables tables = state.tables(partition);
        if (tables != null) {
            tables.remove(object, objectKey, subject, subjectKey);
        }
    }

    void applyAbsentAddition(int object, int relation, int subjectRelation, int subject) {
        EdgeTables tables = state.tables(state.partitionOf(relation, subjectRelation));
        assert tables != null && Math.max(object, subject) < tables.capacity() : "the nodes were not reserved";
        tables.addAbsent(object, EdgeKey.pack(relation, subjectRelation, subject), subject,
                EdgeKey.pack(relation, subjectRelation, object));
    }

    private void applySymbol(RecordReader record) {
        byte[] name = record.symbolBytes();
        state.symbols().append(record.symbolId(), name, 0, name.length);
    }

    private void applyKeyKind(RecordReader record) {
        KeyKind wanted = KeyKind.fromCode(record.keyKindCode());
        KeyKind current = state.keyKind();
        if (current != KeyKind.UNSET && current != wanted) {
            throw new IllegalStateException("graph is keyed by " + current + ", not " + wanted);
        }
        state.keyKind(wanted);
    }

    private void applySchema(RecordReader record) {
        RelationCatalog next = SchemaGuard.parse(record);
        SchemaGuard.requireApplicable(state, state.catalog(), next);
        state.catalog(next);
    }
}
