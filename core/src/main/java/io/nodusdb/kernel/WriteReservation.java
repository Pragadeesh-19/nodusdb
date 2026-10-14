package io.nodusdb.kernel;

import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.adjacency.GrowthPlan;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.symbols.Pseudonym;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordReader;

import java.nio.ByteBuffer;

final class WriteReservation {

    private final GraphState state;
    private final RecordReader reader = new RecordReader();
    private final GrowthPlan direct = new GrowthPlan();
    private final GrowthPlan indirect = new GrowthPlan();
    private final GrowthPlan single = new GrowthPlan();
    private long largestNode;
    private int newSymbols;
    private long symbolBytes;
    private boolean needsIndirect;

    WriteReservation(GraphState state) {
        this.state = state;
    }

    boolean needsNoGrowth(int object, int relation, int subjectRelation, int subject) {
        EdgeTables tables = state.tables(state.partitionOf(relation, subjectRelation));
        if (tables == null || Math.max(object, subject) >= tables.capacity()) {
            return false;
        }
        single.clear();
        single.addEdge(object, subject);
        return single.measure(tables) == 0;
    }

    void prepare(RecordBatch batch) {
        scan(batch.bytes(), 0, batch.size(), false);
    }

    void prepareReplicated(ByteBuffer bytes, int start, int end) {
        scan(bytes, start, end, true);
    }

    private void scan(ByteBuffer bytes, int start, int end, boolean replicated) {
        direct.clear();
        indirect.clear();
        largestNode = -1;
        newSymbols = 0;
        symbolBytes = 0;
        needsIndirect = false;
        RelationCatalog effectiveCatalog = state.catalog();
        KeyKind effectiveKind = state.keyKind();
        long effectiveEpoch = state.epochs().latestEpoch();
        reader.wrap(bytes, start, end);
        while (reader.hasRecord()) {
            switch (reader.type()) {
                case TUPLE_ADD -> scanAddition(effectiveCatalog);
                case TUPLE_REMOVE -> checkNodes();
                case SYMBOL -> scanSymbol();
                case GRAPH_CONFIG -> effectiveKind = scanKeyKind(effectiveKind);
                case SCHEMA -> effectiveCatalog = scanSchema(effectiveCatalog);
                case EPOCH -> effectiveEpoch = scanEpoch(replicated, effectiveEpoch);
                case ERASE -> scanErase(effectiveCatalog);
                case TXN_COMMIT -> { }
            }
            reader.advance();
        }
    }

    void reserve() {
        if (needsIndirect) {
            state.ensureIndirect();
        }
        state.ensureNodes(largestNode + 1);
        state.budget().require(totalBytes());
        perform();
    }

    private void checkNodes() {
        NodeIds.checkValid(reader.object());
        NodeIds.checkValid(reader.subject());
    }

    private void scanAddition(RelationCatalog effective) {
        checkNodes();
        int object = reader.object();
        int subject = reader.subject();
        largestNode = Math.max(largestNode, Math.max(object, subject));
        if (reader.subjectRelation() != 0 || effective.isTupleset(reader.relation())) {
            needsIndirect = true;
            indirect.addEdge(object, subject);
        } else {
            direct.addEdge(object, subject);
        }
    }

    private void scanSymbol() {
        int expected = state.symbols().size() + newSymbols;
        if (reader.symbolId() != expected) {
            throw new IllegalArgumentException("symbol " + reader.symbolId() + " is not the next id " + expected);
        }
        NodeIds.checkValid(reader.symbolId());
        newSymbols++;
        symbolBytes += reader.symbolLength();
    }

    private void scanErase(RelationCatalog effective) {
        int id = reader.eraseSymbolId();
        int known = state.symbols().size() + newSymbols;
        if (id >= known) {
            throw new IllegalArgumentException("symbol " + id + " is not defined");
        }
        byte[] pseudonym = Pseudonym.unpadded(reader.erasePseudonym());
        if (!Pseudonym.isPseudonym(pseudonym)) {
            throw new IllegalArgumentException("the erasure of symbol " + id + " carries a malformed pseudonym");
        }
        if (effective.usesSymbol(id)) {
            throw new SchemaViolationException("symbol " + id + " names a type or relation in the schema");
        }
        symbolBytes += pseudonym.length;
    }

    private KeyKind scanKeyKind(KeyKind effective) {
        KeyKind wanted = KeyKind.fromCode(reader.keyKindCode());
        if (effective != KeyKind.UNSET && effective != wanted) {
            throw new IllegalStateException("graph is keyed by " + effective + ", not " + wanted);
        }
        return wanted;
    }

    private long scanEpoch(boolean replicated, long effective) {
        if (!replicated) {
            throw new IllegalArgumentException("epoch records are written by the log");
        }
        long epoch = reader.epochNumber();
        if (epoch <= effective) {
            throw new IllegalStateException("epoch " + epoch + " does not follow epoch " + effective);
        }
        return epoch;
    }

    private RelationCatalog scanSchema(RelationCatalog effective) {
        RelationCatalog next = SchemaGuard.parse(reader);
        SchemaGuard.requireApplicable(state, effective, next);
        return next;
    }

    private long totalBytes() {
        long bytes = direct.measure(state.direct());
        EdgeTables indirectTables = state.indirectOrNull();
        if (indirectTables != null) {
            bytes += indirect.measure(indirectTables);
        }
        if (newSymbols > 0 || symbolBytes > 0) {
            bytes += state.symbols().bytesToReserve(newSymbols, symbolBytes);
        }
        return bytes;
    }

    private void perform() {
        direct.reserve(state.direct());
        EdgeTables indirectTables = state.indirectOrNull();
        if (indirectTables != null) {
            indirect.reserve(indirectTables);
        }
        if (newSymbols > 0 || symbolBytes > 0) {
            state.symbols().reserve(newSymbols, symbolBytes);
        }
    }
}
