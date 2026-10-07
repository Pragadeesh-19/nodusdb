package io.nodusdb.kernel;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.adjacency.Headroom;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordReader;

final class WriteReservation {

    private final GraphState state;
    private final RecordReader reader = new RecordReader();
    private final AdditionCounter directOut = new AdditionCounter();
    private final AdditionCounter directIn = new AdditionCounter();
    private final AdditionCounter indirectOut = new AdditionCounter();
    private final AdditionCounter indirectIn = new AdditionCounter();
    private final Headroom directOutHeadroom = new Headroom();
    private final Headroom directInHeadroom = new Headroom();
    private final Headroom indirectOutHeadroom = new Headroom();
    private final Headroom indirectInHeadroom = new Headroom();
    private final Headroom singleOutgoing = new Headroom();
    private final Headroom singleIncoming = new Headroom();
    private long largestNode;
    private int newSymbols;
    private long newSymbolBytes;
    private boolean needsIndirect;

    WriteReservation(GraphState state) {
        this.state = state;
    }

    boolean needsNoGrowth(int object, int relation, int subjectRelation, int subject) {
        EdgeTables tables = state.tables(state.partitionOf(relation, subjectRelation));
        if (tables == null || Math.max(object, subject) >= tables.capacity()) {
            return false;
        }
        singleOutgoing.clear();
        singleIncoming.clear();
        tables.accumulateOutgoing(object, 1, singleOutgoing);
        tables.accumulateIncoming(subject, 1, singleIncoming);
        return singleOutgoing.isEmpty() && singleIncoming.isEmpty()
                || tables.bytesToReserve(singleOutgoing, singleIncoming) == 0;
    }

    void prepare(RecordBatch batch) {
        directOut.clear();
        directIn.clear();
        indirectOut.clear();
        indirectIn.clear();
        largestNode = -1;
        newSymbols = 0;
        newSymbolBytes = 0;
        needsIndirect = false;
        RelationCatalog effectiveCatalog = state.catalog();
        KeyKind effectiveKind = state.keyKind();
        reader.wrap(batch.bytes(), 0, batch.size());
        while (reader.hasRecord()) {
            switch (reader.type()) {
                case TUPLE_ADD -> scanAddition(effectiveCatalog);
                case TUPLE_REMOVE -> checkNodes();
                case SYMBOL -> scanSymbol();
                case GRAPH_CONFIG -> effectiveKind = scanKeyKind(effectiveKind);
                case SCHEMA -> effectiveCatalog = scanSchema(effectiveCatalog);
                case EPOCH -> throw new IllegalArgumentException("epoch records are written by the log");
                case ERASE -> throw new UnsupportedFeatureException("erasure is not available in this version");
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
        accumulate();
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
            indirectOut.add(object);
            indirectIn.add(subject);
        } else {
            directOut.add(object);
            directIn.add(subject);
        }
    }

    private void scanSymbol() {
        int expected = state.symbols().size() + newSymbols;
        if (reader.symbolId() != expected) {
            throw new IllegalArgumentException("symbol " + reader.symbolId() + " is not the next id " + expected);
        }
        NodeIds.checkValid(reader.symbolId());
        newSymbols++;
        newSymbolBytes += reader.symbolLength();
    }

    private KeyKind scanKeyKind(KeyKind effective) {
        KeyKind wanted = KeyKind.fromCode(reader.keyKindCode());
        if (effective != KeyKind.UNSET && effective != wanted) {
            throw new IllegalStateException("graph is keyed by " + effective + ", not " + wanted);
        }
        return wanted;
    }

    private RelationCatalog scanSchema(RelationCatalog effective) {
        RelationCatalog next = SchemaGuard.parse(reader);
        SchemaGuard.requireApplicable(state, effective, next);
        return next;
    }

    private void accumulate() {
        EdgeTables direct = state.direct();
        accumulateRuns(directOut, directOutHeadroom, direct, true);
        accumulateRuns(directIn, directInHeadroom, direct, false);
        EdgeTables indirect = state.indirectOrNull();
        if (indirect != null) {
            accumulateRuns(indirectOut, indirectOutHeadroom, indirect, true);
            accumulateRuns(indirectIn, indirectInHeadroom, indirect, false);
        }
    }

    private static void accumulateRuns(AdditionCounter counter, Headroom headroom, EdgeTables tables,
                                       boolean outgoing) {
        headroom.clear();
        counter.sort();
        for (int index = 0; index < counter.size(); ) {
            int run = counter.runLengthAt(index);
            if (outgoing) {
                tables.accumulateOutgoing(counter.nodeAt(index), run, headroom);
            } else {
                tables.accumulateIncoming(counter.nodeAt(index), run, headroom);
            }
            index += run;
        }
    }

    private long totalBytes() {
        long bytes = state.direct().bytesToReserve(directOutHeadroom, directInHeadroom);
        EdgeTables indirect = state.indirectOrNull();
        if (indirect != null) {
            bytes += indirect.bytesToReserve(indirectOutHeadroom, indirectInHeadroom);
        }
        if (newSymbols > 0) {
            bytes += state.symbols().bytesToReserve(newSymbols, newSymbolBytes);
        }
        return bytes;
    }

    private void perform() {
        state.direct().reserve(directOutHeadroom, directInHeadroom);
        EdgeTables indirect = state.indirectOrNull();
        if (indirect != null) {
            indirect.reserve(indirectOutHeadroom, indirectInHeadroom);
        }
        if (newSymbols > 0) {
            state.symbols().reserve(newSymbols, newSymbolBytes);
        }
    }
}
