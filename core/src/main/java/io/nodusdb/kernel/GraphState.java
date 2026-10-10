package io.nodusdb.kernel;

import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.concurrency.WriteSequence;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.lang.foreign.Arena;

final class GraphState {

    private final Arena arena = Arena.ofAuto();
    private final MemoryBudget budget;
    private final WriteSequence sequence = new WriteSequence();
    private final SymbolTable symbols;
    private final EpochHistory epochs = new EpochHistory();
    private final EdgeTables direct;
    private EdgeTables indirect;
    private volatile RelationCatalog catalog = RelationCatalog.EMPTY;
    private volatile KeyKind keyKind = KeyKind.UNSET;
    private volatile long appliedLsn;
    private volatile long lastCommitMicros;
    private boolean faulted;

    GraphState(MemoryBudget budget) {
        this.budget = budget;
        this.symbols = new SymbolTable(arena, budget);
        this.direct = new EdgeTables(arena, budget);
    }

    long appliedLsn() {
        return appliedLsn;
    }

    void appliedLsn(long lsn) {
        appliedLsn = lsn;
    }

    long lastCommitMicros() {
        return lastCommitMicros;
    }

    void lastCommitMicros(long micros) {
        lastCommitMicros = micros;
    }

    boolean faulted() {
        return faulted;
    }

    void fault() {
        faulted = true;
    }

    MemoryBudget budget() {
        return budget;
    }

    WriteSequence sequence() {
        return sequence;
    }

    SymbolTable symbols() {
        return symbols;
    }

    EpochHistory epochs() {
        return epochs;
    }

    EdgeTables direct() {
        return direct;
    }

    EdgeTables indirectOrNull() {
        return indirect;
    }

    EdgeTables ensureIndirect() {
        if (indirect == null) {
            EdgeTables created = new EdgeTables(arena, budget);
            created.ensureCapacity(direct.capacity());
            indirect = created;
        }
        return indirect;
    }

    EdgeTables tables(Partition partition) {
        return partition == Partition.DIRECT ? direct : indirect;
    }

    RelationCatalog catalog() {
        return catalog;
    }

    void catalog(RelationCatalog next) {
        catalog = next;
    }

    KeyKind keyKind() {
        return keyKind;
    }

    void keyKind(KeyKind kind) {
        keyKind = kind;
    }

    int nodeCapacity() {
        return direct.capacity();
    }

    Partition partitionOf(int relation, int subjectRelation) {
        return subjectRelation != 0 || catalog.isTupleset(relation) ? Partition.INDIRECT : Partition.DIRECT;
    }

    long bytesToHoldNodes(int nodes) {
        long bytes = direct.bytesToHold(nodes);
        return indirect == null ? bytes : bytes + indirect.bytesToHold(nodes);
    }

    void ensureNodes(long requiredNodes) {
        if (requiredNodes <= direct.capacity()) {
            return;
        }
        int nodes = (int) requiredNodes;
        budget.require(bytesToHoldNodes(nodes));
        direct.ensureCapacity(nodes);
        if (indirect != null) {
            indirect.ensureCapacity(nodes);
        }
    }
}
