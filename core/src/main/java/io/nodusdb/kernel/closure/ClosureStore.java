package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.adjacency.GrowthPlan;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.memory.MemoryLimitExceededException;

import java.lang.foreign.Arena;

final class ClosureStore {

    private final Arena arena;
    private final MemoryBudget budget;
    private final GrowthPlan plan = new GrowthPlan();
    private volatile EdgeTables tables;
    private int largestPlanned;
    private long plannedAdditions;

    ClosureStore(Arena arena, MemoryBudget budget) {
        this.arena = arena;
        this.budget = budget;
    }

    long entries() {
        EdgeTables current = tables;
        return current == null ? 0 : current.size();
    }

    int degree(int node) {
        EdgeTables current = tables;
        return current == null ? 0 : current.degree(node);
    }

    long keyAt(int node, int index) {
        return tables.outgoingKeyAt(node, index);
    }

    boolean contains(int node, long key) {
        EdgeTables current = tables;
        return current != null && current.contains(node, key);
    }

    int inDegree(int node) {
        EdgeTables current = tables;
        return current == null ? 0 : current.inDegree(node);
    }

    long incomingKeyAt(int node, int index) {
        return tables.incomingKeyAt(node, index);
    }

    boolean holds(int ancestor, int relation, int descendantRelation, int descendant) {
        return contains(ancestor, EdgeKey.pack(relation, descendantRelation, descendant));
    }

    void beginPlan() {
        plan.clear();
        largestPlanned = -1;
        plannedAdditions = 0;
    }

    void planAddition(int ancestor, int descendant) {
        plan.addEdge(ancestor, descendant);
        largestPlanned = Math.max(largestPlanned, Math.max(ancestor, descendant));
        plannedAdditions++;
    }

    boolean reservePlan(long cap) {
        if (plannedAdditions == 0) {
            return true;
        }
        if (plannedAdditions > cap - entries()) {
            return false;
        }
        EdgeTables current = tables;
        if (current == null) {
            try {
                current = new EdgeTables(arena, budget);
            } catch (MemoryLimitExceededException refused) {
                return false;
            }
            tables = current;
        }
        int nodes = largestPlanned + 1;
        long bytes = current.bytesToHold(nodes) + plan.measure(current);
        if (!budget.canCharge(bytes)) {
            return false;
        }
        current.ensureCapacity(nodes);
        plan.reserve(current);
        return true;
    }

    void add(int ancestor, int relation, int descendantRelation, int descendant) {
        tables.addAbsent(ancestor, EdgeKey.pack(relation, descendantRelation, descendant), descendant,
                EdgeKey.pack(relation, descendantRelation, ancestor));
    }

    void remove(int ancestor, int relation, int descendantRelation, int descendant) {
        tables.remove(ancestor, EdgeKey.pack(relation, descendantRelation, descendant), descendant,
                EdgeKey.pack(relation, descendantRelation, ancestor));
    }
}
