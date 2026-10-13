package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;

import java.util.Arrays;

final class ClosureRederivation {

    enum Result { DONE, NOT_APPLICABLE, ABORTED }

    private static final int REACHED = 1;
    private static final int LOST = 0;
    private static final int UNKNOWN = -1;
    private static final int INITIAL_CAPACITY = 16;

    private final TupleGraph graph;
    private final ClosureStore store;
    private final DirtyNodes dirty;
    private long[] order = new long[INITIAL_CAPACITY];

    ClosureRederivation(TupleGraph graph, ClosureStore store, DirtyNodes dirty) {
        this.graph = graph;
        this.store = store;
        this.dirty = dirty;
    }

    Result rederive(Usersets ancestors, Usersets candidates, WorkBudget budget) {
        int count = ancestors.count();
        if (order.length < count) {
            order = new long[Math.max(count, order.length * 2)];
        }
        for (int i = 0; i < count; i++) {
            int node = ancestors.node(i);
            int relation = ancestors.relation(i);
            if (!budget.spend(1) || store.holds(node, relation, relation, node)) {
                return Result.NOT_APPLICABLE;
            }
            int size = closureSize(node, relation, budget);
            if (size < 0) {
                return Result.NOT_APPLICABLE;
            }
            order[i] = ((long) size << Integer.SIZE) | i;
        }
        Arrays.sort(order, 0, count);
        for (int k = 0; k < count; k++) {
            int index = (int) order[k];
            if (!rederiveOne(ancestors.node(index), ancestors.relation(index), candidates, budget)) {
                return Result.ABORTED;
            }
        }
        return Result.DONE;
    }

    private boolean rederiveOne(int node, int relation, Usersets candidates, WorkBudget budget) {
        for (int d = 0; d < candidates.count(); d++) {
            int descendant = candidates.node(d);
            int descendantRelation = candidates.relation(d);
            if (!store.holds(node, relation, descendantRelation, descendant)) {
                continue;
            }
            int reached = stillReached(node, relation, descendant, descendantRelation, budget);
            if (reached == UNKNOWN) {
                return false;
            }
            if (reached == LOST) {
                store.remove(node, relation, descendantRelation, descendant);
            }
        }
        return true;
    }

    private int stillReached(int node, int relation, int descendant, int descendantRelation, WorkBudget budget) {
        EdgeTables indirect = graph.indirectOrNull();
        if (indirect == null) {
            return LOST;
        }
        RelationCatalog catalog = graph.catalog();
        int degree = indirect.degree(node);
        for (int i = 0; i < degree; i++) {
            if (!budget.spend(1)) {
                return UNKNOWN;
            }
            long key = indirect.outgoingKeyAt(node, i);
            int childRelation = EdgeKey.subjectRelation(key);
            if (EdgeKey.relation(key) != relation || childRelation == 0 || !catalog.isMembership(childRelation)) {
                continue;
            }
            int child = EdgeKey.id(key);
            if (dirty.isDirty(child)) {
                return UNKNOWN;
            }
            if (child == descendant && childRelation == descendantRelation
                    || store.holds(child, childRelation, descendantRelation, descendant)) {
                return REACHED;
            }
        }
        return LOST;
    }

    private int closureSize(int node, int relation, WorkBudget budget) {
        int degree = store.degree(node);
        if (!budget.spend(degree)) {
            return -1;
        }
        int size = 0;
        for (int i = 0; i < degree; i++) {
            if (EdgeKey.relation(store.keyAt(node, i)) == relation) {
                size++;
            }
        }
        return size;
    }
}
