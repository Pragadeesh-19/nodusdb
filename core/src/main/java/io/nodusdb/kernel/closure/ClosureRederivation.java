package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;

final class ClosureRederivation {

    enum Result { DONE, NOT_APPLICABLE, ABORTED }

    private enum Reach { REACHED, LOST, UNKNOWN }

    private final TupleGraph graph;
    private final ClosureStore store;
    private final DirtyNodes dirty;

    ClosureRederivation(TupleGraph graph, ClosureStore store, DirtyNodes dirty) {
        this.graph = graph;
        this.store = store;
        this.dirty = dirty;
    }

    Result rederive(Usersets ancestors, Usersets candidates, WorkBudget budget) {
        for (int i = 0; i < ancestors.count(); i++) {
            int node = ancestors.node(i);
            int relation = ancestors.relation(i);
            if (!budget.spend(1) || store.holds(node, relation, relation, node)) {
                return Result.NOT_APPLICABLE;
            }
        }
        boolean changed = true;
        while (changed) {
            changed = false;
            for (int i = 0; i < ancestors.count(); i++) {
                Reach outcome = dropUnreached(ancestors.node(i), ancestors.relation(i), candidates, budget);
                if (outcome == Reach.UNKNOWN) {
                    return Result.ABORTED;
                }
                changed |= outcome == Reach.LOST;
            }
        }
        return Result.DONE;
    }

    private Reach dropUnreached(int node, int relation, Usersets candidates, WorkBudget budget) {
        Reach dropped = Reach.REACHED;
        for (int d = 0; d < candidates.count(); d++) {
            int descendant = candidates.node(d);
            int descendantRelation = candidates.relation(d);
            if (!store.holds(node, relation, descendantRelation, descendant)) {
                continue;
            }
            Reach reach = reach(node, relation, descendant, descendantRelation, budget);
            if (reach == Reach.UNKNOWN) {
                return Reach.UNKNOWN;
            }
            if (reach == Reach.LOST) {
                store.remove(node, relation, descendantRelation, descendant);
                dropped = Reach.LOST;
            }
        }
        return dropped;
    }

    private Reach reach(int node, int relation, int descendant, int descendantRelation, WorkBudget budget) {
        EdgeTables indirect = graph.indirectOrNull();
        if (indirect == null) {
            return Reach.LOST;
        }
        RelationCatalog catalog = graph.catalog();
        int degree = indirect.degree(node);
        for (int i = 0; i < degree; i++) {
            if (!budget.spend(1)) {
                return Reach.UNKNOWN;
            }
            long key = indirect.outgoingKeyAt(node, i);
            int childRelation = EdgeKey.subjectRelation(key);
            if (EdgeKey.relation(key) != relation || childRelation == 0 || !catalog.isMembership(childRelation)) {
                continue;
            }
            int child = EdgeKey.id(key);
            if (dirty.isDirty(child)) {
                return Reach.UNKNOWN;
            }
            if (child == descendant && childRelation == descendantRelation
                    || store.holds(child, childRelation, descendantRelation, descendant)) {
                return Reach.REACHED;
            }
        }
        return Reach.LOST;
    }
}
