package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;

import io.nodusdb.kernel.index.StampedLongSet;

import java.util.Arrays;

final class ClosureUpdater {

    private static final int INITIAL_CAPACITY = 16;

    private final ClosureIndex index;
    private final ClosureStore store;
    private final DirtyNodes dirty;
    private final ClosureSearch search;
    private final WorkBudget budget = new WorkBudget();
    private final Usersets ancestors = new Usersets();
    private final Usersets descendants = new Usersets();
    private final StampedLongSet recomputed = new StampedLongSet();
    private int[] removalNodes = new int[INITIAL_CAPACITY];
    private long[] removalKeys = new long[INITIAL_CAPACITY];
    private int removalCount;

    ClosureUpdater(ClosureIndex index, TupleGraph graph, ClosureStore store, DirtyNodes dirty) {
        this.index = index;
        this.store = store;
        this.dirty = dirty;
        this.search = new ClosureSearch(graph);
    }

    void inserted(int parent, int relation, int childRelation, int child) {
        index.bumpVersion();
        int limit = index.workBudget();
        if (!collectAncestors(parent, relation, limit)) {
            index.startFullPass();
            return;
        }
        if (ancestors.count == 0) {
            return;
        }
        if (dirty.isDirty(child) || !collectDescendants(child, childRelation, limit)
                || (long) ancestors.count * descendants.count > limit) {
            markAncestorsDirty();
            return;
        }
        store.beginPlan();
        forEachMissingPair(true);
        if (!store.reservePlan(index.cap())) {
            index.countRefused();
            markAncestorsDirty();
            return;
        }
        forEachMissingPair(false);
        index.countIncremental();
    }

    void removed(int parent, int relation) {
        index.bumpVersion();
        int limit = index.workBudget();
        if (!collectAncestors(parent, relation, limit)) {
            index.startFullPass();
            return;
        }
        budget.reset(limit);
        removalCount = 0;
        recomputed.clear();
        for (int i = 0; i < ancestors.count; i++) {
            int node = ancestors.nodes[i];
            if (!recomputed.add(node) || collectRemovals(node)) {
                continue;
            }
            markAncestorsDirty();
            return;
        }
        for (int i = 0; i < removalCount; i++) {
            long key = removalKeys[i];
            store.remove(removalNodes[i], EdgeKey.relation(key), EdgeKey.subjectRelation(key), EdgeKey.id(key));
        }
        index.countIncremental();
    }

    private boolean collectRemovals(int node) {
        search.start(node);
        if (!search.advance(budget)) {
            return false;
        }
        int degree = store.degree(node);
        for (int i = 0; i < degree; i++) {
            if (!budget.spend(1)) {
                return false;
            }
            long key = store.keyAt(node, i);
            if (!search.isTarget(key)) {
                addRemoval(node, key);
            }
        }
        return true;
    }

    private boolean collectAncestors(int parent, int relation, int limit) {
        ancestors.clear();
        if (!dirty.isDirty(parent)) {
            ancestors.add(parent, relation);
        }
        int degree = store.inDegree(parent);
        if (degree > limit) {
            return false;
        }
        for (int i = 0; i < degree; i++) {
            long key = store.incomingKeyAt(parent, i);
            int ancestor = EdgeKey.id(key);
            if (EdgeKey.subjectRelation(key) == relation && !dirty.isDirty(ancestor)) {
                ancestors.add(ancestor, EdgeKey.relation(key));
            }
        }
        return true;
    }

    private boolean collectDescendants(int child, int relation, int limit) {
        descendants.clear();
        descendants.add(child, relation);
        int degree = store.degree(child);
        if (degree > limit) {
            return false;
        }
        for (int i = 0; i < degree; i++) {
            long key = store.keyAt(child, i);
            if (EdgeKey.relation(key) == relation) {
                descendants.add(EdgeKey.id(key), EdgeKey.subjectRelation(key));
            }
        }
        return true;
    }

    private void forEachMissingPair(boolean planOnly) {
        for (int a = 0; a < ancestors.count; a++) {
            int ancestor = ancestors.nodes[a];
            int ancestorRelation = ancestors.relations[a];
            for (int d = 0; d < descendants.count; d++) {
                int descendant = descendants.nodes[d];
                int descendantRelation = descendants.relations[d];
                if (store.holds(ancestor, ancestorRelation, descendantRelation, descendant)) {
                    continue;
                }
                if (planOnly) {
                    store.planAddition(ancestor, descendant);
                } else {
                    store.add(ancestor, ancestorRelation, descendantRelation, descendant);
                }
            }
        }
    }

    private void markAncestorsDirty() {
        for (int i = 0; i < ancestors.count; i++) {
            index.markDirty(ancestors.nodes[i]);
        }
    }

    private void addRemoval(int node, long key) {
        if (removalCount == removalKeys.length) {
            removalNodes = Arrays.copyOf(removalNodes, removalCount * 2);
            removalKeys = Arrays.copyOf(removalKeys, removalCount * 2);
        }
        removalNodes[removalCount] = node;
        removalKeys[removalCount++] = key;
    }

    private static final class Usersets {

        private int[] nodes = new int[INITIAL_CAPACITY];
        private int[] relations = new int[INITIAL_CAPACITY];
        private int count;

        void clear() {
            count = 0;
        }

        void add(int node, int relation) {
            if (count == nodes.length) {
                nodes = Arrays.copyOf(nodes, count * 2);
                relations = Arrays.copyOf(relations, count * 2);
            }
            nodes[count] = node;
            relations[count++] = relation;
        }
    }
}
