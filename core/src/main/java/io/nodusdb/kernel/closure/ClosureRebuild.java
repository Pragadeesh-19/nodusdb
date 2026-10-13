package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;

import java.util.Arrays;

final class ClosureRebuild {

    private enum Phase { IDLE, SEARCH, REMOVE, ADD }

    private static final int INITIAL_PARKED = 8;

    private final ClosureIndex index;
    private final TupleGraph graph;
    private final ClosureStore store;
    private final DirtyNodes dirty;
    private final ClosureSearch search;
    private final WorkBudget budget = new WorkBudget();
    private Phase phase = Phase.IDLE;
    private int node;
    private boolean onSweepLine;
    private long startedVersion;
    private int cursor;
    private int[] parked = new int[INITIAL_PARKED];
    private int parkedCount;
    private long parkedVersion;

    ClosureRebuild(ClosureIndex index, TupleGraph graph, ClosureStore store, DirtyNodes dirty) {
        this.index = index;
        this.graph = graph;
        this.store = store;
        this.dirty = dirty;
        this.search = new ClosureSearch(graph);
    }

    boolean hasWork() {
        return phase != Phase.IDLE || dirty.sweeping() || dirty.hasQueued()
                || parkedCount > 0 && parkedVersion != index.version();
    }

    void runSlice(int units) {
        budget.reset(units);
        while (!budget.exhausted()) {
            switch (phase) {
                case IDLE -> {
                    if (!pickNode()) {
                        return;
                    }
                }
                case SEARCH -> searchStep();
                case REMOVE -> removeStep();
                case ADD -> addStep();
            }
        }
    }

    private boolean pickNode() {
        releaseParkedIfChanged();
        if (dirty.sweeping()) {
            int line = dirty.sweepLine();
            if (line < graph.nodeCapacity()) {
                begin(line, true);
                return true;
            }
            dirty.finishSweep();
        }
        int next = dirty.pollMarked(budget);
        if (next == DirtyNodes.NONE) {
            return false;
        }
        begin(next, false);
        return true;
    }

    private void begin(int target, boolean sweep) {
        node = target;
        onSweepLine = sweep;
        startedVersion = index.version();
        search.start(target);
        phase = Phase.SEARCH;
    }

    private boolean restartIfChanged() {
        if (startedVersion == index.version()) {
            return false;
        }
        budget.spend(1);
        begin(node, onSweepLine && dirty.sweeping() && dirty.sweepLine() == node);
        return true;
    }

    private void searchStep() {
        if (restartIfChanged()) {
            return;
        }
        if (search.advance(budget)) {
            cursor = store.degree(node) - 1;
            phase = Phase.REMOVE;
        }
    }

    private void removeStep() {
        if (restartIfChanged()) {
            return;
        }
        while (cursor >= 0) {
            if (!budget.spend(1)) {
                return;
            }
            long key = store.keyAt(node, cursor--);
            if (!search.isTarget(key)) {
                store.remove(node, EdgeKey.relation(key), EdgeKey.subjectRelation(key), EdgeKey.id(key));
            }
        }
        cursor = 0;
        phase = Phase.ADD;
    }

    private void addStep() {
        if (restartIfChanged()) {
            return;
        }
        int total = search.targetCount();
        if (cursor >= total) {
            finish();
            return;
        }
        int end = cursor;
        store.beginPlan();
        while (end < total && budget.spend(1)) {
            long key = search.targetAt(end++);
            if (!store.contains(node, key)) {
                store.planAddition(node, EdgeKey.id(key));
            }
        }
        if (!store.reservePlan(index.cap())) {
            park();
            return;
        }
        for (int i = cursor; i < end; i++) {
            long key = search.targetAt(i);
            if (!store.contains(node, key)) {
                store.add(node, EdgeKey.relation(key), EdgeKey.subjectRelation(key), EdgeKey.id(key));
            }
        }
        cursor = end;
        if (cursor >= total) {
            finish();
        }
    }

    private void finish() {
        dirty.clear(node);
        advanceSweepPast();
        index.countRebuilt();
        phase = Phase.IDLE;
    }

    private void park() {
        dirty.markWithoutQueueing(node);
        advanceSweepPast();
        if (parkedCount == 0) {
            parkedVersion = index.version();
        }
        if (parkedCount == parked.length) {
            parked = Arrays.copyOf(parked, parkedCount * 2);
        }
        parked[parkedCount++] = node;
        index.countRefused();
        phase = Phase.IDLE;
    }

    private void advanceSweepPast() {
        if (onSweepLine && dirty.sweeping() && dirty.sweepLine() == node) {
            dirty.advanceSweep();
        }
    }

    private void releaseParkedIfChanged() {
        if (parkedCount == 0 || parkedVersion == index.version()) {
            return;
        }
        for (int i = 0; i < parkedCount; i++) {
            dirty.enqueue(parked[i]);
        }
        parkedCount = 0;
    }
}
