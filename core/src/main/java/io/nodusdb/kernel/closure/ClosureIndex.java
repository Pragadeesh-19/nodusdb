package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.memory.MemoryBudget;

import java.lang.foreign.Arena;
import java.util.Objects;

public final class ClosureIndex {

    private static final Runnable NO_DEMAND = () -> { };

    private final TupleGraph graph;
    private final ClosureLimits limits;
    private final ClosureStore store;
    private final DirtyNodes dirty = new DirtyNodes();
    private final ClosureUpdater updater;
    private final ClosureRebuild rebuild;
    private volatile Runnable demand = NO_DEMAND;
    private volatile boolean demanded;
    private volatile boolean broken;
    private long version;
    private long incrementalUpdates;
    private long dirtyMarks;
    private long rebuiltNodes;
    private long refusals;
    private long fullPasses;
    private String failure = "";
    private volatile ClosureStats published;

    public ClosureIndex(TupleGraph graph, Arena arena, MemoryBudget budget, ClosureLimits limits) {
        this.graph = Objects.requireNonNull(graph, "graph");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.store = new ClosureStore(arena, budget);
        this.updater = new ClosureUpdater(this, graph, store, dirty);
        this.rebuild = new ClosureRebuild(this, graph, store, dirty);
        publish();
    }

    public void onDemand(Runnable action) {
        demand = Objects.requireNonNull(action, "action");
    }

    public boolean enabled() {
        return limits.enabled() && !broken;
    }

    public boolean covers(int node, int relation) {
        if (!enabled() || !graph.catalog().isMembership(relation)) {
            return false;
        }
        if (!dirty.isDirty(node)) {
            return true;
        }
        demanded = true;
        demand.run();
        return false;
    }

    public boolean reachesMember(int node, int relation, int subject) {
        if (store.degree(node) <= subjectDegree(subject)) {
            return sweepClosure(node, relation, subject);
        }
        return sweepMemberships(node, relation, subject, graph.direct())
                || sweepMemberships(node, relation, subject, graph.indirectOrNull());
    }

    public void tupleAdded(int object, int relation, int subjectRelation, int subject) {
        if (!tracks(relation, subjectRelation)) {
            return;
        }
        try {
            updater.inserted(object, relation, subjectRelation, subject);
        } catch (RuntimeException e) {
            fail(e);
        }
        publish();
    }

    public void tupleRemoved(int object, int relation, int subjectRelation, int subject) {
        if (!tracks(relation, subjectRelation)) {
            return;
        }
        try {
            updater.removed(object, relation, subjectRelation, subject);
        } catch (RuntimeException e) {
            fail(e);
        }
        publish();
    }

    public void catalogChanged(RelationCatalog previous, RelationCatalog next) {
        if (enabled() && next.changesMembershipFrom(previous)) {
            startFullPass();
            publish();
        }
    }

    public void contentReplaced() {
        if (enabled()) {
            startFullPass();
            publish();
        }
    }

    public boolean hasWork() {
        return enabled() && rebuild.hasWork();
    }

    public void runSlice() {
        if (!enabled()) {
            return;
        }
        try {
            rebuild.runSlice(limits.workBudget());
        } catch (RuntimeException e) {
            fail(e);
        }
        publish();
    }

    public ClosureStats stats() {
        return published;
    }

    long version() {
        return version;
    }

    ClosureStore store() {
        return store;
    }

    boolean isDirty(int node) {
        return dirty.isDirty(node);
    }

    void bumpVersion() {
        version++;
    }

    int workBudget() {
        return limits.workBudget();
    }

    long cap() {
        EdgeTables indirect = graph.indirectOrNull();
        long tuples = graph.direct().size() + (indirect == null ? 0 : indirect.size());
        return limits.capFor(tuples);
    }

    void markDirty(int node) {
        dirty.mark(node);
        dirtyMarks++;
        wakeIfDemanded();
    }

    void startFullPass() {
        dirty.startSweep();
        version++;
        fullPasses++;
        wakeIfDemanded();
    }

    void countIncremental() {
        incrementalUpdates++;
    }

    void countRebuilt() {
        rebuiltNodes++;
    }

    void countRefused() {
        refusals++;
    }

    private boolean tracks(int relation, int subjectRelation) {
        if (!enabled() || subjectRelation == 0) {
            return false;
        }
        RelationCatalog catalog = graph.catalog();
        return catalog.isMembership(relation) && catalog.isMembership(subjectRelation);
    }

    private void wakeIfDemanded() {
        if (demanded) {
            demand.run();
        }
    }

    private void fail(RuntimeException e) {
        broken = true;
        failure = e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    private void publish() {
        long pending = dirty.markedCount();
        if (dirty.sweeping()) {
            pending += Math.max(0, graph.nodeCapacity() - dirty.sweepLine());
        }
        published = new ClosureStats(enabled(), hasWork(), pending, store.entries(), cap(), incrementalUpdates,
                dirtyMarks, rebuiltNodes, refusals, fullPasses, failure);
    }

    private int subjectDegree(int subject) {
        EdgeTables indirect = graph.indirectOrNull();
        return graph.direct().inDegree(subject) + (indirect == null ? 0 : indirect.inDegree(subject));
    }

    private boolean sweepClosure(int node, int relation, int subject) {
        EdgeTables direct = graph.direct();
        EdgeTables indirect = graph.indirectOrNull();
        RelationCatalog catalog = graph.catalog();
        int degree = store.degree(node);
        for (int i = 0; i < degree; i++) {
            long key = store.keyAt(node, i);
            if (EdgeKey.relation(key) != relation) {
                continue;
            }
            int member = EdgeKey.subjectRelation(key);
            EdgeTables tables = catalog.isTupleset(member) ? indirect : direct;
            if (tables != null && tables.contains(EdgeKey.id(key), EdgeKey.pack(member, 0, subject))) {
                return true;
            }
        }
        return false;
    }

    private boolean sweepMemberships(int node, int relation, int subject, EdgeTables tables) {
        if (tables == null) {
            return false;
        }
        RelationCatalog catalog = graph.catalog();
        int degree = tables.inDegree(subject);
        for (int i = 0; i < degree; i++) {
            long key = tables.incomingKeyAt(subject, i);
            int membership = EdgeKey.relation(key);
            if (EdgeKey.subjectRelation(key) == 0 && catalog.isMembership(membership)
                    && store.holds(node, relation, membership, EdgeKey.id(key))) {
                return true;
            }
        }
        return false;
    }
}
