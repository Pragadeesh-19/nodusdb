package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.memory.MemoryBudget;

import java.lang.foreign.Arena;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

final class TestGraph implements TupleGraph {

    static final int MEMBER = 1;
    static final int ADMIN = 2;
    static final int VIEWER = 3;
    static final int OWNER = 4;

    record Tuple(int object, int relation, int subjectRelation, int subject) {
    }

    private final Arena arena = Arena.ofAuto();
    private final MemoryBudget budget;
    private final EdgeTables direct;
    private final EdgeTables indirect;
    private final ClosureIndex index;
    private final Set<Tuple> tuples = new HashSet<>();
    private RelationCatalog catalog;
    private int version;

    TestGraph(ClosureLimits limits) {
        this(limits, MemoryBudget.unlimited());
    }

    TestGraph(ClosureLimits limits, MemoryBudget budget) {
        this.budget = budget;
        this.direct = new EdgeTables(arena, budget);
        this.indirect = new EdgeTables(arena, budget);
        this.catalog = catalogWith(MEMBER, ADMIN);
        this.index = new ClosureIndex(this, arena, budget, limits);
    }

    static RelationCatalog catalogOf(int version, int... membershipRelations) {
        int[] ids = {MEMBER, ADMIN, VIEWER, OWNER};
        int[] flags = new int[ids.length];
        for (int i = 0; i < ids.length; i++) {
            for (int membership : membershipRelations) {
                if (ids[i] == membership) {
                    flags[i] = RelationCatalog.MEMBERSHIP;
                }
            }
        }
        return RelationCatalog.of(version, new byte[32], new byte[0], ids, new int[]{0, 0, 1, 1},
                new int[]{2, 3, 4, 5}, flags);
    }

    private RelationCatalog catalogWith(int... membershipRelations) {
        version++;
        return catalogOf(version, membershipRelations);
    }

    ClosureIndex index() {
        return index;
    }

    MemoryBudget budget() {
        return budget;
    }

    Set<Tuple> tuples() {
        return tuples;
    }

    void changeMembership(int... membershipRelations) {
        RelationCatalog previous = catalog;
        catalog = catalogWith(membershipRelations);
        index.catalogChanged(previous, catalog);
    }

    @Override
    public EdgeTables direct() {
        return direct;
    }

    @Override
    public EdgeTables indirectOrNull() {
        return indirect;
    }

    @Override
    public RelationCatalog catalog() {
        return catalog;
    }

    @Override
    public int nodeCapacity() {
        return direct.capacity();
    }

    boolean add(int object, int relation, int subjectRelation, int subject) {
        int nodes = Math.max(object, subject) + 1;
        direct.ensureCapacity(nodes);
        indirect.ensureCapacity(nodes);
        boolean added = tablesFor(relation, subjectRelation).add(object, EdgeKey.pack(relation, subjectRelation,
                subject), subject, EdgeKey.pack(relation, subjectRelation, object));
        if (added) {
            tuples.add(new Tuple(object, relation, subjectRelation, subject));
            index.tupleAdded(object, relation, subjectRelation, subject);
        }
        return added;
    }

    boolean remove(int object, int relation, int subjectRelation, int subject) {
        boolean removed = tablesFor(relation, subjectRelation).remove(object, EdgeKey.pack(relation,
                subjectRelation, subject), subject, EdgeKey.pack(relation, subjectRelation, object));
        if (removed) {
            tuples.remove(new Tuple(object, relation, subjectRelation, subject));
            index.tupleRemoved(object, relation, subjectRelation, subject);
        }
        return removed;
    }

    void drain() {
        int slices = 0;
        while (index.hasWork()) {
            index.runSlice();
            if (++slices > 1_000_000) {
                throw new AssertionError("the rebuild did not finish");
            }
        }
    }

    boolean indexedMember(int node, int relation, int subject) {
        if (tuples.contains(new Tuple(node, relation, 0, subject))) {
            return true;
        }
        return index.reachesMember(node, relation, subject);
    }

    Set<Long> storedClosure(int node) {
        Set<Long> keys = new TreeSet<>();
        ClosureStore store = index.store();
        for (int i = 0; i < store.degree(node); i++) {
            keys.add(store.keyAt(node, i));
        }
        return keys;
    }

    Set<Long> naiveClosure(int node) {
        Set<Long> keys = new TreeSet<>();
        for (Tuple tuple : tuples) {
            if (tuple.object() == node && isNesting(tuple)) {
                keys.addAll(naiveClosure(node, tuple.relation()));
            }
        }
        return keys;
    }

    boolean naiveMember(int node, int relation, int subject) {
        if (tuples.contains(new Tuple(node, relation, 0, subject))) {
            return true;
        }
        for (long key : naiveClosure(node, relation)) {
            if (tuples.contains(new Tuple(EdgeKey.id(key), EdgeKey.subjectRelation(key), 0, subject))) {
                return true;
            }
        }
        return false;
    }

    private Set<Long> naiveClosure(int node, int relation) {
        Set<Long> keys = new TreeSet<>();
        if (!catalog.isMembership(relation)) {
            return keys;
        }
        Map<Long, List<Tuple>> children = nestingByParent();
        Set<Long> seen = new HashSet<>();
        Deque<Long> queue = new ArrayDeque<>();
        queue.add(userset(node, relation));
        while (!queue.isEmpty()) {
            for (Tuple tuple : children.getOrDefault(queue.poll(), List.of())) {
                long found = userset(tuple.subject(), tuple.subjectRelation());
                if (seen.add(found)) {
                    keys.add(EdgeKey.pack(relation, tuple.subjectRelation(), tuple.subject()));
                    queue.add(found);
                }
            }
        }
        return keys;
    }

    private Map<Long, List<Tuple>> nestingByParent() {
        Map<Long, List<Tuple>> children = new HashMap<>();
        for (Tuple tuple : tuples) {
            if (isNesting(tuple)) {
                children.computeIfAbsent(userset(tuple.object(), tuple.relation()), key -> new ArrayList<>())
                        .add(tuple);
            }
        }
        return children;
    }

    private static long userset(int node, int relation) {
        return EdgeKey.pack(relation, 0, node);
    }

    private boolean isNesting(Tuple tuple) {
        return tuple.subjectRelation() != 0 && catalog.isMembership(tuple.relation())
                && catalog.isMembership(tuple.subjectRelation());
    }

    private EdgeTables tablesFor(int relation, int subjectRelation) {
        return subjectRelation != 0 || catalog.isTupleset(relation) ? indirect : direct;
    }
}
