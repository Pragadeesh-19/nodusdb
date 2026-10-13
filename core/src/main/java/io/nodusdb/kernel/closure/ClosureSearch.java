package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.index.StampedLongSet;

import java.util.Arrays;

final class ClosureSearch {

    private static final int INITIAL_CAPACITY = 16;

    private final TupleGraph graph;
    private final StampedLongSet visited = new StampedLongSet();
    private final StampedLongSet targetSet = new StampedLongSet();
    private int[] roots = new int[INITIAL_CAPACITY];
    private long[] queue = new long[INITIAL_CAPACITY];
    private long[] targets = new long[INITIAL_CAPACITY];
    private int node;
    private int rootCount;
    private int rootCursor;
    private boolean rootsFound;
    private int currentRoot;
    private int head;
    private int tail;
    private long expanding;
    private int expandCursor;
    private boolean expandingSet;
    private int targetCount;
    private boolean done;

    ClosureSearch(TupleGraph graph) {
        this.graph = graph;
    }

    void start(int startNode) {
        node = startNode;
        rootCount = 0;
        rootCursor = 0;
        rootsFound = false;
        currentRoot = -1;
        head = 0;
        tail = 0;
        expandingSet = false;
        targetCount = 0;
        targetSet.clear();
        done = false;
    }

    int targetCount() {
        return targetCount;
    }

    long targetAt(int index) {
        return targets[index];
    }

    boolean isTarget(long key) {
        return targetSet.contains(key);
    }

    boolean advance(WorkBudget budget) {
        EdgeTables indirect = graph.indirectOrNull();
        if (indirect == null) {
            done = true;
            return true;
        }
        RelationCatalog catalog = graph.catalog();
        while (!done) {
            if (budget.exhausted()) {
                return false;
            }
            if (!rootsFound) {
                findRoots(indirect, catalog, budget);
            } else if (expandingSet) {
                expand(indirect, catalog, budget);
            } else if (head < tail) {
                expanding = queue[head++];
                expandCursor = 0;
                expandingSet = true;
            } else if (currentRoot + 1 < rootCount) {
                beginRoot(roots[++currentRoot]);
            } else {
                done = true;
            }
        }
        return true;
    }

    private void findRoots(EdgeTables indirect, RelationCatalog catalog, WorkBudget budget) {
        int degree = indirect.degree(node);
        while (rootCursor < degree) {
            if (!budget.spend(1)) {
                return;
            }
            long key = indirect.outgoingKeyAt(node, rootCursor++);
            int relation = EdgeKey.relation(key);
            if (EdgeKey.subjectRelation(key) != 0 && catalog.isMembership(relation) && !hasRoot(relation)) {
                if (rootCount == roots.length) {
                    roots = Arrays.copyOf(roots, rootCount * 2);
                }
                roots[rootCount++] = relation;
            }
        }
        rootsFound = true;
    }

    private boolean hasRoot(int relation) {
        for (int i = 0; i < rootCount; i++) {
            if (roots[i] == relation) {
                return true;
            }
        }
        return false;
    }

    private void beginRoot(int relation) {
        visited.clear();
        head = 0;
        tail = 0;
        enqueue(userset(node, relation));
    }

    private void expand(EdgeTables indirect, RelationCatalog catalog, WorkBudget budget) {
        int from = usersetNode(expanding);
        int relation = usersetRelation(expanding);
        int degree = indirect.degree(from);
        int rootRelation = roots[currentRoot];
        while (expandCursor < degree) {
            if (!budget.spend(1)) {
                return;
            }
            long key = indirect.outgoingKeyAt(from, expandCursor++);
            int childRelation = EdgeKey.subjectRelation(key);
            if (EdgeKey.relation(key) != relation || childRelation == 0 || !catalog.isMembership(childRelation)) {
                continue;
            }
            int child = EdgeKey.id(key);
            long found = userset(child, childRelation);
            if (visited.add(found)) {
                addTarget(EdgeKey.pack(rootRelation, childRelation, child));
                enqueue(found);
            }
        }
        expandingSet = false;
    }

    private void enqueue(long userset) {
        if (tail == queue.length) {
            if (head > 0) {
                System.arraycopy(queue, head, queue, 0, tail - head);
                tail -= head;
                head = 0;
            } else {
                queue = Arrays.copyOf(queue, queue.length * 2);
            }
        }
        queue[tail++] = userset;
    }

    private void addTarget(long key) {
        targetSet.add(key);
        if (targetCount == targets.length) {
            targets = Arrays.copyOf(targets, targetCount * 2);
        }
        targets[targetCount++] = key;
    }

    private static long userset(int node, int relation) {
        return ((long) relation << Integer.SIZE) | (node & 0xFFFF_FFFFL);
    }

    private static int usersetNode(long userset) {
        return (int) userset;
    }

    private static int usersetRelation(long userset) {
        return (int) (userset >>> Integer.SIZE);
    }
}
