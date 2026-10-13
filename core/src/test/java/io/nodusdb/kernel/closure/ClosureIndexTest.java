package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.memory.MemoryBudget;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.nodusdb.kernel.closure.TestGraph.ADMIN;
import static io.nodusdb.kernel.closure.TestGraph.MEMBER;
import static io.nodusdb.kernel.closure.TestGraph.VIEWER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClosureIndexTest {

    private static final int ALICE = 100;
    private static final int BOB = 101;

    private static Set<Long> keys(long... keys) {
        Set<Long> set = new java.util.TreeSet<>();
        for (long key : keys) {
            set.add(key);
        }
        return set;
    }

    private static long entry(int relation, int childRelation, int child) {
        return EdgeKey.pack(relation, childRelation, child);
    }

    @Test
    void aChainIsAnsweredThroughTheClosureOfItsTop() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 2);
        graph.add(2, MEMBER, 0, ALICE);

        assertTrue(graph.index().covers(0, MEMBER));
        assertTrue(graph.indexedMember(0, MEMBER, ALICE));
        assertFalse(graph.indexedMember(0, MEMBER, BOB));
        assertEquals(keys(entry(MEMBER, MEMBER, 1), entry(MEMBER, MEMBER, 2)), graph.storedClosure(0));
    }

    @Test
    void removingTheMiddleLinkOfAChainShrinksEveryAncestor() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 2);
        graph.add(2, MEMBER, MEMBER, 3);
        graph.add(3, MEMBER, 0, ALICE);

        graph.remove(1, MEMBER, MEMBER, 2);

        assertEquals(keys(entry(MEMBER, MEMBER, 1)), graph.storedClosure(0));
        assertEquals(keys(), graph.storedClosure(1));
        assertFalse(graph.indexedMember(0, MEMBER, ALICE));
        assertTrue(graph.indexedMember(2, MEMBER, ALICE));
    }

    @Test
    void aDiamondKeepsTheSharedDescendantUntilTheLastPathGoes() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(0, MEMBER, MEMBER, 2);
        graph.add(1, MEMBER, MEMBER, 3);
        graph.add(2, MEMBER, MEMBER, 3);
        graph.add(3, MEMBER, 0, ALICE);

        graph.remove(1, MEMBER, MEMBER, 3);
        assertTrue(graph.indexedMember(0, MEMBER, ALICE));
        assertTrue(graph.storedClosure(0).contains(entry(MEMBER, MEMBER, 3)));

        graph.remove(2, MEMBER, MEMBER, 3);
        assertFalse(graph.indexedMember(0, MEMBER, ALICE));
        assertEquals(keys(entry(MEMBER, MEMBER, 1), entry(MEMBER, MEMBER, 2)), graph.storedClosure(0));
    }

    @Test
    void aRemovalIsRederivedFromTheBottomUpWhateverOrderTheAncestorsWereLinkedIn() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        int target = 10;
        int leaf = 11;
        graph.add(target, MEMBER, MEMBER, leaf);
        for (int group = 0; group < 3; group++) {
            graph.add(group, MEMBER, MEMBER, target);
        }
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 2);
        graph.remove(0, MEMBER, MEMBER, target);
        graph.remove(1, MEMBER, MEMBER, target);

        graph.remove(target, MEMBER, MEMBER, leaf);

        for (int group = 0; group <= 2; group++) {
            assertFalse(graph.storedClosure(group).contains(entry(MEMBER, MEMBER, leaf)), "group=" + group);
            assertEquals(graph.naiveClosure(group), graph.storedClosure(group), "group=" + group);
        }
    }

    @Test
    void aRemovalInACycleFallsBackToTheSearchAndStaysExact() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 0);
        graph.add(1, MEMBER, MEMBER, 2);
        graph.add(0, MEMBER, MEMBER, 2);

        graph.remove(1, MEMBER, MEMBER, 2);

        for (int group = 0; group <= 2; group++) {
            assertEquals(graph.naiveClosure(group), graph.storedClosure(group), "group=" + group);
        }
        assertTrue(graph.storedClosure(1).contains(entry(MEMBER, MEMBER, 2)));
    }

    @Test
    void aCycleContainsEveryNodeOnItIncludingItself() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 2);
        graph.add(2, MEMBER, MEMBER, 0);
        graph.add(1, MEMBER, 0, ALICE);

        Set<Long> cycle = keys(entry(MEMBER, MEMBER, 0), entry(MEMBER, MEMBER, 1), entry(MEMBER, MEMBER, 2));
        assertEquals(cycle, graph.storedClosure(0));
        assertEquals(cycle, graph.storedClosure(2));
        assertTrue(graph.indexedMember(2, MEMBER, ALICE));

        graph.remove(2, MEMBER, MEMBER, 0);
        assertEquals(keys(entry(MEMBER, MEMBER, 1), entry(MEMBER, MEMBER, 2)), graph.storedClosure(0));
        assertEquals(keys(), graph.storedClosure(2));
        assertFalse(graph.indexedMember(2, MEMBER, ALICE));
    }

    @Test
    void aGroupNestedInItselfHoldsOnlyItself() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 0);

        assertEquals(keys(entry(MEMBER, MEMBER, 0)), graph.storedClosure(0));
        graph.remove(0, MEMBER, MEMBER, 0);
        assertEquals(keys(), graph.storedClosure(0));
    }

    @Test
    void differentRelationsOnOneNodeKeepSeparateClosures() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(0, ADMIN, ADMIN, 2);
        graph.add(1, MEMBER, 0, ALICE);
        graph.add(2, ADMIN, 0, BOB);

        assertTrue(graph.indexedMember(0, MEMBER, ALICE));
        assertFalse(graph.indexedMember(0, MEMBER, BOB));
        assertTrue(graph.indexedMember(0, ADMIN, BOB));
        assertFalse(graph.indexedMember(0, ADMIN, ALICE));
    }

    @Test
    void aRelationThatIsNotMembershipIsNeitherIndexedNorCovered() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, VIEWER, MEMBER, 1);
        graph.add(1, MEMBER, 0, ALICE);

        assertFalse(graph.index().covers(0, VIEWER));
        assertEquals(keys(), graph.storedClosure(0));
        assertTrue(graph.index().covers(1, MEMBER));
    }

    @Test
    void anInsertOverTheWorkBudgetMarksTheAncestorsDirtyAndTheRebuildRestoresThem() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT.withWorkBudget(4));
        for (int child = 1; child <= 6; child++) {
            graph.add(10, MEMBER, MEMBER, child);
        }
        graph.add(0, MEMBER, MEMBER, 10);

        assertFalse(graph.index().covers(0, MEMBER));
        assertTrue(graph.index().hasWork());
        graph.drain();
        assertTrue(graph.index().covers(0, MEMBER));
        assertEquals(graph.naiveClosure(0), graph.storedClosure(0));
        assertEquals(7, graph.storedClosure(0).size());
    }

    @Test
    void aDirtyChildMakesItsNewAncestorsDirty() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT.withWorkBudget(4));
        for (int child = 1; child <= 6; child++) {
            graph.add(10, MEMBER, MEMBER, child);
        }
        graph.add(20, MEMBER, MEMBER, 10);
        assertTrue(graph.index().isDirty(20));

        graph.add(0, MEMBER, MEMBER, 20);

        assertTrue(graph.index().isDirty(0));
        graph.drain();
        assertEquals(graph.naiveClosure(0), graph.storedClosure(0));
    }

    @Test
    void moreAncestorsThanTheBudgetStartAFullPass() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT.withWorkBudget(8));
        for (int parent = 0; parent < 12; parent++) {
            graph.add(parent, MEMBER, MEMBER, 50);
            graph.drain();
        }
        long passes = graph.index().stats().fullPasses();

        graph.add(50, MEMBER, MEMBER, 51);

        assertEquals(passes + 1, graph.index().stats().fullPasses());
        assertTrue(graph.index().isDirty(0));
        graph.drain();
        for (int parent = 0; parent < 12; parent++) {
            assertEquals(graph.naiveClosure(parent), graph.storedClosure(parent), "parent=" + parent);
        }
    }

    @Test
    void theCapParksNodesUntilTheNestingChanges() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT.withCap(3, 0));
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 2);
        graph.add(2, MEMBER, MEMBER, 3);

        assertTrue(graph.index().isDirty(0) && graph.index().isDirty(1) && graph.index().isDirty(2));
        graph.drain();
        assertFalse(graph.index().hasWork());
        assertTrue(graph.index().isDirty(0));
        assertTrue(graph.index().stats().refusals() > 0);
        assertTrue(graph.index().stats().entries() <= 3);

        graph.remove(2, MEMBER, MEMBER, 3);
        assertTrue(graph.index().hasWork());
        graph.drain();
        for (int node = 0; node <= 2; node++) {
            assertFalse(graph.index().isDirty(node), "node=" + node);
            assertEquals(graph.naiveClosure(node), graph.storedClosure(node), "node=" + node);
        }
    }

    @Test
    void aMemoryBudgetThatRefusesGrowthLeavesTheGroupDirtyAndTheTuplesIntact() {
        MemoryBudget budget = MemoryBudget.limitedTo(64L << 20);
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT, budget);
        for (int child = 1; child <= 200; child++) {
            graph.add(0, VIEWER, VIEWER, child);
        }
        budget.charge(budget.limit() - budget.used() - 64);

        graph.changeMembership(MEMBER, ADMIN, VIEWER);
        graph.drain();

        assertTrue(graph.index().isDirty(0));
        assertFalse(graph.index().covers(0, VIEWER));
        assertTrue(graph.index().stats().refusals() > 0);
        assertTrue(graph.index().enabled());
        assertEquals(200, graph.tuples().size());
        assertFalse(graph.index().isDirty(1));
    }

    @Test
    void aSchemaThatChangesMembershipRebuildsEverything() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, VIEWER, VIEWER, 1);
        assertEquals(keys(), graph.storedClosure(0));

        graph.changeMembership(MEMBER, ADMIN, VIEWER);
        assertTrue(graph.index().isDirty(0));
        graph.drain();
        assertEquals(keys(entry(VIEWER, VIEWER, 1)), graph.storedClosure(0));

        graph.changeMembership(MEMBER, ADMIN);
        graph.drain();
        assertEquals(keys(), graph.storedClosure(0));
        assertFalse(graph.index().covers(0, VIEWER));
    }

    @Test
    void aSchemaThatKeepsMembershipStartsNoPass() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);

        graph.changeMembership(MEMBER, ADMIN);

        assertFalse(graph.index().hasWork());
        assertEquals(0, graph.index().stats().fullPasses());
    }

    @Test
    void replacedContentIsRebuiltByAFullPass() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);

        graph.index().contentReplaced();

        assertTrue(graph.index().isDirty(0));
        assertTrue(graph.index().isDirty(1));
        graph.drain();
        assertEquals(keys(entry(MEMBER, MEMBER, 1)), graph.storedClosure(0));
    }

    @Test
    void aDisabledIndexNeverCoversAndStoresNothing() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT.disabled());
        graph.add(0, MEMBER, MEMBER, 1);
        graph.index().contentReplaced();

        assertFalse(graph.index().covers(0, MEMBER));
        assertFalse(graph.index().hasWork());
        assertEquals(0, graph.index().stats().entries());
        assertFalse(graph.index().stats().enabled());
    }

    @Test
    void aDirtyNodeRequestsTheMaintainerOnlyOnceChecksHaveAsked() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT.withWorkBudget(1));
        int[] requests = new int[1];
        graph.index().onDemand(() -> requests[0]++);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 2);
        assertEquals(0, requests[0]);

        assertFalse(graph.index().covers(0, MEMBER));
        assertEquals(1, requests[0]);
        graph.add(5, MEMBER, MEMBER, 1);
        assertTrue(requests[0] > 1);
    }

    @Test
    void statsCountTheWork() {
        TestGraph graph = new TestGraph(ClosureLimits.DEFAULT);
        graph.add(0, MEMBER, MEMBER, 1);
        graph.add(1, MEMBER, MEMBER, 2);
        graph.index().contentReplaced();
        graph.drain();

        ClosureStats stats = graph.index().stats();
        assertTrue(stats.enabled());
        assertFalse(stats.building());
        assertEquals(0, stats.dirtyNodes());
        assertEquals(3, stats.entries());
        assertEquals(2, stats.incrementalUpdates());
        assertEquals(1, stats.fullPasses());
        assertTrue(stats.rebuiltNodes() >= 3);
        assertEquals(ClosureLimits.DEFAULT.minimumCap(), stats.entryCap());
        assertEquals("", stats.failure());
    }
}
