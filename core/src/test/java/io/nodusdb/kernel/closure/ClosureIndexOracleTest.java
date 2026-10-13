package io.nodusdb.kernel.closure;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;
import java.util.Set;

import static io.nodusdb.kernel.closure.TestGraph.ADMIN;
import static io.nodusdb.kernel.closure.TestGraph.MEMBER;
import static io.nodusdb.kernel.closure.TestGraph.VIEWER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClosureIndexOracleTest {

    private static final int GROUPS = 24;
    private static final int USERS = 12;
    private static final int OPERATIONS = 700;
    private static final int[] RELATIONS = {MEMBER, ADMIN, VIEWER};

    private static final class Coverage {
        private long cleanClosures;
        private long coveredQuestions;
        private long grantedThroughNesting;
        private long dirtySeen;
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void everyCleanNodeMatchesTheNaiveClosureWithTheDefaultBudget(long seed) {
        Coverage coverage = run(seed, ClosureLimits.DEFAULT);

        assertTrue(coverage.cleanClosures > 1_000 && coverage.grantedThroughNesting > 50,
                "cleanClosures=" + coverage.cleanClosures + " nested=" + coverage.grantedThroughNesting);
    }

    @ParameterizedTest
    @ValueSource(longs = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
    void everyCleanNodeMatchesTheNaiveClosureWhenTheBudgetForcesDirtyNodes(long seed) {
        Coverage coverage = run(seed, ClosureLimits.DEFAULT.withWorkBudget(4));

        assertTrue(coverage.dirtySeen > 100 && coverage.cleanClosures > 100,
                "dirty=" + coverage.dirtySeen + " clean=" + coverage.cleanClosures);
    }

    @ParameterizedTest
    @ValueSource(longs = {21, 22, 23, 24, 25})
    void everyCleanNodeMatchesTheNaiveClosureWhenTheCapRefusesGrowth(long seed) {
        Coverage coverage = run(seed, ClosureLimits.DEFAULT.withCap(6, 0));

        assertTrue(coverage.dirtySeen > 100, "dirty=" + coverage.dirtySeen);
    }

    private static Coverage run(long seed, ClosureLimits limits) {
        Random random = new Random(seed);
        TestGraph graph = new TestGraph(limits);
        Coverage coverage = new Coverage();
        for (int op = 0; op < OPERATIONS; op++) {
            String step = apply(graph, random);
            if (random.nextInt(5) == 0) {
                runSlices(graph, random.nextInt(4));
            }
            if (random.nextInt(50) == 0) {
                graph.drain();
            }
            verify(graph, random, coverage, "seed=" + seed + " op=" + op + " " + step);
        }
        graph.drain();
        verify(graph, random, coverage, "seed=" + seed + " after drain");
        return coverage;
    }

    private static String apply(TestGraph graph, Random random) {
        int roll = random.nextInt(100);
        int group = random.nextInt(GROUPS);
        int relation = RELATIONS[random.nextInt(RELATIONS.length)];
        if (roll < 2) {
            if (random.nextBoolean()) {
                graph.changeMembership(MEMBER, ADMIN);
            } else {
                graph.changeMembership(MEMBER, ADMIN, VIEWER);
            }
            return "schema";
        }
        boolean add = roll < 65;
        if (roll % 3 == 0) {
            int user = GROUPS + random.nextInt(USERS);
            return (add ? graph.add(group, relation, 0, user) : graph.remove(group, relation, 0, user))
                    + " direct " + group + "#" + relation + "@" + user;
        }
        int child = random.nextInt(GROUPS);
        int childRelation = RELATIONS[random.nextInt(2)];
        return (add ? graph.add(group, relation, childRelation, child) : graph.remove(group, relation, childRelation,
                child)) + " nest " + group + "#" + relation + "@" + child + "#" + childRelation;
    }

    private static void runSlices(TestGraph graph, int slices) {
        for (int i = 0; i < slices && graph.index().hasWork(); i++) {
            graph.index().runSlice();
        }
    }

    private static void verify(TestGraph graph, Random random, Coverage coverage, String context) {
        assertTrue(graph.index().enabled(), context + " " + graph.index().stats().failure());
        for (int node = 0; node < GROUPS; node++) {
            if (graph.index().isDirty(node)) {
                coverage.dirtySeen++;
                continue;
            }
            Set<Long> expected = graph.naiveClosure(node);
            assertEquals(expected, graph.storedClosure(node), context + " node=" + node);
            if (!expected.isEmpty()) {
                coverage.cleanClosures++;
            }
        }
        for (int question = 0; question < 20; question++) {
            int node = random.nextInt(GROUPS);
            int relation = RELATIONS[random.nextInt(RELATIONS.length)];
            int subject = GROUPS + random.nextInt(USERS);
            if (!graph.index().covers(node, relation)) {
                continue;
            }
            boolean expected = graph.naiveMember(node, relation, subject);
            assertEquals(expected, graph.indexedMember(node, relation, subject),
                    context + " question " + node + "#" + relation + "@" + subject);
            coverage.coveredQuestions++;
            if (expected && !graph.tuples().contains(new TestGraph.Tuple(node, relation, 0, subject))) {
                coverage.grantedThroughNesting++;
            }
        }
    }
}
