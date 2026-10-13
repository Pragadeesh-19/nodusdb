package io.nodusdb.authz;

import io.nodusdb.authz.NaiveEvaluator.Tuple;
import io.nodusdb.authz.schema.SchemaDocument;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.authz.schema.SchemaParser;
import io.nodusdb.error.CheckDepthException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.closure.ClosureLimits;
import io.nodusdb.kernel.closure.ClosureStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReachabilityCheckTest {

    private static final int UNBOUNDED_DEPTH = 10_000;
    private static final int GROUPS = 30;
    private static final int USERS = 10;
    private static final long SETTLE_NANOS = TimeUnit.SECONDS.toNanos(20);

    private static TupleStore store(ClosureLimits limits, int depthLimit) {
        TupleStore store = TupleStore.open(new GraphKernel(GraphKernel.NO_MEMORY_LIMIT, limits), depthLimit);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        return store;
    }

    private static void nestChain(TupleStore store, int length) {
        TupleTransaction transaction = new TupleTransaction();
        for (int i = 0; i < length; i++) {
            transaction.add("group:g" + i, "member", "group:g" + (i + 1), "member");
        }
        transaction.add("group:g" + length, "member", "user:alice");
        transaction.add("document:readme", "viewer", "group:g0", "member");
        store.write(transaction);
    }

    private static void settle(TupleStore store, String... subjects) {
        long deadline = System.nanoTime() + SETTLE_NANOS;
        for (int group = 0; group < GROUPS; group++) {
            for (String subject : subjects) {
                store.check("group:g" + group, "member", subject);
            }
        }
        while (store.kernel().closureStats().building()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the index never settled: " + store.kernel().closureStats());
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void aChainDeeperThanTheDepthLimitIsAnsweredThroughTheIndex() {
        TupleStore store = store(ClosureLimits.DEFAULT, TupleStore.DEFAULT_DEPTH_LIMIT);
        nestChain(store, 40);

        assertTrue(store.check("document:readme", "view", "user:alice"));
        assertTrue(store.check("group:g0", "member", "user:alice"));
        assertFalse(store.check("document:readme", "view", "user:bob"));
        store.kernel().close();
    }

    @Test
    void theSameChainWithoutTheIndexStillReachesTheDepthLimit() {
        TupleStore store = store(ClosureLimits.DEFAULT.disabled(), TupleStore.DEFAULT_DEPTH_LIMIT);
        nestChain(store, 40);

        assertThrows(CheckDepthException.class, () -> store.check("document:readme", "view", "user:alice"));
        assertFalse(store.kernel().closureStats().enabled());
        store.kernel().close();
    }

    @Test
    void aGroupMadeDirtyByTheBudgetIsAnsweredByTheEvaluatorAndThenRebuilt() {
        TupleStore store = store(ClosureLimits.DEFAULT.withWorkBudget(2), UNBOUNDED_DEPTH);
        nestChain(store, 10);

        assertTrue(store.check("group:g0", "member", "user:alice"));
        settle(store, "user:alice");

        ClosureStats stats = store.kernel().closureStats();
        assertEquals(0, stats.dirtyNodes());
        assertTrue(stats.dirtyMarks() > 0, String.valueOf(stats));
        assertTrue(stats.rebuiltNodes() > 0, String.valueOf(stats));
        assertTrue(store.check("group:g0", "member", "user:alice"));
        store.write(new TupleTransaction().remove("group:g5", "member", "group:g6", "member"));
        assertFalse(store.check("group:g0", "member", "user:alice"));
        assertTrue(store.check("group:g6", "member", "user:alice"));
        store.kernel().close();
    }

    @Test
    void closingTheKernelStopsTheMaintainer() {
        TupleStore store = store(ClosureLimits.DEFAULT.withWorkBudget(1), UNBOUNDED_DEPTH);
        nestChain(store, 200);
        store.check("group:g0", "member", "user:alice");

        store.kernel().close();

        assertFalse(maintainerAlive(), "a maintainer thread outlived its kernel");
    }

    @Test
    void aGraphThatIsNeverCheckedStartsNoMaintainer() {
        TupleStore store = store(ClosureLimits.DEFAULT.withWorkBudget(1), UNBOUNDED_DEPTH);
        int before = maintainerThreads();

        nestChain(store, 50);

        assertTrue(store.kernel().closureStats().building());
        assertTrue(maintainerThreads() <= before);
        store.kernel().close();
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
    void checksAgreeWithTheNaiveEvaluatorWithTheIndexOnOffAndForcedDirty(long seed) {
        SchemaDocument schema = SchemaParser.parse(SchemaFixtures.DOCUMENTS);
        TupleStore indexed = store(ClosureLimits.DEFAULT, UNBOUNDED_DEPTH);
        TupleStore starved = store(ClosureLimits.DEFAULT.withWorkBudget(3), UNBOUNDED_DEPTH);
        TupleStore plain = store(ClosureLimits.DEFAULT.disabled(), UNBOUNDED_DEPTH);
        Set<Tuple> oracle = new HashSet<>();
        Random random = new Random(seed);
        int nestedGrants = 0;
        for (int round = 0; round < 30; round++) {
            for (int op = 0; op < 25; op++) {
                Tuple tuple = randomTuple(random);
                boolean add = random.nextInt(3) != 0;
                TupleTransaction transaction = add
                        ? new TupleTransaction().add(tuple.object(), tuple.relation(), tuple.subject(),
                                tuple.subjectRelation())
                        : new TupleTransaction().remove(tuple.object(), tuple.relation(), tuple.subject(),
                                tuple.subjectRelation());
                if (add) {
                    oracle.add(tuple);
                } else {
                    oracle.remove(tuple);
                }
                indexed.write(transaction);
                starved.write(transaction);
                plain.write(transaction);
            }
            if (round % 5 == 4) {
                settle(starved, allUsers());
            }
            NaiveEvaluator naive = new NaiveEvaluator(schema, oracle);
            for (int question = 0; question < 60; question++) {
                String object = random.nextBoolean() ? "group:g" + random.nextInt(GROUPS)
                        : "document:d" + random.nextInt(4);
                String name = object.startsWith("group") ? "member" : "view";
                String subject = "user:u" + random.nextInt(USERS);
                boolean expected = naive.check(object, name, subject);
                String context = "seed=" + seed + " round=" + round + " " + object + "#" + name + "@" + subject;
                assertEquals(expected, indexed.check(object, name, subject), "indexed " + context);
                assertEquals(expected, starved.check(object, name, subject), "starved " + context);
                assertEquals(expected, plain.check(object, name, subject), "plain " + context);
                if (expected && !oracle.contains(new Tuple(object, name, subject, null))) {
                    nestedGrants++;
                }
            }
        }
        assertTrue(nestedGrants > 20, "nestedGrants=" + nestedGrants);
        assertTrue(indexed.kernel().closureStats().incrementalUpdates() > 0);
        indexed.kernel().close();
        starved.kernel().close();
        plain.kernel().close();
    }

    private static String[] allUsers() {
        String[] users = new String[USERS];
        for (int i = 0; i < USERS; i++) {
            users[i] = "user:u" + i;
        }
        return users;
    }

    private static Tuple randomTuple(Random random) {
        int roll = random.nextInt(10);
        String group = "group:g" + random.nextInt(GROUPS);
        if (roll < 5) {
            return new Tuple(group, "member", "group:g" + random.nextInt(GROUPS), "member");
        }
        if (roll < 8) {
            return new Tuple(group, "member", "user:u" + random.nextInt(USERS), null);
        }
        return new Tuple("document:d" + random.nextInt(4), "viewer", group, "member");
    }

    private static boolean maintainerAlive() {
        long deadline = System.nanoTime() + SETTLE_NANOS;
        while (maintainerThreads() > 0) {
            if (System.nanoTime() > deadline) {
                return true;
            }
            Thread.onSpinWait();
        }
        return false;
    }

    private static int maintainerThreads() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().equals("nodusdb-closure")) {
                count++;
            }
        }
        return count;
    }
}
