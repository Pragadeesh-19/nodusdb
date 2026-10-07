package io.nodusdb.kernel.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IntersectionTest {

    private static final int ROUNDS = 200;
    private static final int MAX_MEMBERS = 300;
    private static final int KEYSPACE = 400;

    @Test
    void emptyAgainstEmptyIsEmpty() {
        assertIntersection(Set.of(), Set.of());
    }

    @Test
    void emptyAgainstPopulatedIsEmptyInBothOrders() {
        assertIntersection(Set.of(), Set.of(1L, 2L, 3L));
    }

    @Test
    void disjointSetsProduceNothing() {
        assertIntersection(Set.of(1L, 2L), Set.of(3L, 4L));
    }

    @Test
    void identicalSetsProduceEveryMember() {
        assertIntersection(Set.of(-5L, 0L, 9L), Set.of(-5L, 0L, 9L));
    }

    @Test
    void partialOverlapIncludingNegativeAndExtremeValues() {
        assertIntersection(Set.of(Long.MIN_VALUE, -1L, 0L, 5L), Set.of(-1L, 5L, 6L, Long.MAX_VALUE));
    }

    @Test
    void selfIntersectionReturnsAllMembers() {
        IndexedSparseSet set = toSparse(Set.of(3L, 1L, 2L));
        long[] out = new long[set.size()];

        int count = set.intersect(set, out);

        assertEquals(3, count);
        assertEquals(Set.of(1L, 2L, 3L), toHashSet(out, count));
    }

    @Test
    void outputBufferSmallerThanSmallerSetIsRejected() {
        IndexedSparseSet small = toSparse(Set.of(1L, 2L));
        IndexedSparseSet large = toSparse(Set.of(1L, 2L, 3L));

        assertThrows(IllegalArgumentException.class, () -> small.intersect(large, new long[1]));
    }

    @Test
    void outputBufferLargerThanNeededIsAccepted() {
        IndexedSparseSet small = toSparse(Set.of(1L));
        IndexedSparseSet large = toSparse(Set.of(1L, 2L));
        long[] out = new long[100];

        int count = small.intersect(large, out);

        assertEquals(1, count);
        assertEquals(1L, out[0]);
    }

    @ParameterizedTest
    @ValueSource(longs = {201L, 202L, 203L, 204L, 205L, 206L, 207L, 208L, 209L, 210L})
    void randomIntersectionsMatchHashSetRetainAll(long seed) {
        Random random = new Random(seed);
        for (int round = 0; round < ROUNDS; round++) {
            assertIntersection(randomSet(random), randomSet(random));
        }
    }

    private static Set<Long> randomSet(Random random) {
        int target = random.nextInt(MAX_MEMBERS + 1);
        Set<Long> members = new HashSet<>();
        while (members.size() < target) {
            members.add((long) random.nextInt(KEYSPACE) - KEYSPACE / 2L);
        }
        return members;
    }

    private static void assertIntersection(Set<Long> a, Set<Long> b) {
        Set<Long> expected = new HashSet<>(a);
        expected.retainAll(b);

        IndexedSparseSet setA = toSparse(a);
        IndexedSparseSet setB = toSparse(b);

        long[] out = new long[Math.max(1, Math.max(a.size(), b.size()))];
        int countAB = setA.intersect(setB, out);
        assertEquals(expected.size(), countAB, "a∩b count");
        assertEquals(expected, toHashSet(out, countAB), "a∩b members");

        int countBA = setB.intersect(setA, out);
        assertEquals(expected.size(), countBA, "b∩a count");
        assertEquals(expected, toHashSet(out, countBA), "b∩a members");
    }

    private static IndexedSparseSet toSparse(Set<Long> members) {
        IndexedSparseSet set = new IndexedSparseSet();
        for (long m : members) {
            set.add(m);
        }
        return set;
    }

    private static Set<Long> toHashSet(long[] values, int count) {
        Set<Long> result = new HashSet<>();
        for (int i = 0; i < count; i++) {
            result.add(values[i]);
        }
        assertEquals(count, result.size(), "intersection emitted duplicates");
        return result;
    }
}
