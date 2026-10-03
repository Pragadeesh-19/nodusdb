package io.nodusdb.kernel;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TraversalTest {

    @Test
    void commonNeighborsOfTwoLowDegreeNodes() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        kernel.addEdge(0L, 2L);
        kernel.addEdge(0L, 3L);
        kernel.addEdge(4L, 2L);
        kernel.addEdge(4L, 3L);
        kernel.addEdge(4L, 5L);
        long[] out = new long[3];

        int count = kernel.commonNeighbors(0L, 4L, out);

        assertEquals(Set.of(2L, 3L), toSet(out, count));
    }

    @Test
    void commonNeighborsOfLowAndHighDegreeNodes() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 100; v < 120; v++) {
            kernel.addEdge(0L, v);
        }
        kernel.addEdge(4L, 105L);
        kernel.addEdge(4L, 7L);
        long[] out = new long[20];

        assertEquals(Set.of(105L), toSet(out, kernel.commonNeighbors(0L, 4L, out)));
        assertEquals(Set.of(105L), toSet(out, kernel.commonNeighbors(4L, 0L, out)));
    }

    @Test
    void commonNeighborsOfTwoHighDegreeNodes() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 100; v < 120; v++) {
            kernel.addEdge(0L, v);
        }
        for (long v = 110; v < 130; v++) {
            kernel.addEdge(1L, v);
        }
        long[] out = new long[20];

        int count = kernel.commonNeighbors(0L, 1L, out);

        assertEquals(10, count);
        assertEquals(Set.of(110L, 111L, 112L, 113L, 114L, 115L, 116L, 117L, 118L, 119L), toSet(out, count));
    }

    @Test
    void commonNeighborsWithUnknownOrEmptyNodesIsEmpty() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        long[] out = new long[2];

        assertEquals(0, kernel.commonNeighbors(0L, 999_999L, out));
        assertEquals(0, kernel.commonNeighbors(888_888L, 999_999L, out));
    }

    @Test
    void commonNeighborsRejectsOutputSmallerThanSmallerDegree() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        kernel.addEdge(0L, 2L);
        kernel.addEdge(3L, 1L);
        kernel.addEdge(3L, 2L);

        assertThrows(IllegalArgumentException.class, () -> kernel.commonNeighbors(0L, 3L, new long[1]));
    }

    @Test
    void kHopHonoursDepthAndExcludesStart() {
        GraphKernel kernel = new GraphKernel();
        for (long u = 0; u < 9; u++) {
            kernel.addEdge(u, u + 1);
        }
        long[] out = new long[16];

        assertEquals(Set.of(1L), toSet(out, kernel.kHop(0L, 1, out)));
        assertEquals(Set.of(1L, 2L), toSet(out, kernel.kHop(0L, 2, out)));
        assertEquals(Set.of(1L, 2L, 3L, 4L), toSet(out, kernel.kHop(0L, 4, out)));
    }

    @Test
    void kHopOnCycleDoesNotDuplicateOrIncludeStart() {
        GraphKernel kernel = new GraphKernel();
        for (long u = 0; u < 5; u++) {
            kernel.addEdge(u, (u + 1) % 5);
        }
        long[] out = new long[8];

        int count = kernel.kHop(0L, 10, out);

        assertEquals(4, count);
        assertEquals(Set.of(1L, 2L, 3L, 4L), toSet(out, count));
    }

    @Test
    void kHopFollowsOutgoingEdgesOnly() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        kernel.addEdge(2L, 0L);
        long[] out = new long[4];

        assertEquals(Set.of(1L), toSet(out, kernel.kHop(0L, 5, out)));
    }

    @Test
    void kHopZeroDepthOrUnknownStartReturnsNothing() {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        long[] out = new long[4];

        assertEquals(0, kernel.kHop(0L, 0, out));
        assertEquals(0, kernel.kHop(0L, -3, out));
        assertEquals(0, kernel.kHop(500_000L, 3, out));
    }

    @Test
    void kHopRejectsOutputBufferThatCannotHoldTheResult() {
        GraphKernel kernel = new GraphKernel();
        for (long v = 1; v <= 5; v++) {
            kernel.addEdge(0L, v);
        }

        assertThrows(IllegalArgumentException.class, () -> kernel.kHop(0L, 1, new long[3]));
    }

    @Test
    void repeatedQueriesAreStableAcrossGenerationWraparound() throws ReflectiveOperationException {
        GraphKernel kernel = new GraphKernel();
        for (long u = 0; u < 30; u++) {
            kernel.addEdge(u, (u * 7 + 1) % 30);
            kernel.addEdge(u, (u * 3 + 2) % 30);
        }
        long[] out = new long[32];
        Set<Long> expected = toSet(out, kernel.kHop(0L, 3, out));
        KHopTraversal traversal = traversalOf(kernel);
        generationField(traversal).setInt(traversal, Integer.MAX_VALUE - 3);

        for (int round = 0; round < 10; round++) {
            assertEquals(expected, toSet(out, kernel.kHop(0L, 3, out)), "round " + round);
        }
    }

    private static KHopTraversal traversalOf(GraphKernel kernel) throws ReflectiveOperationException {
        Field field = GraphKernel.class.getDeclaredField("traversal");
        field.setAccessible(true);
        return (KHopTraversal) field.get(kernel);
    }

    private static Field generationField(KHopTraversal traversal) throws ReflectiveOperationException {
        Field field = KHopTraversal.class.getDeclaredField("generation");
        field.setAccessible(true);
        return field;
    }

    private static Set<Long> toSet(long[] values, int count) {
        Set<Long> result = new java.util.HashSet<>();
        for (int i = 0; i < count; i++) {
            result.add(values[i]);
        }
        assertEquals(count, result.size(), "duplicates in result");
        return result;
    }
}
