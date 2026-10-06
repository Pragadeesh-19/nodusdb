package io.nodusdb.kernel;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphKernelAllocationTest {

    private static final int NODES = 2_000;
    private static final int LOW_DEGREE_NODE_TARGETS = 3;
    private static final int HIGH_DEGREE_NODE_TARGETS = 25;
    private static final int KHOP_DEPTH = 2;
    private static final int WARMUP_ROUNDS = 20_000;
    private static final int MEASURED_ROUNDS = 200_000;

    @Test
    void primedSteadyStateQueriesAndInTierMutationsAllocateNothing() {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported());
        GraphKernel kernel = buildGraph();
        long[] buffer = new long[NODES];
        long threadId = Thread.currentThread().threadId();

        runRounds(kernel, buffer, WARMUP_ROUNDS);
        long firstWindow = measuredAllocation(bean, threadId, kernel, buffer);
        long windowStart = ManagementFactory.getRuntimeMXBean().getUptime();
        long steadyState = measuredAllocation(bean, threadId, kernel, buffer);
        long windowEnd = ManagementFactory.getRuntimeMXBean().getUptime();
        System.out.println("DIAG window uptimeMs=" + windowStart + ".." + windowEnd + " firstWindow=" + firstWindow
                + " steadyState=" + steadyState + " vm=" + System.getProperty("java.vm.version")
                + " cpus=" + Runtime.getRuntime().availableProcessors());

        assertEquals(0L, steadyState, "bytes allocated in a primed steady-state window");
    }

    private static long measuredAllocation(com.sun.management.ThreadMXBean bean, long threadId,
                                           GraphKernel kernel, long[] buffer) {
        long before = bean.getThreadAllocatedBytes(threadId);
        runRounds(kernel, buffer, MEASURED_ROUNDS);
        long after = bean.getThreadAllocatedBytes(threadId);
        return after - before;
    }

    private static GraphKernel buildGraph() {
        GraphKernel kernel = new GraphKernel();
        for (int u = 0; u < NODES; u++) {
            int targets = u % 2 == 0 ? LOW_DEGREE_NODE_TARGETS : HIGH_DEGREE_NODE_TARGETS;
            for (int offset = 1; offset <= targets; offset++) {
                kernel.addEdge(u, (u + offset) % NODES);
            }
        }
        return kernel;
    }

    private static long runRounds(GraphKernel kernel, long[] buffer, int rounds) {
        long sink = 0;
        for (int round = 0; round < rounds; round++) {
            long low = (round * 2L) % NODES;
            long high = (low + 1) % NODES;
            long lowTarget = (low + 100) % NODES;
            long highTarget = (high + 500) % NODES;

            sink += kernel.hasEdge(low, (low + 1) % NODES) ? 1 : 0;
            sink += kernel.getDegree(high);
            sink += kernel.getInDegree(low);
            sink += kernel.commonNeighbors(low, high, buffer);
            sink += kernel.kHop(high, KHOP_DEPTH, buffer);
            sink += kernel.kHop(low, KHOP_DEPTH, buffer);
            sink += kernel.addEdge(low, lowTarget) ? 1 : 0;
            sink += kernel.removeEdge(low, lowTarget) ? 1 : 0;
            sink += kernel.addEdge(high, highTarget) ? 1 : 0;
            sink += kernel.removeEdge(high, highTarget) ? 1 : 0;
        }
        return sink;
    }
}
