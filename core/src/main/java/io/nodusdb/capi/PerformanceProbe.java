package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;

import java.util.Arrays;

public final class PerformanceProbe {

    public record Timing(long addNanos, long hasNanos) {
    }

    private static final int DEFAULT_OPERATIONS = 200_000;
    private static final int DEFAULT_RUNS = 5;
    private static final int DEFAULT_WARMUPS = 2;

    private PerformanceProbe() {
    }

    public static Timing measure(int operations) {
        try (GraphSession session = new GraphSession(new GraphKernel())) {
            long start = System.nanoTime();
            for (int i = 0; i < operations; i++) {
                session.addEdge(i, i + 1L);
            }
            long added = System.nanoTime() - start;
            start = System.nanoTime();
            int found = 0;
            for (int i = 0; i < operations; i++) {
                if (session.hasEdge(i, i + 1L)) {
                    found++;
                }
            }
            long looked = System.nanoTime() - start;
            if (found != operations) {
                throw new IllegalStateException("the probe lost edges: " + found + " of " + operations);
            }
            return new Timing(added, looked);
        }
    }

    public static void main(String[] arguments) {
        int operations = arguments.length > 0 ? Integer.parseInt(arguments[0]) : DEFAULT_OPERATIONS;
        int runs = arguments.length > 1 ? Integer.parseInt(arguments[1]) : DEFAULT_RUNS;
        int warmups = arguments.length > 2 ? Integer.parseInt(arguments[2]) : DEFAULT_WARMUPS;
        for (int i = 0; i < warmups; i++) {
            measure(operations);
        }
        long[] adds = new long[runs];
        long[] hases = new long[runs];
        for (int i = 0; i < runs; i++) {
            Timing timing = measure(operations);
            adds[i] = timing.addNanos();
            hases[i] = timing.hasNanos();
        }
        System.out.println("add_edge_ns_per_op " + median(adds) / operations);
        System.out.println("has_edge_ns_per_op " + median(hases) / operations);
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }
}
