package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Partition;

public record GraphDigest(long edges, int symbols, long fingerprint) {

    private static final long FIRST_MULTIPLIER = 0xff51afd7ed558ccdL;
    private static final long SECOND_MULTIPLIER = 0xc4ceb9fe1a85ec53L;
    private static final long NODE_STRIDE = 0x9E3779B97F4A7C15L;

    public static GraphDigest of(GraphKernel kernel) {
        long edges = 0;
        long fingerprint = 0;
        int nodes = kernel.nodeCapacity();
        for (Partition partition : Partition.values()) {
            if (!kernel.hasPartition(partition)) {
                continue;
            }
            for (int node = 0; node < nodes; node++) {
                int degree = kernel.degree(partition, true, node);
                for (int i = 0; i < degree; i++) {
                    fingerprint += mix(node * NODE_STRIDE + kernel.keyAt(partition, true, node, i));
                }
                edges += degree;
            }
        }
        return new GraphDigest(edges, kernel.symbols().size(), fingerprint);
    }

    private static long mix(long value) {
        long h = value;
        h ^= h >>> 33;
        h *= FIRST_MULTIPLIER;
        h ^= h >>> 33;
        h *= SECOND_MULTIPLIER;
        h ^= h >>> 33;
        return h;
    }
}
