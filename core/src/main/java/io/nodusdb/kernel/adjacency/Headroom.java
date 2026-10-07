package io.nodusdb.kernel.adjacency;

import io.nodusdb.kernel.memory.NativeBlockPool;

import java.util.Arrays;

public final class Headroom {

    private final int[] poolBlocks = new int[NativeBlockPool.CLASS_COUNT];
    private int slabBlocks;

    public void clear() {
        Arrays.fill(poolBlocks, 0);
        slabBlocks = 0;
    }

    public int[] poolBlocks() {
        return poolBlocks;
    }

    public int slabBlocks() {
        return slabBlocks;
    }

    void addPoolBlock(int logWords) {
        poolBlocks[logWords]++;
    }

    void addSlabBlocks(int blocks) {
        slabBlocks += blocks;
    }
}
