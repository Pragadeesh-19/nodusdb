package io.nodusdb.kernel.adjacency;

import io.nodusdb.kernel.memory.NativeBlockPool;

import java.util.Arrays;

public final class Headroom {

    private final int[] poolBlocks = new int[NativeBlockPool.CLASS_COUNT];
    private int slabBlocks;
    private boolean pooled;

    public void clear() {
        if (pooled) {
            Arrays.fill(poolBlocks, 0);
            pooled = false;
        }
        slabBlocks = 0;
    }

    public boolean isEmpty() {
        return !pooled && slabBlocks == 0;
    }

    public int[] poolBlocks() {
        return poolBlocks;
    }

    public int slabBlocks() {
        return slabBlocks;
    }

    void addPoolBlock(int logWords) {
        poolBlocks[logWords]++;
        pooled = true;
    }

    void addSlabBlocks(int blocks) {
        slabBlocks += blocks;
    }
}
