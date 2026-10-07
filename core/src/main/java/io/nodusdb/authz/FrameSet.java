package io.nodusdb.authz;

import java.util.Arrays;

final class FrameSet {

    private static final int INITIAL_CAPACITY = 64;
    private static final long MIX = 0x9E3779B97F4A7C15L;

    private long[] keys = new long[INITIAL_CAPACITY];
    private int[] stamps = new int[INITIAL_CAPACITY];
    private int generation = 1;
    private int size;

    void clear() {
        generation++;
        size = 0;
        if (generation == Integer.MAX_VALUE) {
            Arrays.fill(stamps, 0);
            generation = 1;
        }
    }

    boolean add(int node, int definition) {
        long key = ((long) node << Integer.SIZE) | definition;
        if (2 * (size + 1) > keys.length) {
            grow();
        }
        int mask = keys.length - 1;
        int slot = (int) (key * MIX >>> 32) & mask;
        while (stamps[slot] == generation) {
            if (keys[slot] == key) {
                return false;
            }
            slot = (slot + 1) & mask;
        }
        keys[slot] = key;
        stamps[slot] = generation;
        size++;
        return true;
    }

    private void grow() {
        long[] oldKeys = keys;
        int[] oldStamps = stamps;
        int oldGeneration = generation;
        keys = new long[oldKeys.length * 2];
        stamps = new int[oldKeys.length * 2];
        generation = 1;
        size = 0;
        for (int i = 0; i < oldKeys.length; i++) {
            if (oldStamps[i] == oldGeneration) {
                add((int) (oldKeys[i] >>> Integer.SIZE), (int) oldKeys[i]);
            }
        }
    }
}
