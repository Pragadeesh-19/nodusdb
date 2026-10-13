package io.nodusdb.kernel.index;

import java.util.Arrays;

public final class StampedLongSet {

    private static final int INITIAL_CAPACITY = 64;
    private static final long MIX = 0x9E3779B97F4A7C15L;

    private long[] keys = new long[INITIAL_CAPACITY];
    private int[] stamps = new int[INITIAL_CAPACITY];
    private int generation = 1;
    private int size;

    public void clear() {
        generation++;
        size = 0;
        if (generation == Integer.MAX_VALUE) {
            Arrays.fill(stamps, 0);
            generation = 1;
        }
    }

    public int size() {
        return size;
    }

    public boolean contains(long key) {
        int mask = keys.length - 1;
        int slot = home(key, mask);
        while (stamps[slot] == generation) {
            if (keys[slot] == key) {
                return true;
            }
            slot = (slot + 1) & mask;
        }
        return false;
    }

    public boolean add(long key) {
        if (2 * (size + 1) > keys.length) {
            grow();
        }
        int mask = keys.length - 1;
        int slot = home(key, mask);
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

    private static int home(long key, int mask) {
        return (int) (key * MIX >>> 32) & mask;
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
                add(oldKeys[i]);
            }
        }
    }
}
