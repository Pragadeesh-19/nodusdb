package io.nodusdb.kernel;

import java.util.Arrays;

public final class LongIntIndex {

    static final int ABSENT = -1;
    static final int DEFAULT_CAPACITY = 16;
    static final int MAX_CAPACITY = 1 << 30;

    private long[] keys;
    private int[] values;
    private int mask;
    private int size;
    private int growthThreshold;

    public LongIntIndex() {
        this(DEFAULT_CAPACITY);
    }

    public LongIntIndex(int capacity) {
        if (capacity < 2 || capacity > MAX_CAPACITY || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity must be a power of two in [2, 2^30]: " + capacity);
        }
        allocate(capacity);
    }

    public int get(long key) {
        int[] table = values;
        long[] keyTable = keys;
        int currentMask = mask;
        int slot = homeOf(key, currentMask);
        for (int probes = 0; probes <= currentMask; probes++) {
            if (slot >= table.length || slot >= keyTable.length || table[slot] == ABSENT) {
                return ABSENT;
            }
            if (keyTable[slot] == key) {
                return table[slot];
            }
            slot = (slot + 1) & currentMask;
        }
        return ABSENT;
    }

    public boolean containsKey(long key) {
        return get(key) != ABSENT;
    }

    public void put(long key, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative: " + value);
        }
        int existing = slotOf(key);
        if (existing >= 0) {
            values[existing] = value;
            return;
        }
        putAbsent(key, value);
    }

    void putAbsent(long key, int value) {
        assert value >= 0 && slotOf(key) == ABSENT : "key is already present: " + key;
        if (size + 1 > growthThreshold) {
            grow();
        }
        insertFresh(key, value);
        size++;
    }

    public boolean remove(long key) {
        int hole = slotOf(key);
        if (hole < 0) {
            return false;
        }
        int cursor = (hole + 1) & mask;
        while (values[cursor] != ABSENT) {
            int home = homeOf(keys[cursor], mask);
            if (((hole - home) & mask) <= ((cursor - home) & mask)) {
                keys[hole] = keys[cursor];
                values[hole] = values[cursor];
                hole = cursor;
            }
            cursor = (cursor + 1) & mask;
        }
        values[hole] = ABSENT;
        size--;
        return true;
    }

    public int size() {
        return size;
    }

    public void clear() {
        Arrays.fill(values, ABSENT);
        size = 0;
    }

    public int capacity() {
        return mask + 1;
    }

    static int homeOf(long key, int mask) {
        return (int) mix(key) & mask;
    }

    static long mix(long key) {
        long h = key;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    private int slotOf(long key) {
        int slot = homeOf(key, mask);
        while (values[slot] != ABSENT) {
            if (keys[slot] == key) {
                return slot;
            }
            slot = (slot + 1) & mask;
        }
        return ABSENT;
    }

    private void insertFresh(long key, int value) {
        int slot = homeOf(key, mask);
        while (values[slot] != ABSENT) {
            slot = (slot + 1) & mask;
        }
        keys[slot] = key;
        values[slot] = value;
    }

    private void grow() {
        int oldCapacity = capacity();
        if (oldCapacity >= MAX_CAPACITY) {
            throw new IllegalStateException("LongIntIndex capacity limit reached: " + MAX_CAPACITY);
        }
        long[] oldKeys = keys;
        int[] oldValues = values;
        allocate(oldCapacity << 1);
        for (int i = 0; i < oldCapacity; i++) {
            if (oldValues[i] != ABSENT) {
                insertFresh(oldKeys[i], oldValues[i]);
            }
        }
    }

    private void allocate(int capacity) {
        keys = new long[capacity];
        values = new int[capacity];
        Arrays.fill(values, ABSENT);
        mask = capacity - 1;
        growthThreshold = capacity >>> 1;
    }
}
