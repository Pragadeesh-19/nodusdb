package io.nodusdb.kernel.index;

import java.util.Arrays;
import java.util.NoSuchElementException;

public final class SparseMap {

    private static final int DEFAULT_CAPACITY = 16;

    private final IndexedSparseSet keys;
    private long[] values;

    public SparseMap() {
        this(DEFAULT_CAPACITY);
    }

    public SparseMap(int initialCapacity) {
        this.keys = new IndexedSparseSet(initialCapacity);
        this.values = new long[initialCapacity];
    }

    public boolean containsKey(long key) {
        return keys.contains(key);
    }

    public int size() {
        return keys.size();
    }

    public boolean remove(long key) {
        int pos = keys.indexOf(key);
        if (pos == LongIntIndex.ABSENT) {
            return false;
        }
        values[pos] = values[keys.size() - 1];
        keys.removeAt(key, pos);
        return true;
    }

    public long getLong(long key) {
        int pos = keys.indexOf(key);
        if (pos == LongIntIndex.ABSENT) {
            throw new NoSuchElementException("key not present: " + key);
        }
        return values[pos];
    }

    public void putLong(long key, long value) {
        int pos = keys.indexOf(key);
        if (pos != LongIntIndex.ABSENT) {
            values[pos] = value;
            return;
        }
        keys.appendAbsent(key);
        int appended = keys.size() - 1;
        if (appended >= values.length) {
            values = Arrays.copyOf(values, values.length << 1);
        }
        values[appended] = value;
    }

    public double getWeight(long key) {
        return Double.longBitsToDouble(getLong(key));
    }

    public void putWeight(long key, double weight) {
        putLong(key, Double.doubleToRawLongBits(weight));
    }
}
