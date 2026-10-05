package io.nodusdb.kernel;

import java.util.Arrays;
import java.util.Objects;

public final class IndexedSparseSet {

    private static final int DEFAULT_CAPACITY = 16;

    private final LongIntIndex index;
    private long[] dense;
    private int degree;

    public IndexedSparseSet() {
        this(DEFAULT_CAPACITY);
    }

    public IndexedSparseSet(int initialCapacity) {
        if (initialCapacity < 1 || Integer.bitCount(initialCapacity) != 1) {
            throw new IllegalArgumentException("initial capacity must be a positive power of two: " + initialCapacity);
        }
        this.dense = new long[initialCapacity];
        this.index = new LongIntIndex(initialCapacity << 1);
    }

    public boolean add(long v) {
        if (indexOf(v) != LongIntIndex.ABSENT) {
            return false;
        }
        appendAbsent(v);
        return true;
    }

    void appendAbsent(long v) {
        if (degree == dense.length) {
            dense = Arrays.copyOf(dense, dense.length << 1);
        }
        dense[degree] = v;
        index.putAbsent(v, degree);
        degree++;
    }

    public boolean remove(long v) {
        int pos = index.get(v);
        if (pos == LongIntIndex.ABSENT) {
            return false;
        }
        removeAt(v, pos);
        return true;
    }

    void removeAt(long v, int pos) {
        assert pos >= 0 && pos < degree && dense[pos] == v : "pos " + pos + " does not hold " + v;
        int last = degree - 1;
        if (pos < last) {
            long moved = dense[last];
            dense[pos] = moved;
            index.put(moved, pos);
        }
        index.remove(v);
        degree = last;
    }

    public boolean contains(long v) {
        return indexOf(v) != LongIntIndex.ABSENT;
    }

    public int indexOf(long v) {
        int pos = index.get(v);
        if (pos == LongIntIndex.ABSENT) {
            return LongIntIndex.ABSENT;
        }
        long[] values = dense;
        return pos < degree && pos < values.length && values[pos] == v ? pos : LongIntIndex.ABSENT;
    }

    long peek(int i) {
        long[] values = dense;
        return i >= 0 && i < degree && i < values.length ? values[i] : NodeIds.NONE;
    }

    public long get(int i) {
        Objects.checkIndex(i, degree);
        return dense[i];
    }

    public int size() {
        return degree;
    }

    long[] denseArray() {
        return dense;
    }

    public int intersect(IndexedSparseSet other, long[] out) {
        IndexedSparseSet smaller;
        IndexedSparseSet larger;
        if (degree <= other.degree) {
            smaller = this;
            larger = other;
        } else {
            smaller = other;
            larger = this;
        }
        if (out.length < smaller.degree) {
            throw new OutputBufferTooSmallException(
                    "output buffer too small: " + out.length + " < " + smaller.degree);
        }
        int count = 0;
        for (int i = 0; i < smaller.degree; i++) {
            long v = smaller.dense[i];
            if (larger.index.containsKey(v)) {
                out[count++] = v;
            }
        }
        return count;
    }

    public void assertInvariant() {
        if (index.size() != degree) {
            throw new IllegalStateException("index size " + index.size() + " != degree " + degree);
        }
        for (int i = 0; i < degree; i++) {
            int pos = index.get(dense[i]);
            if (pos != i) {
                throw new IllegalStateException(
                        "dense[" + i + "]=" + dense[i] + " is indexed at " + pos);
            }
        }
    }
}
