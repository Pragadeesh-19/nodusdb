package io.nodusdb.kernel;

import java.util.Arrays;
import java.util.Objects;

/*
 * A set of longs stored as a packed dense array, with a position-only index.
 *
 *   dense   [ 42 | 17 | 99 |  3 |  .  |  . ]   0 .. degree-1 are live
 *   index   PositionIndex, capacity 2 * dense.length, positions only
 *
 * The index is rebuilt whenever dense doubles, so its load stays at or below 0.5.
 */
public final class IndexedSparseSet {

    private static final int DEFAULT_CAPACITY = 16;

    private final PositionIndex index;
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
        this.index = new PositionIndex(initialCapacity << 1);
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
            index.rebuild(dense, degree, dense.length << 1);
        }
        dense[degree] = v;
        index.insertAbsent(v, degree);
        degree++;
    }

    public boolean remove(long v) {
        int pos = indexOf(v);
        if (pos == LongIntIndex.ABSENT) {
            return false;
        }
        removeAt(v, pos);
        return true;
    }

    void removeAt(long v, int pos) {
        assert pos >= 0 && pos < degree && dense[pos] == v : "pos " + pos + " does not hold " + v;
        index.remove(dense, degree, v);
        int last = degree - 1;
        if (pos < last) {
            long moved = dense[last];
            index.relocate(dense, degree, moved, pos);
            dense[pos] = moved;
        }
        degree = last;
    }

    public boolean contains(long v) {
        return indexOf(v) != LongIntIndex.ABSENT;
    }

    public int indexOf(long v) {
        return index.find(dense, degree, v);
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
            if (larger.indexOf(v) != LongIntIndex.ABSENT) {
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
            int pos = index.find(dense, degree, dense[i]);
            if (pos != i) {
                throw new IllegalStateException(
                        "dense[" + i + "]=" + dense[i] + " is indexed at " + pos);
            }
        }
    }
}
