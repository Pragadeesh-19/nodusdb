package io.nodusdb.kernel.changes;

import java.util.Arrays;
import java.util.Objects;

public final class ChangeBuffer implements EntrySink {

    private static final int INITIAL_ENTRIES = 256;

    private long[] words = new long[INITIAL_ENTRIES * ChangeEntry.LONGS];
    private int size;
    private int changes;

    @Override
    public void entry(long lsn, long meta, long payload) {
        int at = size * ChangeEntry.LONGS;
        if (at + ChangeEntry.LONGS > words.length) {
            words = Arrays.copyOf(words, words.length * 2);
        }
        words[at] = lsn;
        words[at + 1] = meta;
        words[at + 2] = payload;
        size++;
        if (ChangeEntry.kind(meta) != ChangeEntry.COMMIT) {
            changes++;
        }
    }

    public int size() {
        return size;
    }

    public int changes() {
        return changes;
    }

    public long lsn(int index) {
        return words[checked(index) * ChangeEntry.LONGS];
    }

    public long meta(int index) {
        return words[checked(index) * ChangeEntry.LONGS + 1];
    }

    public long payload(int index) {
        return words[checked(index) * ChangeEntry.LONGS + 2];
    }

    public int kind(int index) {
        return ChangeEntry.kind(meta(index));
    }

    public long lastCommitLsn() {
        for (int i = size - 1; i >= 0; i--) {
            if (kind(i) == ChangeEntry.COMMIT) {
                return lsn(i);
            }
        }
        return -1;
    }

    public void truncate(int newSize) {
        if (newSize < 0 || newSize > size) {
            throw new IllegalArgumentException("cannot truncate " + size + " entries to " + newSize);
        }
        for (int i = newSize; i < size; i++) {
            if (kind(i) != ChangeEntry.COMMIT) {
                changes--;
            }
        }
        size = newSize;
    }

    public void clear() {
        size = 0;
        changes = 0;
    }

    private int checked(int index) {
        return Objects.checkIndex(index, size);
    }
}
