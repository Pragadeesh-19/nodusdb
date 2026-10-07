package io.nodusdb.kernel.index;

import java.util.Arrays;

public final class PositionIndex {

    static final int ABSENT = -1;
    private static final int NO_SLOT = -1;

    private int[] table;
    private int size;

    PositionIndex(int capacity) {
        allocate(capacity);
    }

    int size() {
        return size;
    }

    int capacity() {
        return table.length;
    }

    int find(long[] dense, int degree, long key) {
        int slot = slotOf(dense, degree, key);
        return slot == NO_SLOT ? ABSENT : table[slot];
    }

    void insertAbsent(long key, int position) {
        int[] slots = table;
        int mask = slots.length - 1;
        int slot = OpenAddressing.home(key, mask);
        while (slots[slot] != ABSENT) {
            slot = (slot + 1) & mask;
        }
        slots[slot] = position;
        size++;
    }

    void relocate(long[] dense, int degree, long key, int newPosition) {
        int slot = slotOf(dense, degree, key);
        assert slot != NO_SLOT : "key is not indexed: " + key;
        table[slot] = newPosition;
    }

    void remove(long[] dense, int degree, long key) {
        int[] slots = table;
        int mask = slots.length - 1;
        int hole = slotOf(dense, degree, key);
        assert hole != NO_SLOT : "key is not indexed: " + key;
        int cursor = (hole + 1) & mask;
        while (slots[cursor] != ABSENT) {
            int shiftedHome = OpenAddressing.home(dense[slots[cursor]], mask);
            if (OpenAddressing.canMoveInto(hole, cursor, shiftedHome, mask)) {
                slots[hole] = slots[cursor];
                hole = cursor;
            }
            cursor = (cursor + 1) & mask;
        }
        slots[hole] = ABSENT;
        size--;
    }

    void rebuild(long[] dense, int degree, int newCapacity) {
        allocate(newCapacity);
        for (int position = 0; position < degree; position++) {
            insertAbsent(dense[position], position);
        }
    }

    private int slotOf(long[] dense, int degree, long key) {
        int[] slots = table;
        int mask = slots.length - 1;
        int slot = OpenAddressing.home(key, mask);
        for (int probes = 0; probes <= mask; probes++) {
            int position = slots[slot];
            if (position == ABSENT) {
                return NO_SLOT;
            }
            if (position < degree && position < dense.length && dense[position] == key) {
                return slot;
            }
            slot = (slot + 1) & mask;
        }
        return NO_SLOT;
    }

    private void allocate(int capacity) {
        table = new int[capacity];
        Arrays.fill(table, ABSENT);
        size = 0;
    }
}
