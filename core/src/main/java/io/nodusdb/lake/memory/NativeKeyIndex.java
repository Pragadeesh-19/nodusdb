package io.nodusdb.lake.memory;

import io.nodusdb.kernel.index.OpenAddressing;

public final class NativeKeyIndex implements AutoCloseable {

    public static final int ABSENT = -1;

    private static final int EMPTY = 0;
    private static final long SLOT_BYTES = Integer.BYTES;

    private NativeColumn slots;
    private int size;

    public NativeKeyIndex(int slotCount) {
        slots = new NativeColumn(slotCount * SLOT_BYTES);
    }

    public int size() {
        return size;
    }

    int slotCount() {
        return (int) (slots.capacity() / SLOT_BYTES);
    }

    public int find(NativeColumn keys, long key) {
        int slot = slotOf(keys, key);
        return slot == ABSENT ? ABSENT : slots.getIntAtIndex(slot) - 1;
    }

    public void insert(NativeColumn keys, long key, int row) {
        if (size + 1 > slotCount() >>> 1) {
            rebuild(keys, size, slotCount() << 1);
        }
        place(slots, slotCount() - 1, key, row);
        size++;
    }

    public void relocate(NativeColumn keys, long key, int newRow) {
        int slot = slotOf(keys, key);
        if (slot == ABSENT) {
            throw new IllegalStateException("key is not indexed: " + key);
        }
        slots.setIntAtIndex(slot, newRow + 1);
    }

    public void remove(NativeColumn keys, long key) {
        int hole = slotOf(keys, key);
        if (hole == ABSENT) {
            throw new IllegalStateException("key is not indexed: " + key);
        }
        NativeColumn table = slots;
        int mask = slotCount() - 1;
        int cursor = (hole + 1) & mask;
        int entry = table.getIntAtIndex(cursor);
        while (entry != EMPTY) {
            int home = OpenAddressing.home(keys.getLongAtIndex(entry - 1), mask);
            if (OpenAddressing.canMoveInto(hole, cursor, home, mask)) {
                table.setIntAtIndex(hole, entry);
                hole = cursor;
            }
            cursor = (cursor + 1) & mask;
            entry = table.getIntAtIndex(cursor);
        }
        table.setIntAtIndex(hole, EMPTY);
        size--;
    }

    public void clear() {
        slots.segment().fill((byte) 0);
        size = 0;
    }

    @Override
    public void close() {
        slots.close();
    }

    private int slotOf(NativeColumn keys, long key) {
        NativeColumn table = slots;
        int mask = slotCount() - 1;
        int slot = OpenAddressing.home(key, mask);
        for (int probes = 0; probes <= mask; probes++) {
            int entry = table.getIntAtIndex(slot);
            if (entry == EMPTY) {
                return ABSENT;
            }
            if (keys.getLongAtIndex(entry - 1) == key) {
                return slot;
            }
            slot = (slot + 1) & mask;
        }
        return ABSENT;
    }

    private static void place(NativeColumn table, int mask, long key, int row) {
        int slot = OpenAddressing.home(key, mask);
        while (table.getIntAtIndex(slot) != EMPTY) {
            slot = (slot + 1) & mask;
        }
        table.setIntAtIndex(slot, row + 1);
    }

    private void rebuild(NativeColumn keys, int rows, int newSlotCount) {
        NativeColumn next = new NativeColumn(newSlotCount * SLOT_BYTES);
        int mask = newSlotCount - 1;
        for (int row = 0; row < rows; row++) {
            place(next, mask, keys.getLongAtIndex(row), row);
        }
        slots.close();
        slots = next;
    }
}
