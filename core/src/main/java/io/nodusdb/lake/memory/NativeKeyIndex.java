package io.nodusdb.lake.memory;

import io.nodusdb.kernel.index.OpenAddressing;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class NativeKeyIndex implements AutoCloseable {

    public static final int ABSENT = -1;

    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
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

    public int find(MemorySegment keys, long key) {
        int slot = slotOf(keys, key);
        return slot == ABSENT ? ABSENT : slots.segment().getAtIndex(INT, slot) - 1;
    }

    public void insert(MemorySegment keys, long key, int row) {
        if (size + 1 > slotCount() >>> 1) {
            rebuild(keys, size, slotCount() << 1);
        }
        place(slots.segment(), slotCount() - 1, key, row);
        size++;
    }

    public void relocate(MemorySegment keys, long key, int newRow) {
        int slot = slotOf(keys, key);
        if (slot == ABSENT) {
            throw new IllegalStateException("key is not indexed: " + key);
        }
        slots.segment().setAtIndex(INT, slot, newRow + 1);
    }

    public void remove(MemorySegment keys, long key) {
        int hole = slotOf(keys, key);
        if (hole == ABSENT) {
            throw new IllegalStateException("key is not indexed: " + key);
        }
        MemorySegment table = slots.segment();
        int mask = slotCount() - 1;
        int cursor = (hole + 1) & mask;
        int entry = table.getAtIndex(INT, cursor);
        while (entry != EMPTY) {
            int home = OpenAddressing.home(keys.getAtIndex(LONG, entry - 1), mask);
            if (OpenAddressing.canMoveInto(hole, cursor, home, mask)) {
                table.setAtIndex(INT, hole, entry);
                hole = cursor;
            }
            cursor = (cursor + 1) & mask;
            entry = table.getAtIndex(INT, cursor);
        }
        table.setAtIndex(INT, hole, EMPTY);
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

    private int slotOf(MemorySegment keys, long key) {
        MemorySegment table = slots.segment();
        int mask = slotCount() - 1;
        int slot = OpenAddressing.home(key, mask);
        for (int probes = 0; probes <= mask; probes++) {
            int entry = table.getAtIndex(INT, slot);
            if (entry == EMPTY) {
                return ABSENT;
            }
            if (keys.getAtIndex(LONG, entry - 1) == key) {
                return slot;
            }
            slot = (slot + 1) & mask;
        }
        return ABSENT;
    }

    private static void place(MemorySegment table, int mask, long key, int row) {
        int slot = OpenAddressing.home(key, mask);
        while (table.getAtIndex(INT, slot) != EMPTY) {
            slot = (slot + 1) & mask;
        }
        table.setAtIndex(INT, slot, row + 1);
    }

    private void rebuild(MemorySegment keys, int rows, int newSlotCount) {
        NativeColumn next = new NativeColumn(newSlotCount * SLOT_BYTES);
        MemorySegment table = next.segment();
        int mask = newSlotCount - 1;
        for (int row = 0; row < rows; row++) {
            place(table, mask, keys.getAtIndex(LONG, row), row);
        }
        slots.close();
        slots = next;
    }
}
