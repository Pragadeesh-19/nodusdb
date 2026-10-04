package io.nodusdb.capi;

import java.util.Arrays;

final class HandleTable<T> {

    private static final int INITIAL_SLOTS = 4;
    private static final int SLOT_BITS = 32;
    private static final long SLOT_MASK = 0xFFFF_FFFFL;

    private final Object lock = new Object();
    private Object[] entries = new Object[INITIAL_SLOTS];
    private int[] generations = new int[INITIAL_SLOTS];
    private int[] freeSlots = new int[INITIAL_SLOTS];
    private int freeTop;
    private int nextSlot;

    long open(T value) {
        synchronized (lock) {
            int slot;
            if (freeTop > 0) {
                slot = freeSlots[--freeTop];
            } else {
                if (nextSlot == entries.length) {
                    grow();
                }
                slot = nextSlot++;
            }
            entries[slot] = value;
            return encode(slot, generations[slot]);
        }
    }

    @SuppressWarnings("unchecked")
    T get(long handle) {
        synchronized (lock) {
            return (T) entries[decodeSlot(handle)];
        }
    }

    T close(long handle) {
        synchronized (lock) {
            int slot = decodeSlot(handle);
            @SuppressWarnings("unchecked")
            T value = (T) entries[slot];
            entries[slot] = null;
            generations[slot]++;
            freeSlots[freeTop++] = slot;
            return value;
        }
    }

    private int decodeSlot(long handle) {
        int slot = (int) (handle & SLOT_MASK) - 1;
        int generation = (int) (handle >>> SLOT_BITS);
        if (slot < 0 || slot >= nextSlot || entries[slot] == null || generations[slot] != generation) {
            throw new IllegalArgumentException("invalid or closed handle: " + handle);
        }
        return slot;
    }

    private void grow() {
        int capacity = entries.length << 1;
        entries = Arrays.copyOf(entries, capacity);
        generations = Arrays.copyOf(generations, capacity);
        freeSlots = Arrays.copyOf(freeSlots, capacity);
    }

    private static long encode(int slot, int generation) {
        return ((long) generation << SLOT_BITS) | (slot + 1L);
    }
}
