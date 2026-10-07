package io.nodusdb.capi;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;

final class HandleTable<T> {

    private record Slot(Object value, int generation) {
    }

    private static final int INITIAL_SLOTS = 4;
    private static final int SLOT_BITS = 32;
    private static final long SLOT_MASK = 0xFFFF_FFFFL;

    private final Object lock = new Object();
    private volatile AtomicReferenceArray<Slot> slots = new AtomicReferenceArray<>(INITIAL_SLOTS);
    private int[] freeSlots = new int[INITIAL_SLOTS];
    private int freeTop;
    private int nextSlot;

    long open(T value) {
        synchronized (lock) {
            int index;
            if (freeTop > 0) {
                index = freeSlots[--freeTop];
            } else {
                if (nextSlot == slots.length()) {
                    grow();
                }
                index = nextSlot++;
            }
            Slot previous = slots.get(index);
            int generation = previous == null ? 0 : previous.generation();
            slots.set(index, new Slot(value, generation));
            return encode(index, generation);
        }
    }

    @SuppressWarnings("unchecked")
    T get(long handle) {
        return (T) resolve(handle).value();
    }

    T close(long handle) {
        synchronized (lock) {
            Slot slot = resolve(handle);
            int index = decodeIndex(handle);
            slots.set(index, new Slot(null, slot.generation() + 1));
            freeSlots[freeTop++] = index;
            @SuppressWarnings("unchecked")
            T value = (T) slot.value();
            return value;
        }
    }

    private Slot resolve(long handle) {
        AtomicReferenceArray<Slot> current = slots;
        int index = decodeIndex(handle);
        int generation = (int) (handle >>> SLOT_BITS);
        Slot slot = index >= 0 && index < current.length() ? current.get(index) : null;
        if (slot == null || slot.value() == null || slot.generation() != generation) {
            throw new IllegalArgumentException("invalid or closed handle: " + handle);
        }
        return slot;
    }

    private void grow() {
        int capacity = slots.length() << 1;
        AtomicReferenceArray<Slot> larger = new AtomicReferenceArray<>(capacity);
        for (int i = 0; i < slots.length(); i++) {
            larger.set(i, slots.get(i));
        }
        slots = larger;
        freeSlots = Arrays.copyOf(freeSlots, capacity);
    }

    private static int decodeIndex(long handle) {
        return (int) (handle & SLOT_MASK) - 1;
    }

    private static long encode(int index, int generation) {
        return ((long) generation << SLOT_BITS) | (index + 1L);
    }
}
